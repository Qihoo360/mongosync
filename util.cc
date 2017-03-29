#include <sys/time.h>
#include <unistd.h>
#include <stdio.h>

#include <iostream>

#include "util.h"
#include "mongosync.h"

namespace util {

uint64_t Microtime() {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec*1000000 + tv.tv_usec;
}

std::string Int2Str(int64_t num) {
  char buf[32];
  snprintf(buf, sizeof(buf), "%ld", num);
  return buf;
}

std::string Trim(const std::string &str, const std::string del_str) {
  std::string::size_type begin, end;
  std::string ret;
  if ((begin = str.find_first_not_of(del_str)) == std::string::npos) {
    return ret;
  }
  end = str.find_last_not_of(del_str);
  return str.substr(begin, end-begin+1);
}

std::vector<std::string> &Split(const std::string &s,
                                char delim,
                                std::vector<std::string> &elems) {
    elems.clear();
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        if (!item.empty())
            elems.push_back(Trim(item, " "));
    }
    return elems;
}

bool AlmostEqual(int64_t v1, int64_t v2, uint64_t range) {
	if (v1 <= v2 + range && v1 >= v2 - range) {
		return true;
	}
	return false;
}

std::string GetFormatTime(time_t t) { // Output format is: Aug 23 14:55:02 2001
	char buf[32];
	if (t == -1) {
		t = time(NULL);
	}
	strftime(buf, sizeof(buf), "%b %d %T %Y", localtime(&t));
	return std::string(buf);
}

static int DoCreatePath(const char *path, mode_t mode) {
  struct stat st;
  int status = 0;

  if (stat(path, &st) != 0) {
    /* Directory does not exist. EEXIST for race
     * condition */
    if (mkdir(path, mode) != 0 && errno != EEXIST)
      status = -1;
  } else if (!S_ISDIR(st.st_mode)) {
    errno = ENOTDIR;
    status = -1;
  }

  return (status);
}

int CreatePath(const std::string &path, mode_t mode) {
  char           *pp;
  char           *sp;
  int             status;
  char           *copypath = strdup(path.c_str());

  status = 0;
  pp = copypath;
  while (status == 0 && (sp = strchr(pp, '/')) != 0) {
    if (sp != pp) {
      /* Neither root nor double slash in path */
      *sp = '\0';
      status = DoCreatePath(copypath, mode);
      *sp = '/';
    }
    pp = sp + 1;
  }
  if (status == 0)
    status = DoCreatePath(path.c_str(), mode);
  free(copypath);
  return (status);
}


/*******************************************************************************************/

BGThreadGroup::BGThreadGroup(const std::string &srv_ip_port, const std::string &auth_db, const std::string &user, const std::string &passwd, const bool use_mcr, const int32_t bg_thread_num, bool is_apply_oplog)
  :srv_ip_port_(srv_ip_port),
	auth_db_(auth_db),
	user_(user),
	passwd_(passwd),
	running_(false), 
  should_exit_(false),
	use_mcr_(use_mcr),
	bg_thread_num_(bg_thread_num),
  is_apply_oplog_(is_apply_oplog) {

  pthread_mutex_init(&mlock_, NULL);
  pthread_cond_init(&clock_, NULL);

}

BGThreadGroup::~BGThreadGroup() {
	should_exit_ = true;
	pthread_cond_broadcast(&clock_);
	for (std::vector<pthread_t>::const_iterator iter = tids_.begin();
			iter != tids_.end();
			++iter) {
		pthread_join(*iter, NULL);
	}

  pthread_mutex_destroy(&mlock_);
  pthread_cond_destroy(&clock_);
}

void BGThreadGroup::StartThreadsIfNeed() {
  if (running_) {
    return;
  }

	pthread_t tid;
	for (int idx = 0; idx != bg_thread_num_; ++idx) {
  	if (pthread_create(&tid, NULL, BGThreadGroup::Run, this) != -1) {
  	  tids_.push_back(tid);
  	} else {
  	  std::cerr << "[WARN]\tBGThread: " << idx << " starts error!" << std::endl;
  	}
	}
	if (tids_.empty()) {
		std::cerr << "[ERROR]\tBGThreadGroup all start fail!" << std::endl;
		exit(-1);	
	}
	running_ = true;	
}

void BGThreadGroup::AddWriteUnit(const std::string &ns, WriteBatch *batch) {
  StartThreadsIfNeed();

  WriteUnit unit;
  unit.ns = ns;
  unit.batch = batch;


  pthread_mutex_lock(&mlock_);
  while (!write_queue_.empty()) {
    pthread_mutex_unlock(&mlock_);
    usleep(50);
    pthread_mutex_lock(&mlock_);
  }

  write_queue_.push(unit);
  pthread_cond_signal(&clock_);
  pthread_mutex_unlock(&mlock_);  
}

void BGThreadGroup::AddWriteUnit(OplogArgs *args, void *(*handle_fun)(void *)) {
  StartThreadsIfNeed();

  WriteUnit unit;
  unit.args = args;
  unit.handle = handle_fun;

  pthread_mutex_lock(&mlock_);
  while (!write_queue_.empty()) {
    pthread_mutex_unlock(&mlock_);
    usleep(10);
    pthread_mutex_lock(&mlock_);
  }

  write_queue_.push(unit);
  pthread_cond_signal(&clock_);
  pthread_mutex_unlock(&mlock_);  
}

void *BGThreadGroup::Run(void *arg) {
  BGThreadGroup *thread_ptr = reinterpret_cast<BGThreadGroup *>(arg);
	mongo::DBClientConnection *conn = MongoSync::ConnectAndAuth(thread_ptr->srv_ip_port(), thread_ptr->auth_db(), thread_ptr->user(), thread_ptr->passwd(), thread_ptr->use_mcr(), true);
	if (!conn) {
		return NULL;
	}

  std::queue<WriteUnit> *queue_p = thread_ptr->write_queue_p();
  pthread_mutex_t *queue_mutex_p = thread_ptr->mlock_p();
  pthread_cond_t *queue_cond_p = thread_ptr->clock_p();
  WriteUnit unit;

  while (!thread_ptr->should_exit()) {

  	pthread_mutex_lock(queue_mutex_p);
    while (queue_p->empty() && !thread_ptr->should_exit()) {
      pthread_cond_wait(queue_cond_p, queue_mutex_p);
    }
    
    if (thread_ptr->should_exit()) {
  		pthread_mutex_unlock(queue_mutex_p);
      break;
    }

    unit = queue_p->front();
    queue_p->pop();
    pthread_mutex_unlock(queue_mutex_p);

    int retries = 3;
retry:
    try {
      if (thread_ptr->is_apply_oplog()) {
        unit.args->dst_conn = conn;
        unit.handle((void *)unit.args);
        // LOG(WARN) << "insert oplog to dst server: " << unit.args->oplog.toString(0, 0) << std::endl;
        delete unit.args;
      } else {
        conn->insert(unit.ns, *(unit.batch),
                     mongo::InsertOption_ContinueOnError, &mongo::WriteConcern::unacknowledged); 
        delete unit.batch;
      }
    } catch (mongo::DBException &e) {
      if (thread_ptr->is_apply_oplog()) {
        LOG(WARN) << "exception occurs when insert oplog to dst server: " << e.toString() <<
          unit.args->oplog.toString() << std::endl;
      } else {
        LOG(WARN) << "exception occurs when insert data to dst server: " << e.toString() << std::endl;
        for (int i = 0; i < unit.batch->size(); i++) {
          LOG(WARN) << "exception bson data: " << (*(unit.batch))[i].toString() << std::endl;
        }
      }
      if (retries--) {
        goto retry;
      } else {
        LOG(FATAL) << "insert exception occur 3 times, exit" << std::endl;
        exit(-1);
      }
    }
  }

	delete conn;
  return NULL;
}

}
