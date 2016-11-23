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

/*******************************************************************************************/

BGThreadGroup::BGThreadGroup(const std::string &srv_ip_port, const std::string &auth_db, const std::string &user, const std::string &passwd, const bool use_mcr)
  :srv_ip_port_(srv_ip_port),
	auth_db_(auth_db),
	user_(user),
	passwd_(passwd),
	running_(false), 
  should_exit_(false),
	use_mcr_(use_mcr) {

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
	for (int idx = 0; idx != BG_THREAD_NUM; ++idx) {
  	if (pthread_create(&tid, NULL, BGThreadGroup::Run, this) != -1) {
  	  tids_.push_back(tid);
//  	  std::cerr << "BGThread: " << idx << " starts success!" << std::endl;
  	} else {
  	  std::cerr << "BGThread: " << idx << " starts error!" << std::endl;
  	}
	}
	if (tids_.empty()) {
		std::cerr << "BGThreadGroup all start fail!" << std::endl;
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
    sleep(1);
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

    conn->insert(unit.ns, *(unit.batch), mongo::InsertOption_ContinueOnError, &mongo::WriteConcern::unacknowledged); 
    delete unit.batch;
  }

	delete conn;
  return NULL;
}

}
