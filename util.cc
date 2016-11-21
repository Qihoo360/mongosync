#include <sys/time.h>
#include <unistd.h>
#include <stdio.h>

#include <iostream>

#include "util.h"

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

BGThread::BGThread()
  :tid_(0), 
  running_(false), 
  should_exit_(false) {

  pthread_mutex_init(&mlock_, NULL);
  pthread_cond_init(&clock_, NULL);

}

BGThread::~BGThread() {
  if (tid_) {
    should_exit_ = true;
    pthread_cond_signal(&clock_);
    pthread_join(tid_, NULL);
  }

  pthread_mutex_destroy(&mlock_);
  pthread_cond_destroy(&clock_);
}

void BGThread::StartThreadIfNeed() {
  if (running_) {
    return;
  }

  if (pthread_create(&tid_, NULL, BGThread::Run, this) != -1) {
    running_ = true;
  } else {
    std::cerr << "BGThread start error" << std::endl;
  }
}

void BGThread::AddWriteUnit(mongo::DBClientConnection *conn, const std::string &ns, WriteBatch *batch) {
  StartThreadIfNeed();

  WriteUnit unit;
  unit.conn = conn;
  unit.ns = ns;
  unit.batch = batch;


  pthread_mutex_lock(&mlock_);
  while (write_queue_.size() >= MAX_LAG_NUM) {
    pthread_mutex_unlock(&mlock_);
    sleep(1);
    pthread_mutex_lock(&mlock_);
  }

  write_queue_.push(unit);
  pthread_cond_signal(&clock_);
  pthread_mutex_unlock(&mlock_);  
}


void *BGThread::Run(void *arg) {
  BGThread *thread_ptr = reinterpret_cast<BGThread *>(arg);
  std::queue<WriteUnit> *queue_p = thread_ptr->write_queue_p();
  pthread_mutex_t *queue_mutex_p = thread_ptr->mlock_p();
  pthread_cond_t *queue_cond_p = thread_ptr->clock_p();
  WriteUnit unit;

  pthread_mutex_lock(queue_mutex_p);

  while (!thread_ptr->should_exit()) {

    while (queue_p->empty() && !thread_ptr->should_exit()) {
      pthread_cond_wait(queue_cond_p, queue_mutex_p);
    }
    
    if (thread_ptr->should_exit()) {
      break;
    }

    unit = queue_p->front();
    pthread_mutex_unlock(queue_mutex_p);
//    unit.conn->insert(unit.ns, unit.batch, mongo::InsertOption_ContinueOnError, &mongo::WriteConcern::unacknowledged); 
    unit.conn->insert(unit.ns, *(unit.batch), mongo::InsertOption_ContinueOnError, &mongo::WriteConcern::unacknowledged); 
    delete unit.batch;
    pthread_mutex_lock(queue_mutex_p);
    queue_p->pop();
  }

  pthread_mutex_unlock(queue_mutex_p);
  return NULL;
}

}
