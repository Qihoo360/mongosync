#ifndef UTIL_H
#define UTIL_H

#include <stdint.h>
#include <string>
#include <vector>
#include <queue>
#include <pthread.h>

#include "mongo/client/dbclient.h"

namespace util {

typedef std::vector<mongo::BSONObj> WriteBatch;
typedef struct {
  std::string ns;
  WriteBatch *batch;
  mongo::DBClientConnection *conn;
} WriteUnit;

uint64_t Microtime();
std::string Int2Str(int64_t num);

class BGThread {

#define MAX_LAG_NUM 5
public:
  BGThread();
  ~BGThread();

  void AddWriteUnit(mongo::DBClientConnection *conn, const std::string &ns, WriteBatch *unit);

  bool should_exit() {
    return should_exit_;
  }
  
  std::queue<WriteUnit> *write_queue_p() {
    return &write_queue_;
  }

  pthread_cond_t *clock_p() {
    return &clock_;
  }

  pthread_mutex_t *mlock_p() {
    return &mlock_;
  }

private:
  void StartThreadIfNeed();
  static void *Run(void *arg);

  pthread_t tid_;
  bool running_;
 
  bool should_exit_;

  pthread_cond_t clock_;
  pthread_mutex_t mlock_;
  std::queue<WriteUnit> write_queue_;
};


}
#endif
