#ifndef UTIL_H
#define UTIL_H

#include <stdint.h>
#include <string>
#include <vector>
#include <queue>
#include <time.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "mongo/client/dbclient.h"

namespace util {

uint64_t Microtime();
std::string Int2Str(int64_t num);
std::string Trim(const std::string &str, const std::string del_str = " \t\n");
std::vector<std::string> &Split(const std::string &str, char delim,
                                std::vector<std::string> &elems);
bool AlmostEqual(int64_t v1, int64_t v2, uint64_t range);
std::string GetFormatTime(time_t t = -1); // Output format is: Feb 13 16:06:10 2013
int CreatePath(const std::string &path, mode_t mode = 0755);

/*************************************************************************************************/
struct OplogArgs {
  std::string db;
  std::string coll;
  std::string dst_db;
  std::string dst_coll;
  std::string promot;
  mongo::BSONObj oplog;
	mongo::DBClientConnection* dst_conn;
  int op;
};
typedef std::vector<mongo::BSONObj> WriteBatch;
typedef struct {
  std::string ns;
  WriteBatch *batch;
  OplogArgs *args;
  void *(*handle)(void*);
} WriteUnit;

class BGThreadGroup { //This BGThreadGroup is only used for batch write

#define BG_THREAD_NUM 10
public:
  BGThreadGroup(const std::string &srv_ip_port, const std::string &auth_db = "", const std::string &user = "", const std::string &passwd = "", const bool use_mcr = false, int32_t bg_thread_num = 10, bool is_apply_oplog = false);
  ~BGThreadGroup();

  void AddWriteUnit(const std::string &ns, WriteBatch *unit);
  void AddWriteUnit(OplogArgs *args, void *(*handle_fun)(void *));

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

	const std::string &srv_ip_port() const {
		return srv_ip_port_; 
	}

	const std::string &auth_db() const {
		return auth_db_;
	}

	const std::string &user() const {
		return user_;
	}

	const std::string &passwd() const {
		return passwd_;
	}

	bool use_mcr() const {
		return use_mcr_;
	} 

	bool is_apply_oplog() const {
		return is_apply_oplog_;
	} 

private:
  void StartThreadsIfNeed();
  static void *Run(void *arg);

	std::vector<pthread_t> tids_;
  bool running_; 
  bool should_exit_; //TODO: maybe needs protected, but here temporally

	std::string srv_ip_port_;
	std::string auth_db_;
	std::string user_;
	std::string passwd_;
	bool use_mcr_;
	int32_t bg_thread_num_;
	bool is_apply_oplog_;

  pthread_cond_t clock_;
  pthread_mutex_t mlock_;
  std::queue<WriteUnit> write_queue_;
};

}
#endif
