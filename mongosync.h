#ifndef MONGO_SYNC_H
#define MONGO_SYNC_H
#include <string>

#include "mongo/client/dbclient.h"
#include "util.h"
#include "log.h"

#define MAX_BATCH_BUFFER_SIZE (16*1024*1024)
const std::string PROMPT_PREFIX = "\t[mongosync";
const int OPLOG_APPLY_THREADNUM = 6;

enum OplogProcessOp {
	kClone = 0,
	kApply
};

struct OplogTime {
	bool empty() { //mark whether to be initialized
		return sec == 2147483647 && no == 2147483647;
	}

	mongo::Timestamp_t timestamp() {
		return mongo::Timestamp_t(sec, no);
	}

	OplogTime(const mongo::Timestamp_t& t) {
		sec = t.seconds();
		no = t.increment();
	}

	const OplogTime& operator=(const mongo::Timestamp_t& t) {
		sec = t.seconds();
		no = t.increment();
    return *this;
	}

	OplogTime(int32_t _sec = 2147483647, int32_t _no = 2147483647)
		: no(_no), 
		sec(_sec) {
	}

	int32_t no; //the logical time rank with 1 second
	int32_t sec; //the unix time in secon
};

struct Options {
	Options()
    : src_auth_db("admin"),
		  src_use_mcr(false),
      is_mongos(false),
	    dst_auth_db("admin"),		
		  dst_use_mcr(false),
		  oplog(false),
      raw_oplog(false),
      dst_oplog_ns("sync.oplog"),
      no_index(false),
      bg_num(10),
		  batch_size(16*1024*1024) {
  }

	std::string src_ip_port;
	std::string src_user;
	std::string src_passwd;
	std::string src_auth_db;
	bool src_use_mcr;
  bool is_mongos;

  std::string shard_user;
  std::string shard_passwd;

	std::string dst_ip_port;
	std::string dst_user;
	std::string dst_passwd;
	std::string dst_auth_db;
	bool dst_use_mcr;

//the database or collection to be transfered	
	std::string db;
	std::string dst_db;
	std::string coll;
	std::string colls;
	std::string dst_coll;

	bool oplog;
	OplogTime oplog_start;
	OplogTime oplog_end;  //the time is inclusive

	bool raw_oplog;
	std::string dst_oplog_ns;

	bool no_index;
	mongo::Query filter;

	int32_t bg_num; //bg thread for cloning data
	int32_t batch_size; //used for data cloning when grouping data, unit is Byte

	std::string log_level;
	std::string log_dir;

  void ParseCommand(int argc, char** argv);
  void LoadConf(const std::string &conf_file);
	bool ValidCheck();
private:
  std::map<std::string, std::string> items_;
  bool GetConfBool(const std::string &item_key, bool *value);
  int32_t GetConfInt(const std::string &item_key, int32_t *value);
  bool GetConfStr(const std::string &item_key, std::string *value);
  bool GetConfQuery(const std::string &item_key, mongo::Query *value);
  bool GetConfOplogTime(const std::string &item_key, OplogTime *value);
};

class NamespaceString {
public:
	NamespaceString()
		: dot_index_(std::string::npos) {
	}

	NamespaceString(std::string ns)
		: ns_(ns) {
		dot_index_ = ns_.find_first_of(".");
	}

	NamespaceString(std::string db, std::string coll) {
		ns_ = db + "." + coll;
		dot_index_ = db.size();
	}
	
	std::string db() {
		if (dot_index_ == std::string::npos) {
			return std::string();
		}
		return ns_.substr(0, dot_index_);
	}

	std::string coll() {
		if (dot_index_ == std::string::npos || dot_index_ + 1 >= ns_.size()) {
			return std::string();
		}
		return ns_.substr(dot_index_+1);
	}
	
	std::string ns() {
		return ns_;
	}
private:
		std::string ns_;
		size_t dot_index_;
};


class MongoSync {
public:
	static MongoSync* NewMongoSync(const Options *opt);
	static mongo::DBClientConnection* ConnectAndAuth(const std::string &srv_ip_port,
                                                   const std::string &auth_db,
                                                   const std::string &user,
                                                   const std::string &passwd,
                                                   const bool use_mcr,
                                                   const bool bg = false);
	MongoSync(const Options *opt);
	~MongoSync();
	int32_t InitConn();
  bool GetReadableHost(std::string* readable_host);

  // Used when sourse is mongos
  std::vector<std::string> GetShards();
  bool IsBigChunkExist();
  bool IsBalancerRunning();

  void MongosGetOplogOption();
  void MongosCloneDb();
  void MongosSyncOplog();
  void GetAllDb(std::vector<std::string>* all_dbs);

	void Process();
	void CloneOplog();
	void CloneAllDb();
	void CloneDb(std::string db = "");
	void CloneColl(std::string src_ns, std::string dst_ns, int32_t batch_size);
	void SyncOplog();

private:
	Options opt_;
	mongo::DBClientConnection* src_conn_;
	mongo::DBClientConnection* dst_conn_;
	std::string src_version_;
	std::string dst_version_;

	OplogTime oplog_begin_;
	OplogTime oplog_finish_;

	//const static std::string oplog_ns_ = "local.oplog.rs"; // TODO: Is it const
	const static std::string oplog_ns_;

  // Used for LOG
  std::string MONGOSYNC_PROMPT;

  //backgroud thread for Batch write
  util::BGThreadGroup *oplog_bg_thread_group_[OPLOG_APPLY_THREADNUM]; 
  util::BGThreadGroup bg_thread_group_; 

	void CloneCollIndex(std::string sns, std::string dns);
	void GenericProcessOplog(OplogProcessOp op);
	static void *ProcessSingleOplog(void *args);
	static void ApplyInsertOplog(mongo::DBClientConnection* dst_conn,
                               const std::string& dst_db, const std::string& dst_coll,
                               const mongo::BSONObj& oplog);
	static void ApplyCmdOplog(mongo::DBClientConnection* dst_conn,
                            std::string src_db,
                            std::string dst_db, const std::string& dst_coll,
                            const mongo::BSONObj& oplog, bool same_coll = true);
	OplogTime GetSideOplogTime(mongo::DBClientConnection* conn, std::string ns, std::string db, std::string coll, bool first_or_last); //first_or_last==true->get the first timestamp; first_or_last==false->get the last timestamp

	static std::string GetMongoVersion(mongo::DBClientConnection* conn);
	int GetCollIndexesByVersion(mongo::DBClientConnection* conn, std::string version, std::string ns, mongo::BSONObj& indexes);
	static void SetCollIndexesByVersion(mongo::DBClientConnection* conn, std::string version, std::string coll_full_name, mongo::BSONObj index);
	int GetAllCollByVersion(mongo::DBClientConnection* conn, std::string version, std::string db, std::vector<std::string>& colls);
	
	bool need_clone_oplog() {
		return opt_.raw_oplog;
	}

	bool need_clone_all_db() {
		return !opt_.raw_oplog && opt_.db.empty() && opt_.coll.empty() && opt_.oplog_start.empty() && opt_.oplog_end.empty();
	}

	bool need_clone_db() {
		return !opt_.raw_oplog && !opt_.db.empty() && opt_.coll.empty() && opt_.oplog_start.empty() && opt_.oplog_end.empty();
		/*
		 * opt_.raw_oplog && (!opt_.db.empty() && opt_.coll.empty() && opt_.oplog_start.empty() && opt_.oplog_end.empty()
		 * || opt_.db.empty() && opt_.coll.empty() && opt_.oplog_start.empty() && opt_.oplog_end.empty());
		 * 
		 * */
	}

	bool need_clone_coll() {
		return !opt_.raw_oplog && !opt_.coll.empty() && opt_.oplog_start.empty() && opt_.oplog_end.empty();
	}

	bool need_sync_oplog() {
		return !opt_.raw_oplog && opt_.oplog;
	}

	MongoSync(const MongoSync&);
	MongoSync& operator=(const MongoSync&);
};
#endif
