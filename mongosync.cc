#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <string>
#include <iomanip>

#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include "util.h"
#include "mongosync.h"
#include "mongo/util/mongoutils/str.h"

#define str(a) #a
#define xstr(a) str(a)

extern int shards_num;

static void GetSeparateArgs(const std::string& raw_str, std::vector<std::string>* argv_p) {
  const char* p = raw_str.data();
  const char *pch = strtok(const_cast<char*>(p), " ");
  while (pch != NULL) {
    argv_p->push_back(std::string(pch));
    pch = strtok(NULL, " ");
  }
}

static bool before(const OplogTime& t1, const OplogTime& t2) {
	return *reinterpret_cast<const uint64_t*>(&t1) < *reinterpret_cast<const uint64_t*>(&t2);	
}

static void Usage() {
	std::cerr << "Follow is the mongosync-surpported options: " << std::endl;	
	std::cerr << "--help                   to get the help message" << std::endl;
  std::cerr << "-c conf.file             use config file to start mongosync" << std::endl;
	std::cerr << "--src_srv arg            the source mongodb server's ip port" << std::endl;
	std::cerr << "--src_user arg           the source mongodb server's logging user" << std::endl;
	std::cerr << "--src_passwd arg         the source mongodb server's logging password" << std::endl;
	std::cerr << "--src_auth_db arg        the source mongodb server's auth db" << std::endl;
	std::cerr << "--src_use_mcr            force source connection to use MONGODB-CR password machenism" << std::endl;
	std::cerr << "--dst_srv arg            the destination mongodb server's ip port" << std::endl;
	std::cerr << "--dst_user arg           the destination mongodb server's logging user" << std::endl;
	std::cerr << "--dst_passwd arg         the destination mongodb server's logging password" << std::endl;
	std::cerr << "--dst_auth_db arg        the destination mongodb server's auth db" << std::endl;
	std::cerr << "--is_mongos              the source mongodb server is mongos" << std::endl;
	std::cerr << "--shard_user arg         the source mongos server's shard username" << std::endl;
	std::cerr << "--shard_passwd arg       the source mongos server's shard password" << std::endl;
	std::cerr << "--dst_use_mcr            force destination connection to use MONGODB-CR password machenism" << std::endl;
	std::cerr << "--db arg                 the source database to be cloned" << std::endl;
	std::cerr << "--dst_db arg             the destination database" << std::endl;
	std::cerr << "--coll arg               the source collection to be cloned" << std::endl;
	std::cerr << "--colls arg              the source collection list name" << std::endl;
	std::cerr << "--dst_coll arg           the destination collection" << std::endl;
	std::cerr << "--oplog                  whether to sync oplog" << std::endl;
	std::cerr << "--raw_oplog              whether to only clone oplog" << std::endl;
	std::cerr << "--op_start arg           the start timestamp to sync oplog" << std::endl;
	std::cerr << "--op_end arg             the start timestamp to sync oplog" << std::endl;
	std::cerr << "--dst_op_ns arg          the destination namespace for raw oplog mode" << std::endl;
	std::cerr << "--no_index               whether to clone the db or collection corresponding index" << std::endl;
	std::cerr << "--filter arg             the bson format string used to filter the records to be transfered" << std::endl;
	std::cerr << "--bg_num arg             the background thread number for cloning data(not oplog syncing and oplog storing)" << std::endl;
	std::cerr << "--batch_size arg         the data grouping size criterion in cloning data(0-16M, default to 16M), unit is Byte" << std::endl;
	std::cerr << "--log_level arg          specify the log level(INFO, WARN, FATAL)" << std::endl;
}

#define CHECK_ARGS_NUM() \
	  if (argc <= idx + 1) { \
			    LOG(FATAL) << "Wrong argument number" << std::endl; \
			    Usage(); \
			    exit(-1); \
		} \
  	while(0)

void Options::ParseCommand(int argc, char** argv) {
	int32_t idx = 0;
	CHECK_ARGS_NUM();
	++idx;
	std::string time_str;
	int32_t commas_pos;
	while (idx < argc) {
		if (strcasecmp(argv[idx], "--help") == 0) {
      std::cout << "Git Ver: " << xstr(_GITVER_) << std::endl;
      std::cout << "Date:    " << xstr(_COMPILEDATE_) << std::endl;
			Usage();
			exit(0);
    } else if (strcasecmp(argv[idx], "--src_srv") == 0) {
			CHECK_ARGS_NUM();
			src_ip_port = argv[++idx];
		} else if (strcasecmp(argv[idx], "--src_user") == 0) {
			CHECK_ARGS_NUM();
			src_user = argv[++idx];
		} else if (strcasecmp(argv[idx], "--src_passwd") == 0) {
			CHECK_ARGS_NUM();
			src_passwd = argv[++idx];
		} else if (strcasecmp(argv[idx], "--src_auth_db") == 0) {
			CHECK_ARGS_NUM();
			src_auth_db = argv[++idx];
		} else if (strcasecmp(argv[idx], "--src_use_mcr") == 0) {
			src_use_mcr = true;
		} else if (strcasecmp(argv[idx], "--dst_srv") == 0) {
			CHECK_ARGS_NUM();
			dst_ip_port = argv[++idx];
		} else if (strcasecmp(argv[idx], "--dst_user") == 0) {
			CHECK_ARGS_NUM();
			dst_user = argv[++idx];
		} else if (strcasecmp(argv[idx], "--dst_passwd") == 0) {
			CHECK_ARGS_NUM();
			dst_passwd = argv[++idx];
		} else if (strcasecmp(argv[idx], "--dst_auth_db") == 0) {
			CHECK_ARGS_NUM();
			dst_auth_db = argv[++idx];
		} else if (strcasecmp(argv[idx], "--is_mongos") == 0) {
      is_mongos = true;
		} else if (strcasecmp(argv[idx], "--shard_user") == 0) {
			CHECK_ARGS_NUM();
			shard_user = argv[++idx];
		} else if (strcasecmp(argv[idx], "--shard_passwd") == 0) {
			CHECK_ARGS_NUM();
			shard_passwd = argv[++idx];
		} else if (strcasecmp(argv[idx], "--dst_use_mcr") == 0) {
			dst_use_mcr = true;
		} else if (strcasecmp(argv[idx], "--db") == 0) {
			CHECK_ARGS_NUM();
			db = argv[++idx];
		} else if (strcasecmp(argv[idx], "--dst_db") == 0) {
			CHECK_ARGS_NUM();
			dst_db = argv[++idx];
		} else if (strcasecmp(argv[idx], "--coll") == 0) {
			CHECK_ARGS_NUM();
			coll = argv[++idx];
		} else if (strcasecmp(argv[idx], "--colls") == 0) {
			CHECK_ARGS_NUM();
			colls = argv[++idx];
		} else if (strcasecmp(argv[idx], "--dst_coll") == 0) {
			CHECK_ARGS_NUM();
			dst_coll = argv[++idx];
		} else if (strcasecmp(argv[idx], "--oplog") == 0) {
			oplog = true;
		} else if (strcasecmp(argv[idx], "--raw_oplog") == 0) {
			raw_oplog = true;
		} else if (strcasecmp(argv[idx], "--op_start") == 0) {
			CHECK_ARGS_NUM();
			time_str = argv[++idx];
			commas_pos = time_str.find(",");
			oplog_start.sec = atoi(time_str.substr(0, commas_pos).c_str());
			oplog_start.no = atoi(time_str.substr(commas_pos+1).c_str());
		} else if (strcasecmp(argv[idx], "--op_end") == 0) {
			CHECK_ARGS_NUM();
			time_str = argv[++idx];
			commas_pos = time_str.find(",");
			oplog_end.sec = atoi(time_str.substr(0, commas_pos).c_str());
			oplog_end.no = atoi(time_str.substr(commas_pos+1).c_str());
		} else if (strcasecmp(argv[idx], "--dst_op_ns") == 0) {
			CHECK_ARGS_NUM();
			dst_oplog_ns = argv[++idx];
		} else if (strcasecmp(argv[idx], "--no_index") == 0) {
			no_index = true;
		} else if (strcasecmp(argv[idx], "--filter") == 0) {
			CHECK_ARGS_NUM();
			filter = mongo::Query(argv[++idx]);
		} else if (strcasecmp(argv[idx], "--bg_num") == 0) {
			CHECK_ARGS_NUM();
			bg_num = atoi(argv[++idx]);
		} else if (strcasecmp(argv[idx], "--batch_size") == 0) {
			CHECK_ARGS_NUM();
			batch_size = atoi(argv[++idx]);
			if (batch_size < 0 || batch_size > MAX_BATCH_BUFFER_SIZE) {
				LOG(WARN) << "The batch size specified is beyond the range, set to " << MAX_BATCH_BUFFER_SIZE << std::endl;
				batch_size = MAX_BATCH_BUFFER_SIZE;
			}
		} else if (strcasecmp(argv[idx], "--log_level") == 0) {
			CHECK_ARGS_NUM();
			log_level = argv[++idx];
		} else {
			LOG(FATAL) << "Unkown options" << std::endl;
			Usage();
			exit(-1);
		}
		++idx;
	}
}

void Options::LoadConf(const std::string &conf_file) {
  std::ifstream in(conf_file.c_str());
  if (!in.good()) {
		LOG(FATAL) << "cannot open the specified conf file" << std::endl;
    exit(-1);
  }
  std::string line, item_key, item_value;
  std::string::size_type pos;
  int32_t line_no;
  while (!in.eof()) {
    ++line_no;
    getline(in, line);

    line = util::Trim(line);
    if (line.empty() || line.at(0) == '#') {
      continue;
    }
    if ((pos = line.find("=")) == std::string::npos
        || pos == (line.size() - 1)) {
      LOG(WARN) << "error item format at " << line_no << "line in " << conf_file << std::endl;
      continue;
    }

    item_key = line.substr(0, pos);
    item_key = util::Trim(item_key);
    item_value = line.substr(pos + 1);
    item_value = util::Trim(item_value);

    items_[item_key] = item_value; 
  }
  in.close();

  GetConfStr("src_srv", &src_ip_port);
  GetConfStr("src_user", &src_user);
  GetConfStr("src_passwd", &src_passwd);
  GetConfStr("src_auth_db", &src_auth_db);
  GetConfBool("src_use_mcr", &src_use_mcr);
  GetConfStr("db", &db);
  GetConfStr("coll", &coll);
  GetConfStr("colls", &colls);

  GetConfBool("is_mongos", &is_mongos);
  GetConfStr("shard_user", &shard_user);
  GetConfStr("shard_passwd", &shard_passwd);

  GetConfStr("dst_srv", &dst_ip_port);
  GetConfStr("dst_user", &dst_user);
  GetConfStr("dst_passwd", &dst_passwd);
  GetConfStr("dst_auth_db", &dst_auth_db);
  GetConfBool("dst_use_mcr", &dst_use_mcr);
  GetConfStr("dst_db", &dst_db);
  GetConfStr("dst_coll", &dst_coll);

  GetConfBool("oplog", &oplog);   
  GetConfBool("raw_oplog", &raw_oplog);   
  GetConfOplogTime("op_start", &oplog_start);   
  GetConfOplogTime("op_end", &oplog_end);
  GetConfStr("dst_op_ns", &dst_oplog_ns);
  GetConfBool("no_index", &no_index);

  GetConfQuery("filter", &filter);    

	GetConfInt("bg_num", &bg_num);
	GetConfInt("batch_size", &batch_size);

	GetConfStr("log_level", &log_level);
}


#define CHECK_ITEM_EXIST(item_key, iter) \
  std::map<std::string, std::string>::const_iterator iter; \
  do { \
    if ((iter = items_.find(item_key)) == items_.end()) { \
      return false; \
    } \
  } while (false)

bool Options::GetConfBool(const std::string &item_key, bool *value) {
  CHECK_ITEM_EXIST(item_key, iter);
  
  if (strcasecmp(iter->second.c_str(), "on") == 0) {
    *value = true;
  } else if (strcasecmp(iter->second.c_str(), "off") == 0) {
    *value = false;
  } else {
    LOG(WARN) << item_key << " item in conf file is not BOOL" << std::endl;
    return false;
  }
  return true;
}

int32_t Options::GetConfInt(const std::string &item_key, int32_t *value) {
	CHECK_ITEM_EXIST(item_key, iter);

	*value = atoi(iter->second.c_str());
	return true;
}

bool Options::GetConfStr(const std::string &item_key, std::string *value) {
  CHECK_ITEM_EXIST(item_key, iter);
  
  *value = iter->second;
  return true;
}

bool Options::GetConfQuery(const std::string &item_key, mongo::Query *value) {
  CHECK_ITEM_EXIST(item_key, iter);
  
  *value = mongo::Query(iter->second);
  return true;
}

bool Options::GetConfOplogTime(const std::string &item_key, OplogTime *value) {
  CHECK_ITEM_EXIST(item_key, iter);
  
  std::string::size_type commas_pos = iter->second.find(",");
  value->sec = atoi(iter->second.substr(0, commas_pos).c_str());
  value->no = atoi(iter->second.substr(commas_pos+1).c_str());
  return true;
}

bool Options::ValidCheck() {
	if (db.empty() && !coll.empty()) {
		LOG(FATAL) << "source collection is specified, but source database not" << std::endl;
		return false;
	}

	if (dst_db.empty() && !dst_coll.empty()) {
		LOG(FATAL) << "destination collection is specified, but destination database not" << std::endl;
		return false;
	}
	
	if (!dst_coll.empty() && coll.empty()) {
		LOG(FATAL) << "destination collection is specified, but source collection not" << std::endl;
		return false;
	}

	if (db.empty() && !dst_db.empty()) {
		LOG(FATAL) << "destination database is specified, but the source database not" << std::endl;
		return false;
	}

	return true;
}


MongoSync::MongoSync(const Options *opt)
	: opt_(*opt),
	src_conn_(NULL),
	dst_conn_(NULL),
  bg_thread_group_(opt->dst_ip_port, opt->dst_auth_db, opt->dst_user,
                   opt->dst_passwd, opt->dst_use_mcr, opt->bg_num) {
  if (opt->is_mongos) {
    // [mongosync_127.0.0.1:8099]
    MONGOSYNC_PROMPT = PROMPT_PREFIX + "_" + opt->src_ip_port + "]\t";
  } else {
    // [mongosync]
    MONGOSYNC_PROMPT = PROMPT_PREFIX + "]\t";
  }
  for (int i = 0; i < OPLOG_APPLY_THREADNUM; ++i) {
    oplog_bg_thread_group_[i] =
      new util::BGThreadGroup(opt->dst_ip_port, opt->dst_auth_db, opt->dst_user,
                   opt->dst_passwd, opt->dst_use_mcr, 1, true);
  }
}


MongoSync::~MongoSync() {
	if (src_conn_) {
		delete src_conn_;
	}
	if (dst_conn_) {
		delete dst_conn_;
	}
  for (int i = 0; i < OPLOG_APPLY_THREADNUM; ++i) {
    delete oplog_bg_thread_group_[i];
  }
}

MongoSync* MongoSync::NewMongoSync(const Options *opt) {
	MongoSync* mongosync = new MongoSync(opt);
	if (mongosync->InitConn() == -1) {
		delete mongosync;
		return NULL;
	}
	return mongosync;
}

bool MongoSync::GetReadableHost(std::string* readable_host) {
  mongo::BSONObj tmp;
  if (!src_conn_->runCommand("admin", BSON("replSetGetStatus" << 1), tmp, mongo::QueryOption_SlaveOk)) {
    LOG(FATAL) << "runCommand  falied replSetGetStatus" << std::endl;
    return false;
  }
  if (!tmp.hasField("members")) {
    return false;
  }
  mongo::BSONObj members = tmp.getObjectField("members");
  for (int i = 0; i < members.nFields(); i++) {
    mongo::BSONObj member = members[i].Obj();
    std::string info = member.getStringField("stateStr");
    if (info == "SECONDARY") {
      readable_host->assign(member.getStringField("name"));
      return true;
    } else if (info == "PRIMARY") {
      readable_host->assign(member.getStringField("name"));
    }
  }

  return true;
}

mongo::DBClientConnection* MongoSync::ConnectAndAuth(const std::string &srv_ip_port,
                                                     const std::string &auth_db,
                                                     const std::string &user,
                                                     const std::string &passwd,
                                                     const bool use_mcr,
                                                     const bool bg) {
	std::string errmsg;
	mongo::DBClientConnection* conn = NULL;
	conn = new mongo::DBClientConnection();	
	if (!conn->connect(srv_ip_port, errmsg)) {
		LOG(FATAL) << util::GetFormatTime() << "connect to srv: " << srv_ip_port
      << " failed, with errmsg: " << errmsg << std::endl;
		delete conn;
		return NULL;
	}
  if (!bg) {
    LOG(INFO) << util::GetFormatTime() << "connect to srv_rsv: " << srv_ip_port
      << " ok!" << std::endl;
  }

	if (!passwd.empty()) {
		if (!conn->auth(auth_db, user, passwd, errmsg, use_mcr)) {
			LOG(FATAL) << util::GetFormatTime() << "srv: " << srv_ip_port
        << ", dbname: " << auth_db << " failed" << std::endl; 
			delete conn;
			return NULL;
		}
    if (!bg) {
		  LOG(INFO) << util::GetFormatTime() << "srv: " << srv_ip_port
        << ", dbname: " << auth_db << " ok!" << std::endl;
    }
	}
	return conn;
}

int32_t MongoSync::InitConn() {
	if (!(src_conn_ = ConnectAndAuth(opt_.src_ip_port,
                                   opt_.src_auth_db,
                                   opt_.src_user,
                                   opt_.src_passwd,
                                   opt_.src_use_mcr)) ||
      !(dst_conn_ = ConnectAndAuth(opt_.dst_ip_port,
                                   opt_.dst_auth_db,
                                   opt_.dst_user,
                                   opt_.dst_passwd,
                                   opt_.dst_use_mcr))) {
		return -1;	
	}
	src_version_ = GetMongoVersion(src_conn_);
	dst_version_ = GetMongoVersion(dst_conn_);
	return 0;
}

std::vector<std::string> MongoSync::GetShards() {
  std::vector<std::string> shards;
  std::auto_ptr<mongo::DBClientCursor> cursor = \
    src_conn_->query("config.shards",
                     mongo::Query().snapshot(), 0, 0, NULL,
                     mongo::QueryOption_SlaveOk | mongo::QueryOption_NoCursorTimeout);
  if (cursor.get() == NULL) {
    return shards;
  }
  mongo::BSONObj tmp;
  while (cursor->more()) {
    tmp = cursor->next();
    std::string shard = tmp.getStringField("host");
    shards.push_back(shard);
  }
  return shards;
}

bool MongoSync::IsBigChunkExist() {
	mongo::BSONObj obj = src_conn_->findOne("config.chunks",
                                          mongo::Query(BSON("jumbo" << "true")),
                                          NULL,
                                          mongo::QueryOption_SlaveOk).getOwned();
  if (!obj.isEmpty())
    return true;
  return false;
}

bool MongoSync::IsBalancerRunning() {
	mongo::BSONObj obj = src_conn_->findOne("config.settings",
                                          mongo::Query(BSON("_id" << "balancer")),
                                          NULL,
                                          mongo::QueryOption_SlaveOk).getOwned();
  if (obj.isEmpty())
    return true;

  std::string balancer_state = obj.jsonString();
  if (balancer_state.find("stopped") != std::string::npos &&
      balancer_state.find("true") != std::string::npos)
    return false;

  return true;
}

void MongoSync::MongosGetOplogOption() {
  oplog_begin_ = opt_.oplog_start;
  if ((need_clone_all_db() || need_clone_db() || need_clone_coll()) && opt_.oplog_start.empty()) {
    oplog_begin_ = GetSideOplogTime(src_conn_, oplog_ns_, "", "", false);
  } else if (opt_.oplog_start.empty()) {
    oplog_begin_ = GetSideOplogTime(src_conn_, oplog_ns_, opt_.db, opt_.coll, true);
  }
  oplog_finish_ = opt_.oplog_end;
}

void MongoSync::MongosCloneDb() {
	if (need_clone_all_db()) {
		CloneAllDb();	
	} else if (need_clone_db()) {
		CloneDb();
	} else if (need_clone_coll()) {
		std::string sns = opt_.db + "." + opt_.coll;
		std::string dns = (opt_.dst_db.empty() ? opt_.db : opt_.dst_db) + "." + (opt_.dst_coll.empty() ? opt_.coll : opt_.dst_coll);
		CloneColl(sns, dns, opt_.batch_size);
	}
}

void MongoSync::MongosSyncOplog() {
	if (need_sync_oplog()) {
		SyncOplog();
	}
}

void MongoSync::Process() {
  oplog_begin_ = opt_.oplog_start;
  if ((need_clone_all_db() || need_clone_db() || need_clone_coll()) && opt_.oplog_start.empty()) {
    oplog_begin_ = GetSideOplogTime(src_conn_, oplog_ns_, "", "", false);
  } else if (opt_.oplog_start.empty()) {
    oplog_begin_ = GetSideOplogTime(src_conn_, oplog_ns_, opt_.db, opt_.coll, true);
  }
  oplog_finish_ = opt_.oplog_end;

  if (need_clone_oplog()) {
		CloneOplog();
		return;
	}

	if (need_clone_all_db()) {
		CloneAllDb();	
	} else if (need_clone_db()) {
		CloneDb();
	} else if (need_clone_coll()) {
		std::string sns = opt_.db + "." + opt_.coll;
		std::string dns = (opt_.dst_db.empty() ? opt_.db : opt_.dst_db) + "." + (opt_.dst_coll.empty() ? opt_.coll : opt_.dst_coll);
		CloneColl(sns, dns, opt_.batch_size);
	}

	if (need_sync_oplog()) {
		SyncOplog();
	}
}

void MongoSync::CloneOplog() {
	GenericProcessOplog(kClone);
}

void MongoSync::SyncOplog() {
	GenericProcessOplog(kApply);		
}

void MongoSync::GenericProcessOplog(OplogProcessOp op) {
	mongo::Query query;	

	if (/* !opt_.db.empty() &&*/ opt_.coll.empty()) { // both specifying db name and not specifying
		query = mongo::Query(BSON("$or" << BSON_ARRAY(BSON("ns" << BSON("$regex" << ("^"+opt_.db))) << BSON("ns" << "admin.$cmd")) << "ts" << mongo::GTE << oplog_begin_.timestamp() << mongo::LTE << oplog_finish_.timestamp())); //TODO: this cannot exact out the opt_.db related oplog, but the opt_.db-prefixed related oplog
	} else if (!opt_.db.empty() && !opt_.coll.empty()) {
		NamespaceString ns(opt_.db, opt_.coll);
		query = mongo::Query(BSON("$or" << BSON_ARRAY(BSON("ns" << ns.ns()) << BSON("ns" << ns.db() + ".system.indexes") << BSON("ns" << ns.db() + ".$cmd") << BSON("ns" << "admin.$cmd"))
					<< "ts" << mongo::GTE << oplog_begin_.timestamp() << mongo::LTE << oplog_finish_.timestamp()));
	}
  int retries = 3;
  LOG(INFO) << MONGOSYNC_PROMPT << "Start sync oplog" << query.toString() << std::endl;
retry:
	std::auto_ptr<mongo::DBClientCursor> cursor = src_conn_->query(oplog_ns_, query, 0, 0, NULL,
                                                                 mongo::QueryOption_CursorTailable |
                                                                 mongo::QueryOption_AwaitData |
                                                                 mongo::QueryOption_NoCursorTimeout |
                                                                 mongo::QueryOption_SlaveOk);

  try {
    query = mongo::Query(BSON("ts" << oplog_begin_.timestamp()));
    mongo::BSONObj obj = src_conn_->findOne(oplog_ns_, query, NULL, mongo::QueryOption_SlaveOk);
    if (obj.isEmpty()) {
      LOG(FATAL) << "Can not find oplog at" << oplog_begin_.sec << ","
        << oplog_begin_.no << " " << query.toString() << std::endl;
      exit(-1);
    }
  } catch (mongo::DBException& e) {
    LOG(FATAL) << "find oplog DBException: " << e.toString() << std::endl;
    exit(-1);
  }

	std::string dst_db, dst_coll;
	if (need_clone_oplog()) {
		NamespaceString ns(opt_.dst_oplog_ns);
		dst_db = ns.db(),
		dst_coll = ns.coll(); 
	} else {
		dst_db = opt_.dst_db;
		dst_coll = opt_.dst_coll; 
	}

	mongo::BSONObj oplog;	
	OplogTime cur_times, pre_times(0, 0);
	char time_buf[32];
	bool waiting = false;
  bool cursor_dead = false;
	int64_t pre = 0, cur;
  util::OplogArgs *args;

	while (true) {
    try {
      while (!cursor->more()) {
        if (cursor->isDead()) {
          sleep(1);
          mongo::BSONObj error;
          if (cursor->peekError(&error)) {
            LOG(WARN) << MONGOSYNC_PROMPT << error.toString() << std::endl;
          } else {
            LOG(WARN) << MONGOSYNC_PROMPT << "no cursor error" << std::endl;
          }
          cursor_dead = true;
          LOG(WARN) << MONGOSYNC_PROMPT << "cursor is dead, try rebuild" << std::endl;
          cursor = src_conn_->query(oplog_ns_, query, 0, 0, NULL,
                                    mongo::QueryOption_CursorTailable |
                                    mongo::QueryOption_AwaitData |
                                    mongo::QueryOption_NoCursorTimeout |
                                    mongo::QueryOption_SlaveOk);
          continue;
        }
        if (oplog_finish_.no != 2147483647 && oplog_finish_.sec != 2147483647) {
          if (!cur_times.empty() && before(pre_times, cur_times)) {
            LOG(INFO) << util::GetFormatTime() << MONGOSYNC_PROMPT << "synced up to " << cur_times.sec << "," << cur_times.no << " (" << util::GetFormatTime(cur_times.sec) << ")" << std::endl;
          }
          return;
        }

        if (!waiting) {
          if (!cur_times.empty() && before(pre_times, cur_times)) {
            pre_times = cur_times;	
            LOG(INFO) << util::GetFormatTime() << MONGOSYNC_PROMPT << "synced up to " << cur_times.sec << "," << cur_times.no << " (" << util::GetFormatTime(cur_times.sec) << ")" << std::endl;
          }
          LOG(INFO) << util::GetFormatTime() << MONGOSYNC_PROMPT << "waiting for new data..." << std::endl;
          waiting = true;
        }
        usleep(500000);
      }
      // cursor dead but rebuild
      if (cursor_dead) {
        cursor_dead = false;
        LOG(WARN) << MONGOSYNC_PROMPT << "cursor rebuild success!" << std::endl;
      }
      waiting = false;

      oplog = cursor->next().getOwned();

      std::string oplog_ns = oplog.getStringField("ns");	
      std::string type;
      std::string oplog_id;
      mongo::BSONObj id_obj;
      int hash_id = 0;

      if (oplog.isEmpty()) {
        LOG(WARN) << "oplog is empty" << std::endl;
        goto pass_oplog;
      }
      if (mongo::str::endsWith(oplog_ns, ".system.users")) { //filter all user related oplog, 2.4=>db.system.users, 3.x=>admin.system.users
        goto pass_oplog;
      }
      if (!opt_.db.empty() && opt_.coll.empty() && oplog_ns != "admin.$cmd") { //filter the same prefix db names, because of cannot filtering this situation with regex, and pass renameCollection command oplog(in admin.$cmd)
        std::string sns = opt_.db + ".";
        if (oplog_ns.size() < sns.size() 
            || oplog_ns.substr(0, sns.size()) != sns) {
          goto pass_oplog;
        }
      }

      if (mongo::str::endsWith(oplog_ns.c_str(), ".system.indexes")) {
        if (opt_.no_index) {
          goto pass_oplog;
        }
        std::string op_ns = oplog.getObjectField("o").getStringField("ns");
        if (!opt_.coll.empty() && NamespaceString(op_ns).coll() != opt_.coll) {
          goto pass_oplog;
        }
      }
      type = oplog.getStringField("op");
      if (type.empty()) {
        LOG(WARN) << "oplog doesn't not has \"op\" filed" << std::endl;
        goto pass_oplog;
      }
      if (type.at(0) == 'c') {
        if ((dst_db == "local") || (dst_db == "admin") && dst_coll != "$cmd") { //for dst_db = admin and dst_coll == $cmd situation's filtering is to be done later
          goto pass_oplog;
        }
        if (!opt_.coll.empty() && !mongo::str::endsWith("." + std::string(oplog.getObjectField("o").firstElement().valuestr()), "." + opt_.coll)) { // filter the only coll-related oplog
          goto pass_oplog;
        }
      }

      // Get oplog id to hash
      if (type.at(0) == 'u') {
        id_obj = oplog.getObjectField("o2");
      } else {
        id_obj = oplog.getObjectField("o");
      }
      if (!id_obj.isEmpty()) {
        mongo::BSONElement e;
        if (id_obj.getObjectID(e)) {
          oplog_id = e.__oid().toString();
          if (oplog_id.empty()) {
            goto pass_oplog;
          }
          hash_id = static_cast<char>(oplog_id[oplog_id.size() - 1]) % OPLOG_APPLY_THREADNUM;
        }

        args = new util::OplogArgs();
        args->db = opt_.db;
        args->coll = opt_.coll;
        args->dst_db = dst_db;
        args->dst_coll = dst_coll;
        args->oplog = oplog.copy();
        args->op = op;
        args->promot = MONGOSYNC_PROMPT;
        oplog_bg_thread_group_[hash_id]->AddWriteUnit(args, MongoSync::ProcessSingleOplog);
        memcpy(&cur_times, oplog["ts"].value(), 2*sizeof(int32_t));
      }

pass_oplog:
      time(&cur);
      if (cur > pre && !cur_times.empty() && before(pre_times, cur_times)) {
        pre = cur;
        if (cur - cur_times.sec < 20) {
          shards_num--;
          if (shards_num < 0)
            shards_num = 0;
        }
        if (shards_num == 0) {
          LOG(INFO) << MONGOSYNC_PROMPT << "Syncronization almost done" << std::endl;
        }
        pre_times = cur_times;
        LOG(INFO) << util::GetFormatTime() << MONGOSYNC_PROMPT << "synced up to " << cur_times.sec << "," << cur_times.no << " (" << util::GetFormatTime(cur_times.sec) << ")" << std::endl;
      }
    } catch (mongo::DBException &e) {
      LOG(WARN) << "sync oplog exception occurs: " << opt_.src_ip_port << e.toString() << ", retry it" << std::endl;
      if (retries--) {
        delete args;
        goto retry;
      } else {
        LOG(FATAL) << "sync oplog occurs exception 3 times, exit" << std::endl;
        exit(-1);
      }
    }
  }
}

void MongoSync::GetAllDb(std::vector<std::string>* all_dbs) {
	mongo::Query q = BSON("listDatabases" << 1);
	mongo::BSONObj obj = src_conn_->findOne("admin.$cmd", q, NULL, mongo::QueryOption_SlaveOk).getObjectField("databases").getOwned();
	std::set<std::string> fields;
	obj.getFieldNames(fields);
	std::string db;

	for (std::set<std::string>::iterator iter = fields.begin(); iter != fields.end(); ++iter) {
		db = obj.getObjectField(*iter).getStringField("name");
		if (db == "admin" ||
        db == "local" ||
        db == "config") {
			continue;
		}
    all_dbs->push_back(db);
	}
}

void MongoSync::CloneAllDb() {
  std::vector<std::string> all_dbs;
  GetAllDb(&all_dbs);
  for (int i = 0; i < all_dbs.size(); i++) {
    CloneDb(all_dbs[i]);
  }
}

void MongoSync::CloneDb(std::string db) {
	if (db == "") {
		db = opt_.db;
	}

	std::vector<std::string> colls;
  if (!opt_.colls.empty()) {
    util::Split(opt_.colls, ',', colls);
  } else if (GetAllCollByVersion(src_conn_, src_version_, db, colls) == -1) { // get collections failed
    return;
  }

	std::string dst_db = opt_.dst_db.empty() ? db : opt_.dst_db;
  LOG(INFO) << util::GetFormatTime() << MONGOSYNC_PROMPT << "cloning db: " << db << " ----> " << dst_db << "\n" << std::endl;
	for (std::vector<std::string>::const_iterator iter = colls.begin();
			iter != colls.end();
			++iter) {
		CloneColl(db + "." + *iter, dst_db + "." + *iter, opt_.batch_size);
	}
  LOG(INFO) << util::GetFormatTime() << MONGOSYNC_PROMPT << "clone db: " << db << " ----> " << dst_db << " finished\n" << std::endl;
}

void MongoSync::CloneColl(std::string src_ns, std::string dst_ns, int batch_size) {
//	std::string src_ns = opt_.db + "." + opt_.coll;
//	std::string dst_ns = (opt_.dst_db.empty() ? opt_.db : opt_.dst_db) + "." + (opt_.dst_coll.empty() ? opt_.coll : opt_.dst_coll);
	LOG(INFO) << util::GetFormatTime() << MONGOSYNC_PROMPT << "cloning "	<< src_ns << " to " << dst_ns << std::endl;
	uint64_t total = src_conn_->count(src_ns, opt_.filter, mongo::QueryOption_SlaveOk | mongo::QueryOption_NoCursorTimeout), cnt = 0;
	std::auto_ptr<mongo::DBClientCursor> cursor;
	int32_t acc_size, percent, retries = 3;
	uint64_t time_pre, time_cur;
	char buf[32];

  // Clone index
	if (!opt_.no_index) {
		CloneCollIndex(src_ns, dst_ns);
	}

  // Clone record
retry:
	cursor = src_conn_->query(src_ns, opt_.filter.snapshot(), 0, 0, NULL,
                            mongo::QueryOption_AwaitData | mongo::QueryOption_SlaveOk | mongo::QueryOption_NoCursorTimeout);
	std::vector<mongo::BSONObj> *batch = new std::vector<mongo::BSONObj>; //to be deleted by bg thread
	acc_size = 0;
	percent = 0;
	time_pre = 0;
	cnt = 0;
	try {
		while (cursor->more()) {
			mongo::BSONObj obj = cursor->next();
			acc_size += obj.objsize();
			batch->push_back(obj.getOwned());
			if (acc_size >= batch_size) {
  	    bg_thread_group_.AddWriteUnit(dst_ns, batch);
  	    batch = new std::vector<mongo::BSONObj>;
				acc_size = 0;
			}
			++cnt;
			if (!(cnt & 0xFF)) {
				time_cur = time(NULL);
				if (time_cur > time_pre) {
					percent = cnt * 100 / total;
					snprintf(buf, sizeof(buf), "%d/%d", cnt, total);
					LOG(INFO) << util::GetFormatTime() << MONGOSYNC_PROMPT << "cloing progress: " << std::setfill(' ') << std::setw(20) << buf << "\t"<< percent << "%" << "\t(objects)" << std::endl;
					time_pre = time_cur;
				}
			}
		}
		if (!batch->empty()) {
//      dst_conn_->insert(dst_ns, batch, mongo::InsertOption_ContinueOnError, &mongo::WriteConcern::unacknowledged);
  	  bg_thread_group_.AddWriteUnit(dst_ns, batch);
		}
	} catch (mongo::DBException &e) {
		LOG(WARN) << "exception occurs: " << opt_.src_ip_port << e.toString() << ", retry it" << std::endl;
		delete batch;
		if (retries--) {
			goto retry;
		} else {
			LOG(FATAL) << "occurs exception 3 times, exit" << std::endl;
			exit(-1);
		}
	}

  while (!bg_thread_group_.write_queue_p()->empty()) {
    sleep(1);
  }

	LOG(INFO) << util::GetFormatTime() << MONGOSYNC_PROMPT << "clone "	<< src_ns << " to " << dst_ns << " success, total " << cnt << " objects\n" << std::endl;
}

void MongoSync::CloneCollIndex(std::string sns, std::string dns) {
	LOG(INFO) << util::GetFormatTime() << MONGOSYNC_PROMPT << "cloning " << sns << " indexes" << std::endl;
	mongo::BSONObj indexes;
	if (GetCollIndexesByVersion(src_conn_, src_version_, sns, indexes) == -1) {
		return;
	}
	int32_t indexes_num = indexes.nFields(), idx = 0;
	mongo::BSONElement element;
	while (idx < indexes_num) {
	  mongo::BSONObjBuilder builder;
		mongo::BSONObjIterator i(indexes.getObjectField(util::Int2Str(idx++)));
		std::string field_name;
		while (i.more()) {
			element = i.next();
			field_name = element.fieldName();
			if (field_name == "background" || field_name == "ns") {
				continue;
			}
			builder << element;			
		}
		builder << "background" << "true";
		builder << "ns" << dns;
		SetCollIndexesByVersion(dst_conn_, dst_version_, dns, builder.obj());
	}
	LOG(INFO) << util::GetFormatTime() << MONGOSYNC_PROMPT << "clone " << sns << " indexes success, total " << indexes_num << "objects\n" << std::endl;
}

void *MongoSync::ProcessSingleOplog(void *pargs) {
  util::OplogArgs *args = reinterpret_cast<util::OplogArgs *>(pargs);
  std::string db = args->db;
  std::string coll = args->coll;
  std::string dst_db = args->dst_db;
  std::string dst_coll = args->dst_coll;
  mongo::BSONObj oplog = args->oplog.copy();
  int op = args->op;
	mongo::DBClientConnection* dst_conn = args->dst_conn;

	std::string oplog_ns = oplog.getStringField("ns");	

	if (dst_db.empty()) {
		dst_db = db.empty() ? NamespaceString(oplog_ns).db() : db;
	}
	if (dst_coll.empty()) {
		dst_coll = coll.empty() ? NamespaceString(oplog_ns).coll() : coll;
	}
	std::string dns = dst_db + "." + dst_coll;

	if (op == kClone) {
		dst_conn->insert(dns, oplog, 0, &mongo::WriteConcern::unacknowledged);
    return NULL;
	}

	mongo::BSONObj obj = oplog.getObjectField("o");
	std::string type = oplog.getStringField("op");
	switch(type.at(0)) {
		case 'i':
			ApplyInsertOplog(dst_conn, dst_db, dst_coll, oplog);
			break;
		case 'u':
			dst_conn->update(dns, oplog.getObjectField("o2"), oplog.getObjectField("o"));
			break;
		case 'd':
			dst_conn->remove(dns, oplog.getObjectField("o"));
			break;
		case 'n':
			break;
		case 'c':
			if (mongo::str::endsWith(dst_coll, "$cmd")) { //destination collection name is not specified
				dst_coll = "";
			}
			ApplyCmdOplog(dst_conn, db, dst_db, dst_coll, oplog, coll == dst_coll);
	}
}

void MongoSync::ApplyInsertOplog(mongo::DBClientConnection* dst_conn,
                                 const std::string& dst_db, const std::string& dst_coll,
                                 const mongo::BSONObj& oplog) {
	assert(!dst_db.empty() && !dst_coll.empty());

	mongo::BSONObj obj = oplog.getObjectField("o");
	std::string ns = oplog.getStringField("ns");
	std::string dns = dst_db + "." + dst_coll;
	if (ns.size() < sizeof(".system.indexes")
			|| ns.substr(ns.size()-sizeof(".system.indexes")+1) != ".system.indexes") { //not index-ceating oplog
		dst_conn->insert(dns, obj, 0, &mongo::WriteConcern::unacknowledged);
		return;
	}

	if (dst_coll == "system.indexes") {
		dns = dst_db + "." + NamespaceString(obj.getStringField("ns")).coll(); // the collection name is system.indexes, needs modified	
	}
	mongo::BSONObjBuilder build;
	mongo::BSONObjIterator iter(obj);
	mongo::BSONElement ele;
	std::string field;
	build << "background" << "true" << "ns" << dns;
	while (iter.more()) {
		ele = iter.next();
		field = ele.fieldName();
		if (field == "background" || field == "ns") {
			continue;
		}
		build.append(ele);
	}
  std::string dst_version = GetMongoVersion(dst_conn);
	SetCollIndexesByVersion(dst_conn, dst_version, dns, build.obj());	
}

void MongoSync::ApplyCmdOplog(mongo::DBClientConnection* dst_conn,
                              std::string src_db,
                              std::string dst_db, const std::string& dst_coll,
                              const mongo::BSONObj& oplog, bool same_ns) {
  //TODO: other cmd oplog could be reconsidered
	mongo::BSONObj obj = oplog.getObjectField("o"), tmp;
	std::string first_field = obj.firstElementFieldName();
	if (first_field != "deleteIndexes" 
			&& first_field != "dropIndexes"
			&& first_field != "drop"
			&& first_field != "collMod"
			&& first_field != "create"
			&& first_field != "dropDatabase"
			&& first_field != "renameCollection") { // in admin.$cmd, under db, coll specified, this option has been filtered
		return;
	}
	
	if (first_field == "renameCollection") { //get here coll must be empty, so the dst_coll is also empty
		mongo::BSONObjIterator iter(obj);
		mongo::BSONElement ele;
		mongo::BSONObjBuilder build;
		std::string field;
		while (iter.more()) {
			ele = iter.next();
			field = ele.fieldName();
			if (field == "renameCollection" || field == "to") {
				if (dst_db == "admin") { //get here, db is empty, all db(exclude admin and local) is related
					build.append(ele);
					continue;
				}
				std::string ns = ele.valuestr();	
				if (!src_db.empty() && NamespaceString(ns).db() != src_db) { // should put opt_.db here, optimize it later
					return;	
				}
				build << field << dst_db + "." + NamespaceString(ns).coll();
				continue;
			}
			build.append(ele);
		}
		obj = build.obj();
		dst_db = "admin"; //renameCollection command must run against to admin database
	} else if (!same_ns || dst_coll.empty()) {
		mongo::BSONObjIterator iter(obj);
		mongo::BSONElement ele;
		mongo::BSONObjBuilder build;
		std::string field;
		while (iter.more()) {
			ele = iter.next();
			field = ele.fieldName();
			if (field == "deleteIndexes" 
					|| field == "dropIndexes"
					|| field == "drop"
					|| field == "collMod"
					|| field == "create") {
				if (dst_coll.empty()) {
					build << field << std::string(ele.valuestr());
				} else {
					build << field << dst_coll;
				}
				continue;
			}		
			build.append(ele);
		}
		obj = build.obj();
	}
	std::string str = obj.toString();
	if (!dst_conn->runCommand(dst_db, obj, tmp, mongo::QueryOption_SlaveOk)) {
		LOG(WARN) << "administration oplog sync failed, with oplog: " << obj.toString() << ", errms: " << tmp << std::endl;
	}
}

OplogTime MongoSync::GetSideOplogTime(mongo::DBClientConnection* conn, std::string oplog_ns, std::string db, std::string coll, bool first_or_last) {
	int32_t order = first_or_last ? 1 : -1;	
	mongo::BSONObj obj;
	if (db.empty() || coll.empty()) {
		obj = conn->findOne(oplog_ns, mongo::Query().sort("$natural", order), NULL, mongo::QueryOption_SlaveOk);	
	} else if (!db.empty() && coll.empty()) {
		obj = conn->findOne(oplog_ns, mongo::Query(BSON("ns" << BSON("$regex" << db))).sort("$natural", order), NULL, mongo::QueryOption_SlaveOk);
	} else if (!db.empty() && !coll.empty()) {
		NamespaceString ns(db, coll);
		obj = conn->findOne(oplog_ns, mongo::Query(BSON("$or" << BSON_ARRAY(BSON("ns" << ns.ns()) 
																																					<< BSON("ns" << ns.db() + ".system.indexes")
																																					<< BSON("ns" << ns.db() + ".system.cmd")))).sort("$natural", order), NULL, mongo::QueryOption_SlaveOk);
	} else {
		LOG(FATAL) << "get side oplog time erorr" << std::endl;
		exit(-1);
	}
	return *reinterpret_cast<const OplogTime*>(obj["ts"].value());
}

std::string MongoSync::GetMongoVersion(mongo::DBClientConnection* conn) {
	std::string version;
	mongo::BSONObj build_info;
	bool ok = conn->simpleCommand("admin", &build_info, "buildInfo");
	if (ok && build_info["version"].trueValue()) {
		version = build_info.getStringField("version");
	}
	return version;
}

int MongoSync::GetAllCollByVersion(mongo::DBClientConnection* conn, std::string version, std::string db, std::vector<std::string>& colls) {
	std::string version_header = version.substr(0, 4);
	mongo::BSONObj tmp;
	if (version_header == "3.0." || version_header == "3.2." || version_header == "3.4.") {
		mongo::BSONObj array;
		if (!conn->runCommand(db, BSON("listCollections" << 1), tmp, mongo::QueryOption_SlaveOk)) {
			LOG(FATAL) << "get " << db << "'s collections failed" << std::endl;
			return -1;
		}
		array = tmp.getObjectField("cursor").getObjectField("firstBatch");
		int32_t idx = 0;
		while (array.hasField(util::Int2Str(idx))) {
			colls.push_back(array.getObjectField(util::Int2Str(idx++)).getStringField("name"));
		} 
	} else if (version_header == "2.4." || version_header == "2.6.") {
		std::auto_ptr<mongo::DBClientCursor> cursor = conn->query(db + ".system.namespaces", mongo::Query().snapshot(), 0, 0, NULL, mongo::QueryOption_SlaveOk | mongo::QueryOption_NoCursorTimeout);
		if (cursor.get() == NULL) {
			LOG(FATAL) << "get " << db << "'s collections failed" << std::endl;
			return -1;
		}
		std::string coll;
		while (cursor->more()) {
			tmp = cursor->next();
			coll = tmp.getStringField("name");
      if (mongoutils::str::endsWith(coll.c_str(), ".system.namespaces") ||
          mongoutils::str::endsWith(coll.c_str(), ".system.users") ||
          mongoutils::str::endsWith(coll.c_str(), ".system.js") ||
          mongoutils::str::endsWith(coll.c_str(), ".system.profile") ||
          mongoutils::str::endsWith(coll.c_str(), ".system.indexes") ||
          mongoutils::str::contains(coll, ".$")) {
				continue;
			}
			colls.push_back(coll.substr(coll.find(".")+1));
		}	
	} else {
		LOG(FATAL) << "version: " << version << " is not supported" << std::endl;
		exit(-1);
	}
	return 0;
}

int MongoSync::GetCollIndexesByVersion(mongo::DBClientConnection* conn, std::string version, std::string coll_full_name, mongo::BSONObj& indexes) {
	NamespaceString ns(coll_full_name);
	mongo::BSONObj tmp;
	std::string version_header = version.substr(0, 4);
	if (version_header == "3.0." || version_header == "3.2." || version_header == "3.4.") {
		if (!conn->runCommand(ns.db(), BSON("listIndexes" << ns.coll()), tmp, mongo::QueryOption_SlaveOk)) {
			LOG(FATAL) << coll_full_name << " get indexes failed" << std::endl;
			return -1;
		}
		indexes = tmp.getObjectField("cursor").getObjectField("firstBatch").getOwned();
	} else if (version_header == "2.4." || version_header == "2.6.") {
		std::auto_ptr<mongo::DBClientCursor> cursor;
		mongo::BSONArrayBuilder array_builder;
		cursor = conn->query(ns.db() + ".system.indexes", mongo::Query(BSON("ns" << coll_full_name)).snapshot(), 0, 0, 0, mongo::QueryOption_SlaveOk | mongo::QueryOption_NoCursorTimeout);
		if (cursor.get() == NULL) {
			LOG(FATAL) << coll_full_name << " get indexes failed" << std::endl;
			return -1;
		}	
		while (cursor->more()) {
			tmp = cursor->next();
			array_builder << tmp;	
		}
		indexes = array_builder.arr();
	} else {
		LOG(FATAL) << "version: " << version << " is not surpported" << std::endl;
		exit(-1);
	}
	return 0;
}

void MongoSync::SetCollIndexesByVersion(mongo::DBClientConnection* conn, std::string version, std::string coll_full_name, mongo::BSONObj index) {
	std::string version_header = version.substr(0, 4);
	NamespaceString ns(coll_full_name);
	if (version_header == "3.0." || version_header == "3.2." || version_header == "3.4.") {
		mongo::BSONObj tmp;
		conn->runCommand(ns.db(), BSON("createIndexes" << ns.coll() << "indexes" << BSON_ARRAY(index)), tmp, mongo::QueryOption_SlaveOk);
	} else if (version_header == "2.4." || version_header == "2.6.") {
		conn->insert(ns.db() + ".system.indexes", index, 0, &mongo::WriteConcern::unacknowledged);	
	} else {
		LOG(FATAL) << "version: " << version << " is not surpported" << std::endl;
		exit(-1);
	}
}

const std::string MongoSync::oplog_ns_ = "local.oplog.rs";
