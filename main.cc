#include "mongosync.h"
#include "log.h"

#include <iostream>

void *mongo_to_mongos(void *args) {
  MongoSync *mongosync = reinterpret_cast<MongoSync *>(args);
  mongosync->Process();

  delete mongosync;
  pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
	/*
	 *  first set default log level to INFO
	 */
	mlog::Init(mlog::kInfo, "./log", "mongosync");
	LOG(INFO) << "monogosync started, first set log level to INFO" << std::endl;

  mongo::client::GlobalInstance instance;
  if (!instance.initialized()) {
		LOG(FATAL) << "failed to initialize the client driver: " << instance.status() << std::endl;
    exit(-1);
  }
	
  Options opt;
  if (argc == 3 && (strncmp(argv[1], "-c", 2) == 0)) {
    opt.LoadConf(argv[2]);
  } else {
    opt.ParseCommand(argc, argv);	
  }

	if (!opt.log_level.empty()) {
		LOG(INFO) << "with log level option, set log level to " << opt.log_level << std::endl; 
		if (!mlog::SetLogLevel(opt.log_level)) {
			LOG(WARN) << "log level option value invalid, set log level to default to INFO" << std::endl;
		}
	}

  // mongos -> mongos
  if (opt.is_mongos) {
    if (opt.shard_user.empty() || opt.shard_passwd.empty()) {
      LOG(FATAL)
        << "Shard username or password should not be empty when src is mongos\n"
        << std::endl;
      return -1;
    }
    // Get mongos shards host info
    MongoSync *mongosync = MongoSync::NewMongoSync(&opt);
    if (!mongosync) {
      LOG(FATAL) << "Create mongosync instance failed" << std::endl;
      return -1;
    }
    std::vector<std::string> shards = mongosync->GetShards();
    mongosync->StopBalancer();
    delete mongosync;

    // Create connection between shard and dst mongos
    int ret;
    pthread_t tid;
    std::vector<pthread_t> tids;
    std::vector<std::string> shard_ips;
    Options shard_opt(opt);
    shard_opt.src_user = opt.shard_user;
    shard_opt.src_passwd = opt.shard_passwd;
    for (int i = 0; i < shards.size(); i++) {
      size_t slash_pos = shards[i].find('/');
      std::string shard_addr = shards[i].substr(slash_pos + 1);
      shard_ips = util::Split(shard_addr, ',');

      // find a SECONDARY src mongodb
      MongoSync *mongosync = NULL;
      int j = 0;
      for (j = 0; j < shard_ips.size(); j++) {
        shard_opt.src_ip_port = shard_ips[j];
        shard_opt.is_mongos = false;
        mongosync = MongoSync::NewMongoSync(&shard_opt); // delete in thread
        if (mongosync && !mongosync->IsMasterMongo())
          break;
        else if (mongosync && j == shard_ips.size() - 1)
          break;
        else
          delete mongosync;
      }
      if (!mongosync) {
        LOG(FATAL) << "Create shard: " << shard_ips[j] <<
          "mongosync instance failed" << std::endl;
        return -1;
      }

      ret = pthread_create(&tid, NULL, mongo_to_mongos, (void *)mongosync);
      if (ret != 0)
        return -1;
      tids.push_back(tid);
    }

    for (int i = 0; i < tids.size(); i++) {
      pthread_join(tids[i], NULL);
    }
  } else {
    // Others sync methods
    MongoSync *mongosync = MongoSync::NewMongoSync(&opt);
    if (!mongosync) {
      return -1;
    }
    mongosync->Process();
    delete mongosync;
  }

  return 0;
}
