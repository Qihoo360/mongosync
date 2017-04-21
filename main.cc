#include "mongosync.h"
#include "log.h"

#include <iostream>

int shards_num = 1;
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
    if (mongosync->IsBalancerRunning()) {
      LOG(FATAL) << "Balancer is running" << std::endl;
      return -1;
    }
    if (mongosync->IsBigChunkExist()) {
      LOG(FATAL) << "Big chunk exist" << std::endl;
      return -1;
    }
    delete mongosync;

    // Create connection between shard and dst mongos
    int ret;
    pthread_t tid;
    std::vector<pthread_t> tids;
    std::vector<std::string> shard_ips;
    Options shard_opt(opt);
    shard_opt.src_user = opt.shard_user;
    shard_opt.src_passwd = opt.shard_passwd;
    shards_num = shards.size();
    for (int i = 0; i < shards.size(); i++) {
      size_t slash_pos = shards[i].find('/');
      size_t comma_pos = shards[i].find(',');
      std::string shard_addr =
        shards[i].substr(slash_pos + 1, comma_pos - slash_pos - 1);

      // find a SECONDARY src mongodb
      MongoSync *mongosync = NULL;
      shard_opt.src_ip_port = shard_addr;
      mongosync = MongoSync::NewMongoSync(&shard_opt); // delete in thread
      std::string readable_host = shard_addr;
      if (!mongosync || !mongosync->GetReadableHost(&readable_host)) {
        LOG(FATAL) << "Create shard mongosync instance failed" << std::endl;
        return -1;
      }
      if (readable_host != shard_addr) {
        delete mongosync;
        shard_opt.src_ip_port = readable_host;
        mongosync = MongoSync::NewMongoSync(&shard_opt);
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
