#include "mongosync.h"
#include "log.h"

#include <iostream>

void *mongo_to_mongos(void *args) {
  Options *opt = reinterpret_cast<Options *>(args);
  int ret = 0;

  MongoSync *mongosync = MongoSync::NewMongoSync(opt);
  if (!mongosync) {
    ret = -1;
    pthread_exit(&ret);
  }
  mongosync->Process();

  delete mongosync;
  delete opt;
  pthread_exit(&ret);
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
    for (int i = 0; i < shards.size(); i++) {
      Options *sopt = new Options(opt); // delete in thread
      size_t slash_pos = shards[i].find('/');
      sopt->src_ip_port = shards[i].substr(slash_pos + 1);
      sopt->src_user = sopt->shard_user;
      sopt->src_passwd = sopt->shard_passwd;
      ret = pthread_create(&tid, NULL, mongo_to_mongos, (void *)sopt);
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
