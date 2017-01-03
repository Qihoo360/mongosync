#include "mongosync.h"
#include "log.h"

#include <iostream>

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

  MongoSync *mongosync = MongoSync::NewMongoSync(opt);
  if (!mongosync) {
     return -1;
  }
  mongosync->Process();
  delete mongosync;
  return 0;
}
