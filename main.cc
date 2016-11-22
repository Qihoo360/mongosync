#include "mongosync.h"
#include <iostream>

int main(int argc, char *argv[]) {
//  mongo::client::Options o;
//  mongo::client::GlobalInstance instance(o);
  mongo::client::GlobalInstance instance;
  if (!instance.initialized()) {
    std::cerr << "failed to initialize the client driver: " << instance.status() << std::endl;
    exit(-1);
  }

  Options opt;
  if (argc == 3 && (strncmp(argv[1], "-c", 2) == 0)) {
    opt.LoadConf(argv[2]);
  } else {
    opt.ParseCommand(argc, argv);	
  }

  MongoSync *mongosync = MongoSync::NewMongoSync(opt);
  if (!mongosync) {
     return -1;
  }
  mongosync->Process();
  delete mongosync;
  return 0;
}
