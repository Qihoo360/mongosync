#include "mongosync.h"
#include <iostream>

int main(int argc, char *argv[]) {
  mongo::client::Options o;
  mongo::client::GlobalInstance instance(o);
//  mongo::client::GlobalInstance instance;
  if (!instance.initialized()) {
    std::cerr << "failed to initialize the client driver: " << instance.status() << std::endl;
    exit(-1);
  }
  Options opt;
  ParseOptions(argc, argv, &opt);	
  MongoSync *mongosync = MongoSync::NewMongoSync(opt);
  mongosync->Process();
  delete mongosync;
  return 0;
}
