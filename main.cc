#include "mongosync.h"

int main(int argc, char *argv[]) {
	Options opt;
	ParseOptions(argc, argv, &opt);	
	MongoSync *mongosync = MongoSync::NewMongoSync(opt);
	mongosync->Process();
	delete mongosync;
	return 0;
}
