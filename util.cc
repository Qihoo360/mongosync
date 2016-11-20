#include <sys/time.h>

#include <stdio.h>
#include "util.h"
namespace util {

uint64_t Microtime() {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec*1000000 + tv.tv_usec;
}

std::string Int2Str(int64_t num) {
  char buf[32];
  snprintf(buf, sizeof(buf), "%ld", num);
  return buf;
}

}
