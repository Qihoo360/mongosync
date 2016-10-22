#include <sys/time.h>

#include <stdio.h>
#include "util.h"
namespace util {
uint64_t microtime() {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec*1000000 + tv.tv_usec;
}
}
