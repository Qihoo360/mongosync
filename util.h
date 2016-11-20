#ifndef UTIL_H
#define UTIL_H

#include <stdint.h>
#include <string>

namespace util {

uint64_t Microtime();
std::string Int2Str(int64_t num);

}
#endif
