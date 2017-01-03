#ifndef LOG_H
#define LOG_H

#include <pthread.h>
#include <string>
#include <sstream>
#include <map>


#include <stdint.h>
namespace mlog {

enum LogLevel {
	kInfo,
	kWarn,
	kFatal,
	kMaxLevel
};

extern LogLevel work_level;
extern pthread_mutex_t mlock;

class Log {
public:
	Log(const LogLevel level);
  ~Log();

	std::stringstream& strm() {
		return strm_;
	}

private:
	std::stringstream strm_;
	LogLevel self_level_;
};

void Init(const LogLevel level = kInfo, const std::string &log_dir = "./log", const std::string &file_prefix = "", const bool screen_out = true);
bool SetLogLevel(const std::string &level_str);
bool SetLogDir(const std::string &level_dir);

std::string GetLevelStr();

}

#define INFO    mlog::kInfo
#define WARN    mlog::kWarn
#define FATAL   mlog::kFatal

#define LOG(level) \
	if (level >= mlog::work_level) \
		mlog::Log(level).strm()

#endif
