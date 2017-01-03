#include <iostream>
#include <utility>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cstdlib>
#include <strings.h>
#include <unistd.h>


#include "log.h"
#include "util.h"

namespace mlog {


struct LogMeta {
	LogMeta();
	~LogMeta();

	std::map<LogLevel, std::string> log_level_prompts;
	std::map<LogLevel, std::string> log_level_strs;
	std::map<LogLevel, int32_t> log_level_files;
	
	std::string dir;
	std::string file_prefix;
	bool screen_out;
};

LogMeta::LogMeta() {

	log_level_prompts.insert(std::make_pair(kInfo,  "[INFO ]"));
	log_level_prompts.insert(std::make_pair(kWarn,  "[WARN ]"));
	log_level_prompts.insert(std::make_pair(kFatal, "[FATAL]"));

	log_level_strs.insert(std::make_pair(kInfo,  "info" ));
	log_level_strs.insert(std::make_pair(kWarn,  "warn" ));
	log_level_strs.insert(std::make_pair(kFatal, "fatal"));

}

LogMeta::~LogMeta() {

	for (std::map<LogLevel, int32_t>::const_iterator iter = log_level_files.begin();
			iter != log_level_files.end();
			++iter) {
		if (iter->second > 0) {
			close(iter->second);
		}
	}

}

LogLevel work_level;
LogMeta log_meta;

void Init(const LogLevel level, const std::string &log_dir, const std::string &file_prefix, const bool screen_out) {
	log_meta.file_prefix = file_prefix;
	work_level = level;
	log_meta.screen_out = screen_out;

	if (access(log_dir.c_str(), F_OK)) {
		util::CreatePath(log_dir);	
	}
	std::string log_path = log_dir;
	if (!log_path.empty() && *log_path.rbegin() == '/') {
		log_path.erase(log_path.size()-1);
	}

	std::string file_name;
	int32_t fd;
	for (int32_t idx = kInfo; idx < kMaxLevel; ++idx) {
		file_name = file_prefix.empty() ? "" : (file_prefix + "_");
		file_name = log_path + "/" + file_name; 
		file_name += log_meta.log_level_strs[static_cast<LogLevel>(idx)] + ".log";
		fd = open(file_name.c_str(), O_RDWR | O_APPEND | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
		if (fd == -1) {
			std::cerr << __FILE__ << ":"  << __LINE__ << ", Open " << file_name << " failed" << std::endl;
			exit(-1);
		}

		log_meta.log_level_files.insert(std::make_pair(static_cast<LogLevel>(idx), fd));
	}
}

static LogLevel InterpretLogLevel(const std::string level_str) {
	for (int32_t idx = kInfo; idx < kMaxLevel; ++idx) {
		if (strcasecmp(log_meta.log_level_strs[static_cast<LogLevel>(idx)].c_str(), level_str.c_str()) == 0) {
			return static_cast<LogLevel>(idx);
		}
  }
  return kMaxLevel;
}

std::string GetLevelStr() {
	return log_meta.log_level_strs[work_level];
}

bool SetLogLevel(const std::string &level_str) {
	bool ret = true;
	LogLevel level = InterpretLogLevel(level_str);
	if (level == kMaxLevel) {
		ret = false;
		level = kInfo;
	}

	/*
	 * to be pretected
	 */

	work_level = level;
	return ret;
}

Log::Log(const LogLevel level) {
	self_level_ = level;
	strm_ << log_meta.log_level_prompts[self_level_] << "\t\t";
}

Log::~Log() {
	std::string str = strm_.str();

	if (log_meta.screen_out) {
		std::cerr << str;
	}

	if (write(log_meta.log_level_files[self_level_], str.c_str(), str.size()) < 0) {
		 std::cerr << __FILE__ << ":"  << __LINE__ << ", write " << log_meta.log_level_strs[self_level_] << " failed" << std::endl;
	}
}

}

