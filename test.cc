#include <fmt/core.h>

#include <chrono>
#include <fstream>
#include <string_view>

#include "log/logger.h"

using namespace std;

class Logger {
 public:
  template <typename... Args>
  static void Log(std::string_view fmt, Args&&... args) {
    auto& log_file = GetLogFile();
    auto formatted = fmt::format(fmt, std::forward<Args>(args)...);
    log_file << formatted << std::endl;
  }

 private:
  static std::ofstream& GetLogFile() {
    static std::ofstream log_file;
    if (!log_file.is_open()) {
      std::string filename = "log" + CurrentDate() + ".txt";
      log_file.open(filename, std::ios::out);
      if (!log_file.is_open()) {
        throw std::runtime_error("Failed to open log file.");
      }
    }
    return log_file;
  }

  static std::string CurrentDate() {
    auto now = std::chrono::system_clock::now();
    auto now_tm = std::chrono::system_clock::to_time_t(now);
    return "aaa";
  }
};

int main() {
  int i = 0;
  string msg{"hhh"};
  tinydb::Logger::Log("test log i={}, msg={}", i, msg);
  tinydb::Logger::Log("hahaha {}", i + 3);
}