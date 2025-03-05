#pragma once
#include <cstdint>

#include "common/slice.h"
#include "common/status.h"
#include "log/logger.h"
#include "util/file.h"

namespace tinydb {
namespace log {

extern const int kHeaderSize;

class Reader {
 public:
  Reader(std::unique_ptr<SequentialFile> file)
      : file_(std::move(file)), buffer_(new char[4096]) {}
  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;
  ~Reader() = default;

  bool ReadRecord(Slice* record);

 private:
  std::unique_ptr<SequentialFile> file_;
  std::unique_ptr<char[]> buffer_;
};

} // namespace log
} // namespace tinydb
