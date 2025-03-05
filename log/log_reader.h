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
      : file_(std::move(file)),
        backing_store_(new char[4096]),
        buffer_(),
        eof_(false) {}
  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;
  ~Reader() { delete[] backing_store_; }

  bool ReadRecord(Slice* record);

 private:
  std::unique_ptr<SequentialFile> file_;
  char* const backing_store_;
  Slice buffer_;
  bool eof_; // XXX
};

} // namespace log
} // namespace tinydb
