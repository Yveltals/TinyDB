#pragma once
#include <cstdint>
#include "common/slice.h"
#include "common/status.h"
#include "util/file.h"

namespace tinydb {
namespace log {

int kHeaderSize = 2;

class Reader {
 public:
  Reader(SequentialFile* file)
      : file_(file), backing_store_(new char[4096]), buffer_(), eof_(false) {}
  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;
  ~Reader() { delete[] backing_store_; }

  bool ReadRecord(Slice* record);

 private:
  SequentialFile* const file_;
  char* const backing_store_;
  Slice buffer_;
  bool eof_; // XXX
};

} // namespace log
} // namespace tinydb
