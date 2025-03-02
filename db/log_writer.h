#pragma once
#include <cstdint>
#include "common/slice.h"
#include "common/status.h"
#include "util/file.h"

namespace tinydb {
namespace log {

extern int kHeaderSize;

class Writer {
 public:
  explicit Writer(WritableFile* dest) : dest_(dest) {}
  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;
  ~Writer() = default;

  Status AddRecord(const Slice& slice);

 private:
  WritableFile* dest_;
};

} // namespace log
} // namespace tinydb
