#pragma once
#include <cstdint>
#include "tinydb/env.h"
#include "tinydb/slice.h"
#include "tinydb/status.h"

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
  Status EmitPhysicalRecord(const char* ptr, size_t len);

  WritableFile* dest_;
};

} // namespace log
} // namespace tinydb
