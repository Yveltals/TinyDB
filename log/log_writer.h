#pragma once
#include <cstdint>
#include "common/slice.h"
#include "common/status.h"
#include "util/file.h"

namespace tinydb {
namespace log {

class Writer {
 public:
  explicit Writer(std::unique_ptr<WritableFile> dest)
      : dest_(std::move(dest)) {}
  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;
  ~Writer() = default;

  Status AddRecord(const Slice& slice);
  Status Sync() { return dest_->Sync(); }

 private:
  std::unique_ptr<WritableFile> dest_;
};

} // namespace log
} // namespace tinydb
