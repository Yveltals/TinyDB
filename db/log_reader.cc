#include "db/log_reader.h"
#include <cstdio>
#include <iostream>
#include "util/file.h"

namespace tinydb {
namespace log {

bool Reader::ReadRecord(Slice* record) {
  record->clear();

  auto handle_err = [&](Status& st) {
    buffer_.clear();
    std::cout << st.ToString() << std::endl;
  };
  // read pre_length
  Status st = file_->Read(kHeaderSize, &buffer_, backing_store_);
  if (!st.ok()) {
    handle_err(st);
    return false;
  }
  const char* header = buffer_.data();
  const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
  const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
  const uint32_t length = a | (b << 8);

  // read data
  st = file_->Read(length, &buffer_, backing_store_);
  if (!st.ok() || (buffer_.size() < length)) {
    handle_err(st);
    return false;
  }
  *record = Slice(buffer_.data(), length);
  return true;
};

} // namespace log
} // namespace tinydb
