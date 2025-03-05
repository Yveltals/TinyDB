#include "log/log_reader.h"

#include <cstdio>
#include <iostream>

#include "log/logger.h"
#include "util/file.h"

namespace tinydb {
namespace log {

const int kHeaderSize = 2;

bool Reader::ReadRecord(Slice* record) {
  record->clear();

  auto handle_err = [&](Status& st) { Logger::Log(st.ToString()); };
  // Read data length
  Slice s;
  Status st = file_->Read(kHeaderSize, &s, buffer_.get());
  if (!st.ok()) {
    handle_err(st);
    return false;
  }
  const char* header = s.data();
  const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
  const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
  const uint32_t length = a | (b << 8);

  // Read data
  st = file_->Read(length, &s, buffer_.get());
  if (!st.ok() || (s.size() < length)) {
    handle_err(st);
    return false;
  }
  *record = Slice(s.data(), length);
  return true;
};

} // namespace log
} // namespace tinydb
