#include "log/log_writer.h"

#include <cstdint>

#include "log/log_reader.h"
#include "util/coding.h"
#include "util/file.h"

namespace tinydb {
namespace log {

Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t len = slice.size();

  // Format the header
  char buf[kHeaderSize];
  buf[0] = static_cast<char>(len & 0xff);
  buf[1] = static_cast<char>(len >> 8);

  // Write the header and the payload
  Status st = dest_->Append(Slice(buf, kHeaderSize));
  if (st.ok()) {
    st = dest_->Append(Slice(ptr, len));
    if (st.ok()) {
      st = dest_->Flush();
    }
  }
  return st;
}

} // namespace log
} // namespace tinydb
