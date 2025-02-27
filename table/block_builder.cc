#include "table/block_builder.h"
#include <algorithm>
#include <cassert>
#include "tinydb/comparator.h"
#include "tinydb/options.h"
#include "util/coding.h"

namespace tinydb {

void BlockBuilder::Reset() {
  buffer_.clear();
  finished_ = false;
  last_key_.clear();
}
size_t BlockBuilder::CurrentSizeEstimate() const { return buffer_.size(); }

Slice BlockBuilder::Finish() {
  finished_ = true;
  return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  // Add "<key_size> <value_size>" to buffer_
  PutVarint32(&buffer_, key.size());
  PutVarint32(&buffer_, value.size());

  // Add "<key> <value>" string to buffer_
  buffer_.append(key.data(), key.size());
  buffer_.append(value.data(), value.size());

  last_key_ = key.ToString();
}

} // namespace tinydb
