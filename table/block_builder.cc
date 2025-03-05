#include "table/block_builder.h"

#include <algorithm>
#include <cassert>

#include "common/comparator.h"
#include "common/options.h"
#include "util/coding.h"

namespace tinydb {

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  Slice last_key(last_key_);
  assert(buffer_.empty() || options_->comparator->Compare(key, last_key) > 0);

  PutVarint32(&buffer_, key.size());
  PutVarint32(&buffer_, value.size());
  buffer_.append(key.data(), key.size());
  buffer_.append(value.data(), value.size());
  last_key_ = key.ToString();
}

Slice BlockBuilder::Finish() {
  finished_ = true;
  return Slice(buffer_);
}

void BlockBuilder::Reset() {
  buffer_.clear();
  finished_ = false;
  last_key_.clear();
}

} // namespace tinydb
