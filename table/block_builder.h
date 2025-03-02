#pragma once
#include <cstdint>
#include <vector>
#include "common/options.h"
#include "common/slice.h"

namespace tinydb {

class BlockBuilder {
 public:
  explicit BlockBuilder(const Options* options)
      : options_(options), finished_(false) {}
  BlockBuilder(const BlockBuilder&) = delete;
  BlockBuilder& operator=(const BlockBuilder&) = delete;

  void Reset();
  // REQUIRES: key is larget than any previously added key
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice
  Slice Finish();

  size_t CurrentSizeEstimate() const;

  bool empty() const { return buffer_.empty(); }

 private:
  const Options* options_;
  std::string buffer_;
  bool finished_;
  std::string last_key_;
};

} // namespace tinydb
