#pragma once
#include <cstddef>
#include <cstdint>
#include "table/format.h"
#include "common/iterator.h"
#include "common/comparator.h"

namespace tinydb {

class Block {
 public:
  explicit Block(const BlockContents& contents)
      : data_(contents.data.data()),
        size_(contents.data.size()),
        owned_(contents.heap_allocated) {
    if (size_ < sizeof(uint32_t)) {
      size_ = 0;  // Error marker
    }
  }
  Block(const Block&) = delete;
  Block& operator=(const Block&) = delete;
  ~Block() {
    if (owned_) {
      delete[] data_;
    }
  }

  size_t size() const { return size_; }
  Iterator* NewIterator(const Comparator* comparator);

 private:
  class Iter;

  const char* data_;
  size_t size_;
  bool owned_; // Block owns data_[]
};

} // namespace tinydb
