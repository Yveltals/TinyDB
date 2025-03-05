#pragma once
#include <cstddef>
#include <cstdint>

#include "common/comparator.h"
#include "common/iterator.h"
#include "iterator/iterator_block.h"
#include "table/format.h"

namespace tinydb {

class Block {
 public:
  // Data in BlockContents will move to Block
  explicit Block(BlockContents& contents)
      : data_(std::move(contents.data)), size_(contents.size) {
    if (size_ < sizeof(uint32_t)) {
      size_ = 0; // Error marker
    }
  }
  Block(const Block&) = delete;
  Block& operator=(const Block&) = delete;
  ~Block() = default;

  size_t size() const { return size_; }

  std::unique_ptr<Iterator> NewIterator(const Comparator* comparator) {
    assert(size_);
    return std::make_unique<IteratorBlock>(comparator, data_.get(), size_);
  }

 private:
  std::unique_ptr<char[]> data_;
  size_t size_;
};

} // namespace tinydb
