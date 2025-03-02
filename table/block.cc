#include "table/block.h"
#include <algorithm>
#include <cstdint>
#include <vector>
#include "common/comparator.h"
#include "util/coding.h"
#include "log/logging.h"

namespace tinydb {

static inline const char* DecodeEntry(const char* p, const char* limit,
                                      uint32_t* key_length,
                                      uint32_t* value_length) {
  if (limit - p < 2) return nullptr;
  if ((p = GetVarint32Ptr(p, limit, key_length)) == nullptr) return nullptr;
  if ((p = GetVarint32Ptr(p, limit, value_length)) == nullptr) return nullptr;
  return p;
}

class Block::Iter : public Iterator {
 private:
  const Comparator* const comparator_;
  const char* const data_;
  const size_t size_;

  uint32_t current_; // current entry offset in data_
  std::string key_;  // current parsed key
  Slice value_;      // current parsed value
  Status status_;
  bool valid_;

  inline int Compare(const Slice& a, const Slice& b) const {
    return comparator_->Compare(a, b);
  }

  // Return the offset in data_ just past the end of the current entry.
  inline uint32_t NextEntryOffset() const {
    return (value_.data() + value_.size()) - data_;
  }

 public:
  Iter(const Comparator* comparator, const char* data, const size_t size)
      : comparator_(comparator), data_(data), size_(size), valid_(false) {}

  bool Valid() const override { return valid_; }
  Status status() const override { return status_; }
  Slice key() const override {
    return key_;
  }
  Slice value() const override {
    return value_;
  }
  void Prev() override {
    assert(false);
  }
  void Next() override {
    ParseNextKey();
  }

  void Seek(const Slice& target) override {
    // TODO: Binary search optimize
    // Linear search for first key >= target
    SeekToFirst();
    while (true) {
      ParseNextKey();
      if (!valid_) return;
      if (Compare(key_, target) >= 0) {
        return;
      }
    }
  }

  void SeekToFirst() override {
    key_.clear();
    value_ = Slice(data_, 0);
    ParseNextKey();
  }

  void SeekToLast() override {
    key_.clear();
    value_ = Slice(data_, 0);
    while (NextEntryOffset() < size_) {
      ParseNextKey();
      if (!valid_) break;
    }
  }

 private:
  void ParseNextKey() {
    valid_ = false;
    current_ = NextEntryOffset();
    const char* p = data_ + current_;
    const char* limit = data_ + size_;
    if (p >= limit) {
      current_ = size_;
      return;
    }
    // Decode next entry
    uint32_t key_len, value_len;
    if (p = DecodeEntry(p, limit, &key_len, &value_len); !p) {
      key_.clear();
      value_.clear();
      current_ = size_;
      status_ = Status::Corruption("bad entry in block");
      return;
    }
    valid_ = true;
    key_ = std::string(p, key_len);
    value_ = Slice(p + key_len, value_len);
  }
};

std::unique_ptr<Iterator> Block::NewIterator(const Comparator* comparator) {
  assert(size_);
  return std::make_unique<Block::Iter>(comparator, data_, size_);
}

}  // namespace tinydb
