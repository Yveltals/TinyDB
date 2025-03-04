#pragma once
#include "common/iterator.h"

namespace tinydb {

class IteratorBase : public Iterator {
 public:
  IteratorBase(const Status& s) : status_(s) {}
  ~IteratorBase() override = default;

  bool Valid() const override { return false; }
  void Seek(const Slice& target) override {}
  void SeekToFirst() override {}
  void SeekToLast() override {}
  void Next() override { assert(false); }
  void Prev() override { assert(false); }
  Slice key() const override { return Slice(); }
  Slice value() const override { return Slice(); }
  Status status() const override { return status_; }

 private:
  Status status_;
};

inline std::unique_ptr<Iterator> NewEmptyIterator() {
  return std::make_unique<IteratorBase>(Status::OK());
}

inline std::unique_ptr<Iterator> NewErrorIterator(const Status& status) {
  return std::make_unique<IteratorBase>(status);
}

} // namespace tinydb
