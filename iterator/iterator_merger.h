#pragma once
#include "common/comparator.h"
#include "common/iterator.h"

namespace tinydb {

class IteratorMerger : public Iterator {
 public:
  IteratorMerger(const Comparator* comparator,
                 std::vector<std::unique_ptr<Iterator>> children)
      : comparator_(comparator),
        children_(std::move(children)),
        current_(nullptr),
        direction_(kForward) {}

  ~IteratorMerger() override {}

  bool Valid() const override { return (current_ != nullptr); }

  void SeekToFirst() override {
    for (auto& child : children_) {
      child->SeekToFirst();
    }
    FindSmallest();
    direction_ = kForward;
  }

  void SeekToLast() override {
    for (auto& child : children_) {
      child->SeekToLast();
    }
    FindLargest();
    direction_ = kReverse;
  }

  void Seek(const Slice& target) override {
    for (auto& child : children_) {
      child->Seek(target);
    }
    FindSmallest();
    direction_ = kForward;
  }

  void Next() override {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kForward) {
      for (auto& child : children_) {
        if (child.get() != current_) {
          child->Seek(key());
          if (child->Valid() &&
              comparator_->Compare(key(), child->key()) == 0) {
            child->Next();
          }
        }
      }
      direction_ = kForward;
    }

    current_->Next();
    FindSmallest();
  }

  void Prev() override {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kReverse) {
      for (auto& child : children_) {
        if (child.get() != current_) {
          child->Seek(key());
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse;
    }

    current_->Prev();
    FindLargest();
  }

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  Status status() const override {
    Status status;
    for (auto& child : children_) {
      if (!child->status().ok()) break;
    }
    return status;
  }

 private:
  // Which direction is the iterator moving?
  enum Direction { kForward, kReverse };

  void FindSmallest();
  void FindLargest();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in tinydb.
  const Comparator* comparator_;
  std::vector<std::unique_ptr<Iterator>> children_;
  Iterator* current_;
  Direction direction_;
};

std::unique_ptr<Iterator> NewMergingIterator(
    const Comparator* comparator,
    std::vector<std::unique_ptr<Iterator>> children);

} // namespace tinydb
