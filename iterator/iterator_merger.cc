#include "iterator/iterator_merger.h"

#include "iterator/iterator_base.h"

namespace tinydb {

void IteratorMerger::FindSmallest() {
  Iterator* smallest = nullptr;
  for (auto& child : children_) {
    if (child->Valid()) {
      if (smallest == nullptr) {
        smallest = child.get();
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        smallest = child.get();
      }
    }
  }
  current_ = smallest;
}

void IteratorMerger::FindLargest() {
  Iterator* largest = nullptr;
  for (auto it = children_.rbegin(); it != children_.rend(); it++) {
    auto& child = *it;
    if (child->Valid()) {
      if (largest == nullptr) {
        largest = child.get();
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
        largest = child.get();
      }
    }
  }
  current_ = largest;
}

std::unique_ptr<Iterator> NewMergingIterator(
    const Comparator* comparator,
    std::vector<std::unique_ptr<Iterator>> children) {
  if (children.empty()) {
    return NewEmptyIterator();
  } else {
    return std::make_unique<IteratorMerger>(comparator, std::move(children));
  }
}

} // namespace tinydb
