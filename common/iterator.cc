#include "common/iterator.h"

namespace tinydb {

Iterator::Iterator() {}

Iterator::~Iterator() {
  for (auto it = cleanup_list_.begin(); it != cleanup_list_.end();) {
    it->fun(it->arg1, it->arg2);
    it = cleanup_list_.erase(it);
  }
}

void Iterator::RegisterCleanup(CleanupFun func, std::any arg1, std::any arg2) {
  assert(func);
  cleanup_list_.emplace_back(func, arg1, arg2);
}

namespace {

class EmptyIterator : public Iterator {
 public:
  EmptyIterator(const Status& s) : status_(s) {}
  ~EmptyIterator() override = default;

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

}  // anonymous namespace

std::unique_ptr<Iterator> NewEmptyIterator() {
  return std::make_unique<EmptyIterator>(Status::OK()); 
}
std::unique_ptr<Iterator> NewErrorIterator(const Status& status) {
  return std::make_unique<EmptyIterator>(status);
}

}  // namespace tinydb
