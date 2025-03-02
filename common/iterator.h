#pragma once
#include <any>
#include <list>
#include "common/slice.h"
#include "common/status.h"

namespace tinydb {

class Iterator {
 public:
  Iterator();
  Iterator(const Iterator&) = delete;
  Iterator& operator=(const Iterator&) = delete;
  virtual ~Iterator();

  virtual bool Valid() const = 0;
  virtual void SeekToFirst() = 0;
  virtual void SeekToLast() = 0;
  virtual void Seek(const Slice& target) = 0;
  virtual void Next() = 0;
  virtual void Prev() = 0;
  virtual Slice key() const = 0;
  virtual Slice value() const = 0;
  virtual Status status() const = 0;

  using CleanupFun = std::function<void(std::any, std::any)>;
  void RegisterCleanup(CleanupFun fun, std::any arg1, std::any arg2);

 private:
  struct CleanupNode {
    CleanupNode(CleanupFun f, std::any a, std::any b)
        : fun(std::move(f)), arg1(a), arg2(b) {}
    CleanupFun fun;
    std::any arg1;
    std::any arg2;
  };
  std::list<CleanupNode> cleanup_list_;
};

Iterator* NewEmptyIterator();
Iterator* NewErrorIterator(const Status& status);

} // namespace tinydb
