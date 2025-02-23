#pragma once
#include <any>
#include "tinydb/slice.h"
#include "tinydb/status.h"

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
    bool IsEmpty() const { return fun == nullptr; }
    void Run() {
      assert(fun != nullptr);
      fun(arg1, arg2);
    }

    CleanupFun fun;
    std::any arg1;
    std::any arg2;
    CleanupNode* next;
  };
  CleanupNode cleanup_head_;
};

} // namespace tinydb
