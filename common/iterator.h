#pragma once
#include <any>
#include <list>
#include <memory>

#include "common/slice.h"
#include "common/status.h"

namespace tinydb {

class Iterator {
 public:
  Iterator() = default;
  Iterator(const Iterator&) = delete;
  Iterator& operator=(const Iterator&) = delete;
  virtual ~Iterator() {
    for (auto it = cleanup_list_.begin(); it != cleanup_list_.end();) {
      it->fun(it->arg1, it->arg2);
      it = cleanup_list_.erase(it);
    }
  }

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
  void RegisterCleanup(CleanupFun func, std::any arg1, std::any arg2) {
    cleanup_list_.emplace_back(func, arg1, arg2);
  }

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

} // namespace tinydb
