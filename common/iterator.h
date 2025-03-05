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
      (*it)();
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

  void RegisterCleanup(std::function<void()> func) {
    cleanup_list_.emplace_back(func);
  }

 private:
  std::list<std::function<void()>> cleanup_list_;
};

} // namespace tinydb
