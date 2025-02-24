#pragma once

#include <string>
#include "tinydb/status.h"

namespace tinydb {

class WriteBatch {
 public:
  class Handler {
   public:
    virtual ~Handler();
    virtual void Put(const Slice& key, const Slice& value) = 0;
    virtual void Delete(const Slice& key) = 0;
  };

  WriteBatch() { Clear(); }
  WriteBatch(const WriteBatch&) = default;
  WriteBatch& operator=(const WriteBatch& key) = default;
  ~WriteBatch() = default;

  void Put(const Slice& key, const Slice& value);
  void Delete(const Slice& key);
  void Clear();
  size_t ApproximateSize() const { return rep_.size(); }
  void Append(const WriteBatch& source);
  Status Iterate(Handler* handler) const;

 private:
  friend class WriteBatchInternal;

  std::string rep_;
};

} // namespace tinydb
