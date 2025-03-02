#pragma once
#include <string>
#include "common/status.h"
#include "db/dbformat.h"
#include "db/memtable.h"

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


// Helper class to handle WriteBatch
class WriteBatchInternal {
 public:
  static int Count(const WriteBatch* batch) {
    return DecodeFixed32(batch->rep_.data() + 8);
  }

  static void SetCount(WriteBatch* batch, int n) {
    EncodeFixed32(&batch->rep_[8], n);
  }

  static SequenceNumber Sequence(const WriteBatch* batch) {
    return SequenceNumber(DecodeFixed64(batch->rep_.data()));
  }

  static void SetSequence(WriteBatch* batch, SequenceNumber seq) {
    EncodeFixed64(&batch->rep_[0], seq);
  }

  static Slice Contents(const WriteBatch* batch) { return Slice(batch->rep_); }

  static size_t ByteSize(const WriteBatch* batch) { return batch->rep_.size(); }

  static void SetContents(WriteBatch* batch, const Slice& contents) {
    batch->rep_.assign(contents.data(), contents.size());
  }

  static Status InsertInto(const WriteBatch* batch, MemTable* memtable);

  static void Append(WriteBatch* dst, const WriteBatch* src);
};

} // namespace tinydb
