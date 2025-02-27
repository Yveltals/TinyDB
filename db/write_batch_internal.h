#pragma once
#include "db/dbformat.h"
#include "db/memtable.h"
#include "tinydb/write_batch.h"

namespace tinydb {

// class MemTable;  TODO

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
