#pragma once
#include <string>
#include "common/iterator.h"
#include "db/skiplist.h"
#include "db/dbformat.h"
#include "util/arena.h"

namespace tinydb {

class MemTableIterator;

class MemTable {
 public:
  // MemTables are reference count. caller must call Ref() at least once
  explicit MemTable(const InternalKeyComparator& comparator)
      : comparator_(comparator), refs_(0), table_(comparator_, &arena_) {}

  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  void Ref() { ++refs_; }
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  size_t ApproximateMemoryUsage() { return arena_.MemoryUsage(); }
  std::unique_ptr<Iterator> NewIterator();
  void Add(SequenceNumber seq, ValueType type, const Slice& key,
           const Slice& value);
  // If contains value, store it in *value ans return true
  // If contains a deletion for key, store a NotFound() and return true
  // Else, return false
  bool Get(const LookupKey& key, std::string* value, Status* s);

 private:
  friend class MemTableIterator;
  friend class MemTableBackwardIterator;

  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
    int operator()(const char* a, const char* b) const;
  };

  using Table = SkipList<const char*, KeyComparator>;

  ~MemTable() { assert(refs_ == 0); }

  KeyComparator comparator_;
  int refs_;
  Arena arena_;
  Table table_;
};

} // namespace tinydb
