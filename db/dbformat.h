#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
#include "common/comparator.h"
#include "common/filter_policy.h"
#include "common/slice.h"
#include "util/coding.h"
#include "log/logging.h"

namespace tinydb {

namespace config {
static const int kNumLevels = 7;
// Level-0 compaction is started when we hit this many files.
static const int kL0_CompactionTrigger = 4;
// Soft limit on number of level-0 files.  We slow down writes at this point.
static const int kL0_SlowdownWritesTrigger = 8;
// Maximum number of level-0 files.  We stop writes at this point.
static const int kL0_StopWritesTrigger = 12;
// Maximum level to which a new compacted memtable is pushed if it
// does not create overlap.  We try to push to level 2 to avoid the
// relatively expensive level 0=>1 compactions and to avoid some
// expensive manifest file operations.  We do not push all the way to
// the largest level since that can generate a lot of wasted disk
// space if the same key space is being repeatedly overwritten.
static const int kMaxMemCompactLevel = 2;
// Approximate gap in bytes between samples of data read during iteration.
static const int kReadBytesPeriod = 1048576;
} // namespace config


enum ValueType { kTypeDeletion = 0x0, kTypeValue = 0x1 };
static const ValueType kValueTypeForSeek = kTypeValue;

using SequenceNumber = uint64_t;
static const SequenceNumber kMaxSequenceNumber = ((0x1ull << 56) - 1);

inline Slice ExtractUserKey(const Slice& internal_key) {
  assert(internal_key.size() >= 8);
  return Slice(internal_key.data(), internal_key.size() - 8);
}

static uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
  assert(seq <= kMaxSequenceNumber);
  assert(t <= kValueTypeForSeek);
  return (seq << 8) | t;
}

// Encode: user_key + seq_num(56bit)+ type(8bit)
class InternalKey {
 public:
  InternalKey() = default;
  InternalKey(const Slice& user_key, SequenceNumber seq, ValueType t)
      : user_key_(user_key), sequence_(seq), type_(t) {
    data_.append(user_key_.data(), user_key_.size());
    PutFixed64(&data_, PackSequenceAndType(sequence_, type_));
  }
  // Parse InternalKey from slice
  bool DecodeFrom(const Slice& s);
  Slice UserKey() const { return user_key_; }
  SequenceNumber Sequence() const { return sequence_; }
  ValueType Type() const { return type_; }
  Slice Data() const { return data_; }
  void Clear() {
    user_key_.clear();
    data_.clear();
  }
  std::string DebugString() const;

 private:
  Slice user_key_;
  SequenceNumber sequence_;
  ValueType type_;
  std::string data_;
};

// Compare user_key first, compare seq_no when equal.
class InternalKeyComparator : public Comparator {
 public:
  explicit InternalKeyComparator(const Comparator* c) : user_comparator_(c) {}
  const char* Name() const override { return "tinydb.InternalKeyComparator"; }
  const Comparator* user_comparator() const { return user_comparator_; }
  int Compare(const Slice& a, const Slice& b) const override;
  int Compare(const InternalKey& a, const InternalKey& b) const {
    return Compare(a.Data(), b.Data());
  }

 private:
  const Comparator* user_comparator_;
};

// Filter policy wrapper that converts from internal keys to user keys
class InternalFilterPolicy : public FilterPolicy {
 public:
  explicit InternalFilterPolicy(const FilterPolicy* p) : user_policy_(p) {}
  const char* Name() const override { return user_policy_->Name(); }
  void CreateFilter(const Slice* keys, int n, std::string* dst) const override;
  bool KeyMayMatch(const Slice& key, const Slice& filter) const override;

 private:
  const FilterPolicy* const user_policy_;
};

// InternalKeySize + InternalKey.
// Initialize *this for looking up user_key at a snapshot with
// the specified sequence number.
class LookupKey {
 public:
  LookupKey(const Slice& user_key, SequenceNumber sequence);
  LookupKey(const LookupKey&) = delete;
  LookupKey& operator=(const LookupKey&) = delete;
  ~LookupKey() { delete[] start_; }

  Slice memtable_key() const { return Slice(start_, end_ - start_); }
  Slice internal_key() const { return Slice(kstart_, end_ - kstart_); }
  Slice user_key() const { return Slice(kstart_, end_ - kstart_ - 8); }

 private:
  const char* start_;
  const char* kstart_;
  const char* end_;
};

} // namespace tinydb
