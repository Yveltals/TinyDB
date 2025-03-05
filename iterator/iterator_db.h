#pragma once
#include <cstdint>

#include "db/db_impl.h"

namespace tinydb {

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  IteratorDB
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
class IteratorDB : public Iterator {
 public:
  // Which direction is the iterator currently moving?
  // (1) When moving forward, the internal iterator is positioned at
  //     the exact entry that yields this->key(), this->value()
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().
  enum Direction { kForward, kReverse };

  IteratorDB(DBImpl* db, const Comparator* cmp, std::unique_ptr<Iterator> iter,
             SequenceNumber s)
      : db_(db),
        user_comparator_(cmp),
        iter_(std::move(iter)),
        sequence_(s),
        direction_(kForward),
        valid_(false) {}

  IteratorDB(const IteratorDB&) = delete;
  IteratorDB& operator=(const IteratorDB&) = delete;

  ~IteratorDB() override {}
  bool Valid() const override { return valid_; }
  Slice key() const override {
    assert(valid_);
    return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
  }
  Slice value() const override {
    assert(valid_);
    return (direction_ == kForward) ? iter_->value() : saved_value_;
  }
  Status status() const override {
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  void Next() override;
  void Prev() override;
  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;

 private:
  void FindNextUserEntry(bool skipping, std::string* skip);
  void FindPrevUserEntry();
  bool ParseKey(InternalKey* key);

  inline void SaveKey(const Slice& k, std::string* dst) {
    dst->assign(k.data(), k.size());
  }

  inline void ClearSavedValue() {
    if (saved_value_.capacity() > 1048576) {
      std::string empty;
      swap(empty, saved_value_);
    } else {
      saved_value_.clear();
    }
  }

  DBImpl* db_;
  const Comparator* const user_comparator_;
  std::unique_ptr<Iterator> iter_;
  SequenceNumber const sequence_;
  Status status_;
  std::string saved_key_;   // == current key when direction_==kReverse
  std::string saved_value_; // == current raw value when direction_==kReverse
  Direction direction_;
  bool valid_;
};

// Return a new iterator that converts internal keys (yielded by
// "*internal_iter") that were live at the specified "sequence" number
// into appropriate user keys.
std::unique_ptr<Iterator> NewDBIterator(DBImpl* db,
                                        const Comparator* user_key_comparator,
                                        std::unique_ptr<Iterator> internal_iter,
                                        SequenceNumber sequence);

} // namespace tinydb
