#pragma once

#include "table/format.h"
#include "table/block.h"
#include "table/iterator_wrapper.h"
#include "tinydb/iterator.h"
#include "tinydb/options.h"
#include "tinydb/table.h"

namespace tinydb {

using BlockFunction = std::function<Iterator*(
    std::any arg, const ReadOptions& options, const Slice&)>;

class TwoLevelIterator : public Iterator {
 public:
  TwoLevelIterator(Iterator* index_iter, BlockFunction block_function,
                   std::any arg, const ReadOptions& options);

  ~TwoLevelIterator() override;

  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() override;
  void Prev() override;

  bool Valid() const override { return data_iter_.Valid(); }
  Slice key() const override {
    assert(Valid());
    return data_iter_.key();
  }
  Slice value() const override {
    assert(Valid());
    return data_iter_.value();
  }
  Status status() const override {
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  BlockFunction block_function_;
  std::any arg_;
  const ReadOptions options_;
  Status status_;
  IteratorWrapper index_iter_;
  IteratorWrapper data_iter_;  // May be nullptr
  // If data_iter_ is non-null, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  std::string data_block_handle_;
};

Iterator* NewTwoLevelIterator(Iterator* index_iter, BlockFunction func,
                              std::any arg, const ReadOptions& options);

} // namespace tinydb
