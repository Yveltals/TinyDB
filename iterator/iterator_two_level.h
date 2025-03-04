#pragma once

#include "common/options.h"
#include "iterator/iterator_base.h"
#include "table/block.h"
#include "table/format.h"
#include "table/table.h"

namespace tinydb {

// Build iterator of block entries
using BuildIterator = std::function<std::unique_ptr<Iterator>(
    const ReadOptions& options, const Slice&)>;

class IteratorTwoLevel : public Iterator {
 public:
  IteratorTwoLevel(std::unique_ptr<Iterator> index_iter, BuildIterator builder,
                   const ReadOptions& options);

  ~IteratorTwoLevel() override;

  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() override;
  void Prev() override;

  bool Valid() const override { return data_iter_->Valid(); }
  Slice key() const override {
    assert(Valid());
    return data_iter_->key();
  }
  Slice value() const override {
    assert(Valid());
    return data_iter_->value();
  }
  Status status() const override {
    if (!index_iter_->status().ok()) {
      return index_iter_->status();
    } else if (data_iter_ && !data_iter_->status().ok()) {
      return data_iter_->status();
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
  void InitDataBlock();

  BuildIterator builder_;
  const ReadOptions options_;
  Status status_;
  std::unique_ptr<Iterator> index_iter_;
  std::unique_ptr<Iterator> data_iter_; // May be nullptr
};

std::unique_ptr<Iterator> NewTwoLevelIterator(
    std::unique_ptr<Iterator> index_iter, BuildIterator builder,
    const ReadOptions& options);

} // namespace tinydb
