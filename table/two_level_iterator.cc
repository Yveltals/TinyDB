#include "table/two_level_iterator.h"

namespace tinydb {

TwoLevelIterator::TwoLevelIterator(std::unique_ptr<Iterator> index_iter,
                                   BuildIterator builder,
                                   const ReadOptions& options)
    : builder_(builder),
      options_(options),
      index_iter_(std::move(index_iter)),
      data_iter_(nullptr) {}

// For table, seek block data offset and size, then init Block::Iter.
// For version_set, seek file number and size to Open table,
// then init Table::Iterator.
void TwoLevelIterator::Seek(const Slice& target) {
  index_iter_->Seek(target);
  InitDataBlock();
  if (data_iter_) data_iter_->Seek(target);
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
  index_iter_->SeekToFirst();
  InitDataBlock();
  if (data_iter_) data_iter_->SeekToFirst();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  index_iter_->SeekToLast();
  InitDataBlock();
  if (data_iter_) data_iter_->SeekToLast();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_->Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_->Prev();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (!data_iter_ || !data_iter_->Valid()) {
    // Move to next block
    if (!index_iter_->Valid()) {
      data_iter_.reset();
      return;
    }
    index_iter_->Next();
    InitDataBlock();
    if (data_iter_) data_iter_->SeekToFirst();
  }
}

void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (!data_iter_ || !data_iter_->Valid()) {
    // Move to next block
    if (!index_iter_->Valid()) {
      data_iter_.reset();
      return;
    }
    index_iter_->Prev();
    InitDataBlock();
    if (data_iter_) data_iter_->SeekToLast();
  }
}

void TwoLevelIterator::InitDataBlock() {
  if (!index_iter_->Valid()) {
    data_iter_.reset();
    return;
  }
  Slice handle_value = index_iter_->value();
  data_iter_ = std::move(builder_(options_, handle_value));
}

std::unique_ptr<Iterator> NewTwoLevelIterator(
    std::unique_ptr<Iterator> index_iter, BuildIterator builder,
    const ReadOptions& options) {
  return std::make_unique<TwoLevelIterator>(std::move(index_iter), builder,
                                            options);
}

}  // namespace tinydb
