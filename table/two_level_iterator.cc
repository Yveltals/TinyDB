#include "table/two_level_iterator.h"

namespace tinydb {

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,
                                   BlockFunction block_function, std::any arg,
                                   const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(nullptr) {}

void TwoLevelIterator::Seek(const Slice& target) {
  index_iter_.Seek(target);
  // index_iter.value(): block data offset and size
  InitDataBlock();
  if (data_iter_.iter()) data_iter_.Seek(target);
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataBlock();
  if (data_iter_.iter()) data_iter_.SeekToFirst();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter()) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (!data_iter_.iter() || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      data_iter_.Set(nullptr);
      return;
    }
    index_iter_.Next();
    InitDataBlock();
    if (data_iter_.iter()) data_iter_.SeekToFirst();
  }
}

void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (!data_iter_.iter() || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      data_iter_.Set(nullptr);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter()) data_iter_.SeekToLast();
  }
}

void TwoLevelIterator::InitDataBlock() {
  if (!index_iter_.Valid()) {
    data_iter_.Set(nullptr);
    return;
  }
  Slice handle_value = index_iter_.value();
  Iterator* iter = block_function_(arg_, options_, handle_value);
  data_iter_.Set(iter);
}

Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              BlockFunction block_function, std::any arg,
                              const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace tinydb
