#pragma once

#include <cstdint>

#include "common/iterator.h"
#include "common/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/file.h"

namespace tinydb {

// Input: InternalKey and Value
using HandleResult = std::function<void(const Slice&, const Slice&)>;

class Table {
 public:
  static Status Open(const Options& options,
                     std::unique_ptr<RandomAccessFile> file, uint64_t file_size,
                     Table** table);
  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;
  ~Table() {
    delete filter_;
    delete index_block_;
  }

  std::unique_ptr<Iterator> NewIterator(const ReadOptions&) const;
  uint64_t ApproximateOffsetOf(const Slice& key) const;

 private:
  friend class TableCache;

  Table(Options opt, std::unique_ptr<RandomAccessFile> file, uint64_t cache_id,
        Block* index_block)
      : options_(opt),
        file_(std::move(file)),
        cache_id_(cache_id),
        filter_(nullptr),
        index_block_(index_block) {}

  static std::unique_ptr<Iterator> BlockReader(const Table* table,
                                               const ReadOptions&,
                                               const Slice&);

  Status InternalGet(const ReadOptions&, const Slice& key,
                     HandleResult handler);

  void ReadFilter(const Footer& footer);

  Options options_;
  Status status_;
  uint64_t cache_id_;
  std::unique_ptr<RandomAccessFile> file_;
  Block* index_block_;
  FilterBlockReader* filter_;
  uint64_t filter_offset_;
};

} // namespace tinydb
