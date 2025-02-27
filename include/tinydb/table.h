#pragma once

#include <cstdint>
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "tinydb/iterator.h"
#include "tinydb/env.h"
#include "tinydb/options.h"

namespace tinydb {

using HandleResult = std::function<void(std::any, const Slice&, const Slice&)>;

class Table {
 public:
  static Status Open(const Options& options, RandomAccessFile* file,
                     uint64_t file_size, Table** table);
  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;
  ~Table() {
    delete filter_;
    delete[] filter_data_;
    delete index_block_;
  }

  Iterator* NewIterator(const ReadOptions&) const;
  uint64_t ApproximateOffsetOf(const Slice& key) const;

 private:
  friend class TableCache;

  Table(Options opt, RandomAccessFile* file, uint64_t cache_id,
        Block* index_block)
      : options_(opt),
        file_(file),
        cache_id_(cache_id),
        filter_(nullptr),
        filter_data_(nullptr),
        index_block_(index_block) {}

  static Iterator* BlockReader(std::any, const ReadOptions&, const BlockHandle&);

  Status InternalGet(const ReadOptions&, const Slice& key, std::any arg,
                     HandleResult handler);
  
  void ReadFilter(const Footer& footer);

  Options options_;
  Status status_;
  RandomAccessFile* file_;
  uint64_t cache_id_;
  FilterBlockReader* filter_;
  const char* filter_data_;
  Block* index_block_;
  uint64_t filter_offset_;
};

} // namespace tinydb
