#pragma once

#include <cstdint>
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "tinydb/env.h"
#include "tinydb/options.h"
#include "tinydb/status.h"

namespace tinydb {

class TableBuilder {
 public:
  TableBuilder(const Options& opt, WritableFile* file);
  TableBuilder(const TableBuilder&) = delete;
  TableBuilder& operator=(const TableBuilder&) = delete;
  ~TableBuilder();

  Status ChangeOptions(const Options& options);
  void Add(const Slice& key, const Slice& value);
  void Flush();
  Status Finish();
  void Abandon();
  uint64_t NumEntries() const;
  uint64_t FileSize() const;

 private:
  void WriteBlock(BlockBuilder* block, BlockHandle* handle);
  void WriteRawBlock(const Slice& data, BlockHandle* handle);

  Options options_;
  Options index_block_options_;
  WritableFile* file_;
  uint64_t offset_;
  BlockBuilder data_block_;
  BlockBuilder index_block_;
  FilterBlockBuilder* filter_block_;

  std::string last_key_;
  int64_t num_entries_;
  Status status_;
  bool closed_;
  // Should add index for current key? Only when a new data block created
  bool pending_index_entry_;
  BlockHandle data_block_handle_;  // Current pended data block
};

} // namespace tinydb
