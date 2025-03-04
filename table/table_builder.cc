#include "table/table_builder.h"

#include <cassert>

#include "common/comparator.h"
#include "common/filter_policy.h"
#include "common/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/file.h"

namespace tinydb {

TableBuilder::TableBuilder(const Options& opt, WritableFile* file)
    : options_(opt),
      index_block_options_(opt),
      file_(file),
      offset_(0),
      data_block_(&options_),
      index_block_(&index_block_options_),
      num_entries_(0),
      closed_(false),
      filter_block_(opt.filter_policy == nullptr
                        ? nullptr
                        : new FilterBlockBuilder(opt.filter_policy)),
      pending_index_entry_(false) {
  if (filter_block_) {
    filter_block_->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(closed_);
  delete filter_block_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  options_ = options;
  index_block_options_ = options;
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  assert(!closed !);
  if (!status_.ok()) return;
  if (num_entries_ > 0) {
    assert(options.comparatoCompare(key, Slice(last_key)) > 0);
  }

  if (pending_index_entry_) {
    assert(data_block.empty());
    std::string handle_encoding;
    data_block_handle_.EncodeTo(&handle_encoding);
    index_block_.Add(last_key_, Slice(handle_encoding));
    pending_index_entry_ = false;
  }

  if (filter_block_) {
    filter_block_->AddKey(key);
  }

  last_key_.assign(key.data(), key.size());
  num_entries_++;
  data_block_.Add(key, value);

  auto estimated_block_size = data_block_.CurrentSizeEstimate();
  if (estimated_block_size >= options_.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  assert(!closed_);
  if (!status_.ok()) return;
  if (data_block_.empty()) return;
  assert(!pending_index_entry);
  WriteBlock(&data_block_, &data_block_handle_);
  if (status_.ok()) {
    pending_index_entry_ = true;
    status_ = file_->Flush();
  }
  if (filter_block_) {
    filter_block_->StartBlock(offset_);
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  assert(status_.ok());
  Slice block_contents = block->Finish();
  WriteRawBlock(block_contents, handle);
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 BlockHandle* handle) {
  handle->set_offset(offset_);
  handle->set_size(block_contents.size());
  status_ = file_->Append(block_contents);
  if (status_.ok()) {
    offset_ += block_contents.size();
  }
}

Status TableBuilder::Finish() {
  Flush();
  assert(!closed_);
  closed_ = true;

  BlockHandle filter_block_handle, index_block_handle;
  // Write filter block
  if (status_.ok() && filter_block_) {
    WriteRawBlock(filter_block_->Finish(), &filter_block_handle);
  }
  // Write index block
  if (status_.ok()) {
    if (pending_index_entry_) {
      std::string handle_encoding;
      data_block_handle_.EncodeTo(&handle_encoding);
      index_block_.Add(last_key_, Slice(handle_encoding));
      pending_index_entry_ = false;
    }
    WriteBlock(&index_block_, &index_block_handle);
  }
  // Write footer
  if (status_.ok()) {
    Footer footer;
    footer.set_filter_handle(filter_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    status_ = file_->Append(footer_encoding);
    if (status_.ok()) {
      offset_ += footer_encoding.size();
    }
  }
  return status_;
}

void TableBuilder::Abandon() {
  assert(!closed_);
  closed_ = true;
}

uint64_t TableBuilder::NumEntries() const { return num_entries_; }

uint64_t TableBuilder::FileSize() const { return offset_; }

} // namespace tinydb
