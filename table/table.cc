#include "table/table.h"

#include "common/comparator.h"
#include "common/filter_policy.h"
#include "common/options.h"
#include "iterator/iterator_two_level.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/cache.h"
#include "util/coding.h"
#include "util/file.h"

namespace tinydb {

Status Table::Open(const Options& options,
                   std::unique_ptr<RandomAccessFile> file, uint64_t size,
                   Table** table) {
  *table = nullptr;
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  BlockContents index_block_contents;
  ReadOptions opt;
  s = ReadBlock(file.get(), opt, footer.index_handle(), &index_block_contents);

  if (s.ok()) {
    auto cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    auto index_block = new Block(index_block_contents);
    *table = new Table(options, std::move(file), cache_id, index_block);
    (*table)->ReadFilter(footer);
  }
  return s;
}

void Table::ReadFilter(const Footer& footer) {
  if (!options_.filter_policy) {
    return;
  }
  // Read the filter handle
  ReadOptions opt;
  BlockContents contents;
  if (!ReadBlock(file_.get(), opt, footer.filter_handle(), &contents).ok()) {
    return;
  }
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&contents.data).ok()) {
    return;
  }
  filter_offset_ = filter_handle.offset();
  // Read the filter block
  BlockContents block;
  if (!ReadBlock(file_.get(), opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    filter_data_ = block.data.data(); // Will need to delete later
  }
  filter_ = new FilterBlockReader(options_.filter_policy, block.data);
}

static void DeleteBlock(std::any arg, std::any ignored) {
  delete std::any_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, std::any value) {
  delete std::any_cast<Block*>(value);
}

static void ReleaseBlock(std::any arg, std::any h) {
  auto cache = std::any_cast<Cache*>(arg);
  auto handle = std::any_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Read data block (maybe from cache), return IteratorBlock to seek keys
std::unique_ptr<Iterator> Table::BlockReader(const Table* table,
                                             const ReadOptions& options,
                                             const Slice& handle_value) {
  auto block_cache = table->options_.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = handle_value;
  Status st = handle.DecodeFrom(&input);
  if (st.ok()) {
    BlockContents contents;
    if (block_cache) {
      char cache_key_buffer[16]; // cache_id + block_offset
      EncodeFixed64(cache_key_buffer, table->cache_id_);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle) {
        block = std::any_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        st = ReadBlock(table->file_.get(), options, handle, &contents);
        if (st.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(
                key, std::any(block), block->size(), &DeleteCachedBlock);
          }
        }
      }
    } else {
      st = ReadBlock(table->file_.get(), options, handle, &contents);
      if (st.ok()) {
        block = new Block(contents);
      }
    }
  }

  if (!block) {
    return NewErrorIterator(st);
  }
  auto iter = block->NewIterator(table->options_.comparator);
  if (!cache_handle) {
    iter->RegisterCleanup(&DeleteBlock, std::any(block), std::any{});
  } else {
    iter->RegisterCleanup(&ReleaseBlock, std::any(block_cache),
                          std::any(cache_handle));
  }
  return iter;
}

std::unique_ptr<Iterator> Table::NewIterator(const ReadOptions& options) const {
  auto block_reader = [this](const ReadOptions& options,
                             const Slice& handle_value) {
    return BlockReader(this, options, handle_value);
  };
  return NewTwoLevelIterator(index_block_->NewIterator(options_.comparator),
                             block_reader, options);
}

Status Table::InternalGet(const ReadOptions& options, const Slice& key,
                          HandleResult handler) {
  Status s;
  auto iiter = index_block_->NewIterator(options_.comparator);
  // Seek the block containing the key
  iiter->Seek(key);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    BlockHandle handle;
    auto st = handle.DecodeFrom(&handle_value);
    if (st.ok() && filter_ && !filter_->KeyMayMatch(handle.offset(), key)) {
      // Not found by filter
    } else {
      // Seek the key in block
      auto block_iter = BlockReader(this, options, iiter->value());
      if (!block_iter->Valid()) {
        return block_iter->status();
      }
      block_iter->Seek(key);
      if (block_iter->Valid()) {
        handler(block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  auto index_iter = index_block_->NewIterator(options_.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      return handle.offset();
    }
  }
  return filter_offset_; // approximate the last key
}

} // namespace tinydb
