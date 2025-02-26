#pragma once

#include <cstdint>
#include <string>

#include "tinydb/slice.h"
#include "tinydb/status.h"

namespace tinydb {

class Block;
class RandomAccessFile;
struct ReadOptions;

class BlockHandle {
 public:
  // Maximum encoding length of a BlockHandle
  enum { kMaxEncodedLength = 10 + 10 };

  BlockHandle() : offset_(UINT64_MAX), size_(UINT64_MAX) {}
  uint64_t offset() const { return offset_; }
  void set_offset(uint64_t offset) { offset_ = offset; }
  uint64_t size() const { return size_; }
  void set_size(uint64_t size) { size_ = size; }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

 private:
  uint64_t offset_;
  uint64_t size_;
};

class Footer {
 public:
  // Encoded length of a Footer
  enum { kEncodedLength = 2 * BlockHandle::kMaxEncodedLength };

  const BlockHandle& filter_handle() const { return filter_handle_; }
  void set_filter_handle(const BlockHandle& h) { filter_handle_ = h; }
  const BlockHandle& index_handle() const { return index_handle_; }
  void set_index_handle(const BlockHandle& h) { index_handle_ = h; }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

 private:
  BlockHandle filter_handle_;
  BlockHandle index_handle_;
};

struct BlockContents {
  Slice data;           // Actual contents of data
  bool cachable;        // True iff data can be cached
  bool heap_allocated;  // True iff caller should delete[] data.data()
};

// Read the block identified by "handle" from "file"
Status ReadBlock(RandomAccessFile* file, const ReadOptions& options,
                 const BlockHandle& handle, BlockContents* result);


}  // namespace tinydb

