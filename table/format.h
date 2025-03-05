#pragma once
#include <cstdint>
#include <string>

#include "common/options.h"
#include "common/slice.h"
#include "common/status.h"
#include "util/file.h"

namespace tinydb {

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
  Status DecodeFrom(Slice& input);
  Status DecodeFrom(char* input, size_t n);

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
  Status DecodeFrom(Slice& input);
  Status DecodeFrom(char* input, size_t n);

 private:
  BlockHandle filter_handle_;
  BlockHandle index_handle_;
};

struct BlockContents {
  std::unique_ptr<char[]> data; // data contents
  size_t size;                  // data length
  bool cachable;                // True if data can be cached
};

// Read the block identified by "handle" from "file"
Status ReadBlock(RandomAccessFile* file, const ReadOptions& options,
                 const BlockHandle& handle, BlockContents* result);

} // namespace tinydb
