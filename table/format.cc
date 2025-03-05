#include "table/format.h"

#include <memory>

#include "common/options.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/file.h"

namespace tinydb {

void BlockHandle::EncodeTo(std::string* dst) const {
  assert(offset_ != UINT64_MAX);
  assert(size_ != UINT64_MAX);
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(char* data, size_t n) {
  Slice input(data, n);
  return DecodeFrom(input);
}

Status BlockHandle::DecodeFrom(Slice& input) {
  if (GetVarint64(&input, &offset_) && GetVarint64(&input, &size_)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

void Footer::EncodeTo(std::string* dst) const {
  filter_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);
  dst->resize(2 * BlockHandle::kMaxEncodedLength); // Padding
}

Status Footer::DecodeFrom(char* data, size_t n) {
  Slice input(data, n);
  return DecodeFrom(input);
}

Status Footer::DecodeFrom(Slice& input) {
  if (input.size() < kEncodedLength) {
    return Status::Corruption("not an sstable (footer too short)");
  }
  Status result = filter_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  return result;
}

Status ReadBlock(RandomAccessFile* file, const ReadOptions& options,
                 const BlockHandle& handle, BlockContents* result) {
  result->size = 0;
  result->data.reset();
  result->cachable = false;

  auto n = handle.size();
  std::unique_ptr<char[]> buf(new char[n]);
  Slice contents;
  Status s = file->Read(handle.offset(), n, &contents, buf.get());
  if (!s.ok()) {
    return s;
  }
  if (contents.size() != n) {
    return Status::Corruption("truncated block read");
  }
  result->size = n;
  result->data = std::move(buf);
  result->cachable = true;
  return Status::OK();
}

} // namespace tinydb
