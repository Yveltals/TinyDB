#include "table/format.h"
#include <memory>
#include "table/block.h"
#include "util/file.h"
#include "common/options.h"
#include "util/coding.h"

namespace tinydb {

void BlockHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != UINT64_MAX);
  assert(size_ != UINT64_MAX);
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

void Footer::EncodeTo(std::string* dst) const {
  filter_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding
}

Status Footer::DecodeFrom(Slice* input) {
  if (input->size() < kEncodedLength) {
    return Status::Corruption("not an sstable (footer too short)");
  }
  Status result = filter_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  // TODO: Should skip over any leftover data (just padding for now) in "input" ?
  return result;
}

Status ReadBlock(RandomAccessFile* file, const ReadOptions& options,
                 const BlockHandle& handle, BlockContents* result) {
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  // Read the block contents as well as the type/crc footer.  TODO
  // See table_builder.cc for the code that built this structure.
  size_t n = static_cast<size_t>(handle.size());
  std::unique_ptr<char[]> buf(new char[n]);
  Slice contents;
  Status s = file->Read(handle.offset(), n, &contents, buf.get());
  if (!s.ok()) {
    return s;
  }
  if (contents.size() != n) {
    return Status::Corruption("truncated block read");
  }
  result->data = Slice(buf.get(), n);
  result->heap_allocated = true;
  result->cachable = true;
  return Status::OK();
}

}  // namespace tinydb
