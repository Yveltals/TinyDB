#include "tinydb/env.h"

#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <memory>
#include <utility>

namespace tinydb {

Status SequentialFile::Read(size_t n, Slice* result, char* buffer) {
  if (!file_.is_open()) {
    return Status::IOError(filename_, std::strerror(errno));
  }
  file_.read(buffer, n);
  *result = Slice(buffer, file_.gcount());
  if (file_.gcount() != n) {
    return Status::IOError(filename_, "read bytes not enough");
  }
  return Status::OK();
}

Status WritableFile::Append(const Slice& data) {
  size_t size = data.size();
  const char* write_data = data.data();
  if (!IsAvailable(size)) {
    return Flush();
  }
  memcpy(buffer_.get() + buffer_used_, data.data(), size);
  buffer_used_ += size;
  return Status::OK();
}

Status WritableFile::Flush() {
  if (!file_.is_open()) {
    return Status::IOError(filename_, std::strerror(errno));
  }
  file_ << std::string(buffer_.get(), buffer_used_);
  buffer_used_ = 0;
  return Status::OK();
}

Status WritableFile::Sync() {
  auto st = Flush();
  file_.flush();
  return st;
}

Status WritableFile::Close() {
  auto st = Flush();
  file_.close();
  return st;
}

Status RandomAccessFile::Read(uint64_t offset, size_t n, Slice* result,
                              char* buffer) {
  if (!file_.is_open()) {
    return Status::IOError(filename_, std::strerror(errno));
  }
  file_.seekg(offset);
  file_.read(buffer, n);
  *result = Slice(buffer, file_.gcount());
  if (file_.gcount() != n) {
    return Status::IOError(filename_, "read bytes not enough");
  }
  return Status::OK();
}

Status Env::NewSequentialFile(const std::string& filename,
                              SequentialFile** result) {
  *result = new SequentialFile(filename);
  return Status::OK();
}

Status Env::NewRandomAccessFile(const std::string& filename,
                                RandomAccessFile** result) {
  *result = new RandomAccessFile(filename);
  return Status::OK();
}

Status Env::NewWritableFile(const std::string& filename,
                            WritableFile** result) {
  *result = new WritableFile(filename);
  return Status::OK();
}

} // namespace tinydb
