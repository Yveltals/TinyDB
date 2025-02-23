#include "tinydb/env.h"

#include <unistd.h>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <memory>
#include <utility>

namespace tinydb {

constexpr const size_t kWritableFileBufferSize = 65536;

Status PosixError(const std::string& context, int error_number) {
  if (error_number == ENOENT) {
    return Status::NotFound(context, std::strerror(error_number));
  } else {
    return Status::IOError(context, std::strerror(error_number));
  }
}

class SequentialFileImpl final : public SequentialFile {
 public:
  SequentialFileImpl(std::string fname, int fd)
      : fd_(fd), fname_(std::move(fname)) {}

  Status Read(size_t n, Slice* result, char* scratch) override {
    Status st;
    while (true) {
      auto read_size = ::read(fd_, scratch, n);
      if (read_size < 0) {
        if (errno == EINTR) {
          continue;
        }
        st = PosixError(fname_, errno);
        break;
      }
      *result = Slice(scratch, read_size);
      break;
    }
    return st;
  }

 private:
  const int fd_;
  const std::string fname_;
};

class WritableFileImpl final : public WritableFile {
 public:
  WritableFileImpl(std::string fname, int fd)
      : buffer_(new char[kWritableFileBufferSize]), fd_(fd), fname_(std::move(fname)) {}
  ~WritableFileImpl() override { Close(); }
  

  Status Append(const Slice& data) override {
    size_t size = data.size();
    const char* write_data = data.data();
    if (!IsAvailable(size)) {
      return Flush();
    }
    WriteToBuffer(data.data(), size);
    return Status::OK();
  }

  void WriteToBuffer(const char *data, int size) {
    assert(IsAvailable(size));
    memcpy(buffer_.get() + buffer_used_, data, size);
    buffer_used_ += size;
  }

  Status Flush() override {
    while (buffer_size_ > 0) {
      ssize_t write_result = ::write(fd_, buffer_.get(), buffer_size_);
      if (write_result < 0) {
        if (errno == EINTR) {
          continue;  // Retry
        }
        return PosixError(fname_, errno);
      }
      buffer_size_ = 0;
    }
    return Status::OK();
  }

  Status Sync() override {
    auto st = Flush();
    if (!st.ok()) return st;

    auto sync_success = ::fdatasync(fd_) == 0;
    if (sync_success) {
      return Status::OK();
    }
    return PosixError(fname_, errno);
  }

  Status Close() override {
    auto st = Flush();
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
    return st;
  }
  
  bool IsAvailable(int size) { return buffer_size_ - buffer_used_ >= size; }

 private:
  std::unique_ptr<char[]> buffer_;
  int buffer_size_;
  int buffer_used_{0};
  int fd_;
  const std::string fname_;
};

} // namespace tinydb
