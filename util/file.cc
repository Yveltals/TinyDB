#include "util/file.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <utility>

#include "log/logger.h"

namespace tinydb {

namespace fs = std::filesystem;

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

std::unique_ptr<SequentialFile> File::NewSequentialFile(
    const std::string& filename) {
  return std::make_unique<SequentialFile>(filename);
}

std::unique_ptr<RandomAccessFile> File::NewRandomAccessFile(
    const std::string& filename) {
  return std::make_unique<RandomAccessFile>(filename);
}

std::unique_ptr<WritableFile> File::NewWritableFile(
    const std::string& filename) {
  return std::make_unique<WritableFile>(filename, false);
}

std::unique_ptr<WritableFile> File::NewAppendableFile(
    const std::string& filename) {
  return std::make_unique<WritableFile>(filename, true);
}

Status File::RenameFile(const std::string& from, const std::string& to) {
  if (std::rename(from.c_str(), to.c_str()) != 0) {
    return Status::IOError(from, std::strerror(errno));
  }
  return Status::OK();
}

Status File::RemoveFile(const std::string& filename) {
  if (std::remove(filename.c_str()) != 0) {
    return Status::IOError(filename, std::strerror(errno));
  }
  return Status::OK();
}

Status GetChildren(const std::string& dir_path,
                   std::vector<std::string>* result) {
  result->clear();
  if (fs::exists(dir_path) && fs::is_directory(dir_path)) {
    for (const auto& entry : fs::directory_iterator(dir_path)) {
      if (fs::is_regular_file(entry.status())) {
        result->push_back(entry.path().filename().string());
      }
    }
  } else {
    Logger::Log("Directory {} does not exist", dir_path);
    return Status::IOError(dir_path, std::strerror(errno));
  }
  return Status::OK();
}

Status WriteStringToFile(File* file, const Slice& data,
                         const std::string& fname, bool sync) {
  auto w_file = std::make_unique<WritableFile>(fname, true);
  auto s = w_file->Append(data);
  if (s.ok() && sync) {
    s = w_file->Sync();
  }
  if (s.ok()) {
    s = w_file->Close();
  }
  if (!s.ok()) {
    file->RemoveFile(fname);
  }
  return s;
}

Status ReadFileToString(File* file, const std::string& fname,
                        std::string* data) {
  Status s;
  data->clear();
  auto s_file = std::make_unique<SequentialFile>(fname);
  static const int kBufferSize = 8192;
  std::unique_ptr<char[]> space(new char[kBufferSize]);

  while (true) {
    Slice fragment;
    s = s_file->Read(kBufferSize, &fragment, space.get());
    if (!s.ok()) {
      break;
    }
    data->append(fragment.data(), fragment.size());
    if (fragment.empty()) {
      break;
    }
  }
  return s;
}

} // namespace tinydb
