#pragma once

#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <vector>
#include "tinydb/status.h"


#define THREAD_ANNOTATION_ATTRIBUTE__(x) __attribute__((x))
#define GUARDED_BY(x) THREAD_ANNOTATION_ATTRIBUTE__(guarded_by(x))

namespace tinydb {

constexpr const size_t kWritableFileBufferSize = 65536;

class SequentialFile;
class RandomAccessFile;
class WritableFile;

class Env {
 public:
  Env();
  Env(const Env&) = delete;
  Env& operator=(const Env&) = delete;
  ~Env();

  SequentialFile* NewSequentialFile(const std::string& filename);
  RandomAccessFile* NewRandomAccessFile(const std::string& filename);
  WritableFile* NewWritableFile(const std::string& filename);

  Status RenameFile(const std::string& from, const std::string& to);
  Status RemoveFile(const std::string& filename);
};

Status WriteStringToFile(Env* env, const Slice& data,
                                const std::string& fname, bool sync);
Status ReadFileToString(Env* env, const std::string& fname,
                               std::string* data);

class SequentialFile {
 public:
  SequentialFile(const std::string& filename) : filename_(filename) {
    file_.open(filename, std::ios::in | std::ios::binary);
  };
  SequentialFile(const SequentialFile&) = delete;
  SequentialFile& operator=(SequentialFile&) = delete;
  ~SequentialFile() { file_.close(); }
  

  Status Read(size_t n, Slice* result, char* scratch);

 private:
  std::ifstream file_;
  std::string filename_;
};

class RandomAccessFile {
 public:
  RandomAccessFile(const std::string& filename) : filename_(filename) {
    file_.open(filename, std::ios::in | std::ios::binary);
  }
  RandomAccessFile(const RandomAccessFile&) = delete;
  RandomAccessFile& operator=(const RandomAccessFile&) = delete;
  ~RandomAccessFile() { file_.close(); }

  Status Read(uint64_t offset, size_t n, Slice* result, char* buffer);

 private:
  std::ifstream file_;
  std::string filename_;
};

class WritableFile {
 public:
  WritableFile(const std::string& filename)
      : buffer_(new char[kWritableFileBufferSize]),
        filename_(filename) {
    file_.open(filename, std::ios::out | std::ios::trunc);
  };
  WritableFile(const WritableFile&) = delete;
  WritableFile& operator=(const WritableFile&) = delete;
  ~WritableFile() { Close(); }

  Status Append(const Slice& data);
  Status Close();
  Status Flush();
  Status Sync();
  bool IsAvailable(int size) { return buffer_size_ - buffer_used_ >= size; }

 private:
  std::unique_ptr<char[]> buffer_;
  int buffer_size_;
  int buffer_used_{0};
  std::ofstream file_;
  std::string filename_;
};

} // namespace tinydb
