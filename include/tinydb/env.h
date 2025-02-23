#pragma once

#include <vector>
#include "tinydb/status.h"

namespace tinydb {

class SequentialFile;
class WritableFile;

class Env {
 public:
  Env();
  Env(const Env&) = delete;
  Env& operator=(const Env&) = delete;
  virtual ~Env();

  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) = 0;

  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result) = 0;
};

class SequentialFile {
 public:
  SequentialFile() = default;
  SequentialFile(const SequentialFile&) = delete;
  SequentialFile& operator=(SequentialFile&) = delete;
  virtual ~SequentialFile();

  virtual Status Read(size_t n, Slice* result, char* scratch) = 0;
};

class WritableFile {
 public:
  WritableFile() = default;
  WritableFile(const WritableFile&) = delete;
  WritableFile& operator=(const WritableFile&) = delete;
  virtual ~WritableFile();

  virtual Status Append(const Slice& data) = 0;
  virtual Status Close() = 0;
  virtual Status Flush() = 0;
  virtual Status Sync() = 0;
};

} // namespace tinydb
