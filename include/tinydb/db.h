#pragma once

#include <cstdint>
#include <cstdio>

#include "tinydb/iterator.h"
#include "tinydb/options.h"

namespace tinydb {

static const int kMajorVersion = 1;
static const int kMinorVersion = 23;

struct Options;
struct ReadOptions;
struct WriteOptions;
class WriteBatch;

class DB {
 public:
  static Status Open(const Options& options, const std::string& name,
                     DB** dbptr);

  DB() = default;
  DB(const DB&) = delete;
  DB& operator=(const DB&) = delete;
  virtual ~DB();

  virtual Status Put(const WriteOptions& options, const Slice& key,
                     const Slice& value) = 0;
  virtual Status Delete(const WriteOptions& options, const Slice& key) = 0;
  virtual Status Write(const WriteOptions& options, WriteBatch* updates) = 0;
  virtual Status Get(const ReadOptions& options, const Slice& key,
                     std::string* value) = 0;
  virtual Iterator* NewIterator(const ReadOptions& options) = 0;
  virtual bool GetProperty(const Slice& property, std::string* value) = 0;
  virtual void GetApproximateSizes(const Slice& start, const Slice& limit,
                                   int n, uint64_t* sizes) = 0;
  virtual void CompactRange(const Slice* begin, const Slice* end) = 0;
};

Status DestoryDB(const std::string& name, const Options& options);
Status RepairDB(const std::string& dbname, const Options& options);

} // namespace tinydb
