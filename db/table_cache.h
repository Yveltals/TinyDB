#pragma once
#include <cstdint>
#include <string>
#include "common/iterator.h"
#include "common/options.h"
#include "db/dbformat.h"
#include "table/table.h"
#include "util/file.h"

namespace tinydb {

class TableCache {
 public:
  TableCache(const std::string& dbname, const Options& options, int entries)
      : env_(options.file),
        dbname_(dbname),
        options_(options),
        cache_(NewLRUCache(entries)) {}
  TableCache(const TableCache&) = delete;
  TableCache& operator=(const TableCache&) = delete;
  ~TableCache() { delete cache_; }

  std::unique_ptr<Iterator> NewIterator(const ReadOptions& options,
                                        uint64_t file_number,
                                        uint64_t file_size,
                                        Table** tableptr = nullptr);

  Status Get(const ReadOptions& options, uint64_t file_number,
             uint64_t file_size, const Slice& k, HandleResult handler);

  void Evict(uint64_t file_number);

 private:
  // Find table by file number from cache.
  // If not existed, open SSTable and insert to cache
  Status FindOrOpenTable(uint64_t file_number, uint64_t file_size,
                         Cache::Handle**);

  File* const env_;
  const std::string dbname_;
  const Options& options_;
  Cache* cache_;
};

} // namespace tinydb
