#include "db/table_cache.h"

#include "util/coding.h"
#include "util/filename.h"

namespace tinydb {

static void DeleteEntry(const Slice& key, std::any value) {
  auto table = std::any_cast<Table*>(value);
  delete table;
}

static void UnrefEntry(std::any arg1, std::any arg2) {
  auto cache = std::any_cast<Cache*>(arg1);
  auto h = std::any_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

Status TableCache::FindOrOpenTable(uint64_t file_number, uint64_t file_size,
                                   Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    std::string fname = SSTTableFileName(dbname_, file_number);
    Table* table = nullptr;
    auto file = file_->NewRandomAccessFile(fname);
    s = Table::Open(options_, std::move(file), file_size, &table);
    if (s.ok()) {
      *handle = cache_->Insert(key, table, 1, &DeleteEntry);
    }
  }
  return s;
}

std::unique_ptr<Iterator> TableCache::NewIterator(const ReadOptions& options,
                                                  uint64_t file_number,
                                                  uint64_t file_size,
                                                  Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }
  Cache::Handle* handle = nullptr;
  auto s = FindOrOpenTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }
  auto table = std::any_cast<Table*>(cache_->Value(handle));
  auto result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& key,
                       HandleResult handler) {
  Cache::Handle* handle = nullptr;
  auto s = FindOrOpenTable(file_number, file_size, &handle);
  if (s.ok()) {
    auto table = std::any_cast<Table*>(cache_->Value(handle));
    s = table->InternalGet(options, key, handler);
    cache_->Release(handle);
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

} // namespace tinydb
