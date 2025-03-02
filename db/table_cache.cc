#include "db/table_cache.h"
#include "util/coding.h"
#include "util/filename.h"

namespace tinydb {

// Handle in table cache
struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, std::any value) {
  auto tf = std::any_cast<TableAndFile>(value);
  delete tf.table;
  delete tf.file;
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
    auto file = env_->NewRandomAccessFile(fname);
    s = Table::Open(options_, file, file_size, &table);
    if (!s.ok()) {
      assert(table == nullptr);
      delete file;
    } else {
      TableAndFile tf{file, table};
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
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
  auto table_file = std::any_cast<TableAndFile>(cache_->Value(handle));
  auto table = table_file.table;
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
    auto t = std::any_cast<TableAndFile>(cache_->Value(handle)).table;
    s = t->InternalGet(options, key, handler);
    cache_->Release(handle);
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace tinydb
