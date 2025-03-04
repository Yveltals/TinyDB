#include "db/builder.h"

#include "db/db.h"
#include "db/version_edit.h"
#include "table/table_builder.h"
#include "util/file.h"
#include "util/filename.h"

namespace tinydb {

Status BuildTable(const std::string& dbname, File* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = SSTTableFileName(dbname, meta->number);
  if (iter->Valid()) {
    auto file = env->NewWritableFile(fname);
    auto builder = std::make_unique<TableBuilder>(options, std::move(file));
    meta->smallest.DecodeFrom(iter->key());
    Slice key;
    for (; iter->Valid(); iter->Next()) {
      key = iter->key();
      builder->Add(key, iter->value());
    }
    if (!key.empty()) {
      meta->largest.DecodeFrom(key);
    }

    s = builder->Finish();
    if (s.ok()) {
      // Finish and check for builder errors
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
      builder->Flush();
      // Verify that the table is usable
      auto it = table_cache->NewIterator(ReadOptions(), meta->number,
                                         meta->file_size);
      s = it->status();
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(fname);
  }
  return s;
}

} // namespace tinydb
