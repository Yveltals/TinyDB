#pragma once
#include "common/iterator.h"
#include "common/options.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "util/file.h"

namespace tinydb {

// Build a Table file from the contents of *iter.  The generated file
// will be named according to meta->number.  On success, the rest of
// *meta will be filled with metadata about the generated table.
// If no data is present in *iter, meta->file_size will be set to
// zero, and no Table file will be produced.
Status BuildTable(const std::string& dbname, File* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta);

} // namespace tinydb
