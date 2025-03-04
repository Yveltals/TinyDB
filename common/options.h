#pragma once
#include <cstddef>

#include "common/comparator.h"
#include "common/filter_policy.h"
#include "db/snapshot.h"
#include "util/cache.h"
#include "util/file.h"

namespace tinydb {

struct Options {
  const Comparator* comparator;
  // If true, the database will be created if it is missing.
  bool create_if_missing = false;
  // If true, an error is raised if the database already exists.
  bool error_if_exists = false;
  File* file;
  size_t write_buffer_size = 4 * 1024 * 1024;
  size_t max_open_files = 1000;
  Cache* block_cache = nullptr;
  size_t block_size = 4 * 1024;
  size_t max_file_size = 2 * 1024 * 1024;
  const FilterPolicy* filter_policy = nullptr;
};

struct ReadOptions {
  // Should the data read for this iteration be cached in memory
  bool fill_cache = true;
  const Snapshot* snapshot = nullptr;
};

struct WriteOptions {
  bool sync = false;
};

} // namespace tinydb
