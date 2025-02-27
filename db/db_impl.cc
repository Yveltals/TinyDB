#include "db/db_impl.h"
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <condition_variable>
#include <mutex>
#include <set>
#include <string>
#include <vector>
#include "db/dbformat.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/write_batch_internal.h"
#include "tinydb/cache.h"
#include "tinydb/db.h"
#include "tinydb/env.h"
#include "tinydb/status.h"
#include "tinydb/table.h"
#include "tinydb/table_builder.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/logging.h"

namespace tinydb {

const int kNumNonTableCacheFiles = 10;

struct DBImpl::Writer {
  explicit Writer(std::mutex* mu)
      : batch(nullptr), sync(false), done(false), mu_(mu) {}

  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  std::mutex* const mu_;
  std::condition_variable cv;
};

struct DBImpl::CompactionState {
  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };

  Output* current_output() { return &outputs[outputs.size() - 1]; }

  // explicit CompactionState(Compaction* c)
  //     : compaction(c),
  //       smallest_snapshot(0),
  //       outfile(nullptr),
  //       builder(nullptr),
  //       total_bytes(0) {}

  // Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;
};

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

// Fix user-supplied options to be reasonable
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  auto clip_to_range = [](size_t* ptr, int min, int max) {
    if ((*ptr) > max) *ptr = max;
    if ((*ptr) < min) *ptr = min;
  };
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  clip_to_range(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
  clip_to_range(&result.write_buffer_size, 64 << 10, 1 << 30);
  clip_to_range(&result.max_file_size, 1 << 20, 1 << 30);
  clip_to_range(&result.block_size, 1 << 10, 4 << 20);

  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      shutting_down_(false),
      mem_(nullptr),
      imm_(nullptr),
      has_imm_(false),
      logfile_(nullptr),
      logfile_number_(0),
      seed_(0),
      tmp_batch_(new WriteBatch) {}

DBImpl::~DBImpl() {
  mutex_.lock();
  shutting_down_.store(true, std::memory_order_release);
  // TODO
  mutex_.unlock();

  if (mem_) mem_->Unref();
  if (imm_) imm_->Unref();
  delete tmp_batch_;
  delete table_cache_;
}

} // namespace tinydb
