#pragma once
#include <atomic>
#include <deque>
#include <mutex>
#include <set>
#include <string>
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/db.h"
#include "util/file.h"

namespace tinydb {

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);
  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;
  ~DBImpl() override;

  // Implementations of the DB interface
  Status Put(const WriteOptions&, const Slice& key,
             const Slice& value) override;
  Status Delete(const WriteOptions&, const Slice& key) override;
  Status Write(const WriteOptions& options, WriteBatch* updates) override;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;
  Iterator* NewIterator(const ReadOptions&) override;
  bool GetProperty(const Slice& property, std::string* value) override;
  void GetApproximateSizes(const Slice& start, const Slice& limit, int n,
                           uint64_t* sizes) override;
  void CompactRange(const Slice* begin, const Slice* end) override;

  void RecordReadSaples(Slice key);

 private:
  friend class DB;
  struct CompactionState;
  struct Writer;

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }

    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;
  };

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot,
                                uint32_t* seed);

  Status NewDB();

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable();

  Status MakeRoomForWrite(bool force /* compact even if there is room? */);

  // Constant after construction
  File* const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;  // options_.comparator == &internal_comparator_
  const std::string dbname_;

  // table_cache_ provides its own synchronization
  TableCache* const table_cache_;

  // State below is protected by mutex_
  std::mutex mutex_;
  std::atomic<bool> shutting_down_;
  MemTable* mem_;
  MemTable* imm_ GUARDED_BY(mutex_); // Memtable being compacted
  std::atomic<bool> has_imm_;        // So bg thread can detect non-null imm_
  WritableFile* logfile_;
  uint64_t logfile_number_ GUARDED_BY(mutex_);
  uint32_t seed_ GUARDED_BY(mutex_);

  // Queue of writers
  std::deque<Writer*> writers_ GUARDED_BY(mutex_);
  WriteBatch* tmp_batch_ GUARDED_BY(mutex_);

  CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
Options SanitizeOptions(const std::string& db,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src);

} // namespace tinydb
