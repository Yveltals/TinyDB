#pragma once
#include <atomic>
#include <deque>
#include <mutex>
#include <set>
#include <string>

#include "db/db.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/snapshot.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "log/log_writer.h"
#include "util/file.h"
#include "util/thread_pool.h"

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
  std::unique_ptr<Iterator> NewIterator(const ReadOptions&) override;
  bool GetProperty(const Slice& property, std::string* value) override;
  void GetApproximateSizes(const Slice& start, const Slice& limit, int n,
                           uint64_t* sizes) override;
  void CompactRange(const Slice* begin, const Slice* end) override;

  void RecordReadSaples(Slice key);

 private:
  friend class DB;
  struct CompactionState;
  struct Writer;

  std::unique_ptr<Iterator> NewInternalIterator(const ReadOptions&,
                                                SequenceNumber* latest_snapshot,
                                                uint32_t* seed);

  Status NewDB();
  // Delete any unneeded files and stale in-memory entries.
  void RemoveObsoleteFiles();
  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable();

  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base);
  Status MakeRoomForWrite(bool force /* compact even if there is room? */);
  void RecordBackgroundError(const Status& s);
  // TODO EXCLUSIVE_LOCKS_REQUIRED
  void MaybeScheduleCompaction();
  static void BGWork(void* db);
  void BackgroundCall();
  void BackgroundCompaction();
  Status DoCompactionWork(CompactionState* compact);
  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status InstallCompactionResults(CompactionState* compact);

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  // Constant after construction
  File* const file_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_; // options_.comparator == &internal_comparator_
  const std::string dbname_;

  // table_cache_ provides its own synchronization
  TableCache* const table_cache_;

  // State below is protected by mutex_
  std::mutex mutex_;
  std::atomic<bool> shutting_down_;
  std::condition_variable bg_work_finished_signal_;
  MemTable* mem_;
  MemTable* imm_ GUARDED_BY(mutex_); // Memtable being compacted
  std::atomic<bool> has_imm_;        // So bg thread can detect non-null imm_
  // WritableFile* logfile_;
  std::unique_ptr<log::Writer> log_;
  uint64_t logfile_number_ GUARDED_BY(mutex_);
  uint32_t seed_ GUARDED_BY(mutex_);

  // Queue of writers
  std::deque<Writer*> writers_ GUARDED_BY(mutex_);
  WriteBatch* tmp_batch_ GUARDED_BY(mutex_);

  SnapshotList snapshots_ GUARDED_BY(mutex_);

  VersionSet* const versions_ GUARDED_BY(mutex_);

  // Files to protect from deletion because they are ongoing compactions
  std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);

  bool bg_compaction_scheduled_ GUARDED_BY(mutex_);

  // Have we encountered a background error in paranoid mode?
  Status bg_error_ GUARDED_BY(mutex_);

  ThreadPool pool_;
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
Options SanitizeOptions(const std::string& db,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src);

} // namespace tinydb
