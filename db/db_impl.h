#pragma once
#include <atomic>
#include <deque>
#include <mutex>
#include <set>
#include <string>

#include "db/db.h"
#include "db/dbformat.h"
#include "db/memtable.h"
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
  const Snapshot* GetSnapshot() override;
  void ReleaseSnapshot(const Snapshot* snapshot) override;
  bool GetProperty(const Slice& property, std::string* value) override;
  void GetApproximateSizes(const Slice& start, const Slice& limit, int n,
                           uint64_t* sizes) override;
  void CompactRange(const Slice* begin, const Slice* end) override;

 private:
  friend class DB;
  struct CompactionState;
  struct Writer;

  std::unique_ptr<Iterator> NewInternalIterator(
      const ReadOptions&, SequenceNumber* latest_snapshot);

  Status NewDB();
  Status Recover(VersionEdit* edit, bool* save_manifest);
  // Delete any unneeded files and stale in-memory entries.
  void RemoveObsoleteFiles();
  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable();
  Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                        VersionEdit* edit, SequenceNumber* max_sequence);
  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base);
  Status MakeRoomForWrite(bool force /* compact even if there is room? */);
  WriteBatch* BuildBatchGroup(Writer** last_writer);
  void RecordBackgroundError(const Status& s);
  // TODO EXCLUSIVE_LOCKS_REQUIRED
  void MaybeScheduleCompaction();
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
  MemTable* imm_;             // Memtable being compacted
  std::atomic<bool> has_imm_; // So bg thread can detect non-null imm_
  std::unique_ptr<log::Writer> log_;
  uint64_t logfile_number_;
  // Queue of writers
  std::deque<Writer*> writers_;
  WriteBatch* tmp_batch_;

  SnapshotList snapshots_;

  VersionSet* const versions_;

  // Files to protect from deletion because they are ongoing compactions
  std::set<uint64_t> pending_outputs_;

  bool bg_compaction_scheduled_;
  Status bg_error_;

  ThreadPool pool_;
};

} // namespace tinydb
