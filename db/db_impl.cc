#include "db/db_impl.h"

#include <fmt/core.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/db.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "iterator/iterator_db.h"
#include "iterator/iterator_merger.h"
#include "log/log_reader.h"
#include "log/log_writer.h"
#include "table/block.h"
#include "table/table.h"
#include "table/table_builder.h"
#include "util/cache.h"
#include "util/coding.h"
#include "util/file.h"
#include "util/filename.h"

namespace tinydb {

const int kNumNonTableCacheFiles = 10;

struct DBImpl::Writer {
  explicit Writer(std::mutex* mu)
      : batch(nullptr), sync(false), done(false), mu_(mu) {}

  Status st;
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

  Output& current_output() { return outputs.back(); }

  explicit CompactionState(std::unique_ptr<Compaction> c)
      : compaction(std::move(c)),
        smallest_snapshot(0),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {}
  ~CompactionState() { compaction->ReleaseInputs(); }

  std::unique_ptr<Compaction> compaction;
  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;
  std::vector<Output> outputs;
  // State kept for output being generated
  std::unique_ptr<WritableFile> outfile;
  std::unique_ptr<TableBuilder> builder;
  uint64_t total_bytes;
};

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

static Options SanitizeOptions(const std::string& dbname,
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
    : file_(raw_options.file),
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
      log_(nullptr),
      logfile_number_(0),
      tmp_batch_(new WriteBatch),
      bg_compaction_scheduled_(false),
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_)) {}

DBImpl::~DBImpl() {
  {
    std::unique_lock<std::mutex> l(mutex_);
    shutting_down_.store(true, std::memory_order_release);
    bg_work_finished_signal_.wait(l,
                                  [this] { return !bg_compaction_scheduled_; });
  }
  if (mem_) mem_->Unref();
  if (imm_) imm_->Unref();
  delete tmp_batch_;
  delete table_cache_;
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  auto file = file_->NewWritableFile(manifest);
  log::Writer log(std::move(file));
  std::string record;
  new_db.EncodeTo(&record);

  Status s = log.AddRecord(record);
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(file_, dbname_, 1);
  } else {
    file_->RemoveFile(manifest);
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  if (bg_error_.ok()) {
    bg_error_ = s;
    bg_work_finished_signal_.notify_all();
  }
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  // mutex_.AssertHeld();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  auto iter = mem->NewIterator();
  Logger::Log("Level-0 table {} started", meta.number);

  Status s;
  {
    mutex_.unlock();
    s = BuildTable(dbname_, file_, options_, table_cache_, iter.get(), &meta);
    mutex_.lock();
  }
  pending_outputs_.erase(meta.number);

  Logger::Log("Level-0 table {}: {} bytes {}", meta.number, meta.file_size,
              s.ToString());
  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.UserKey();
    const Slice max_user_key = meta.largest.UserKey();
    if (base != nullptr) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                  meta.largest);
  }
  return s;
}

Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
  // mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  file_->CreateDir(dbname_);
  assert(db_lock_ == nullptr);
  Status s; // = file_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!file_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      Logger::Log("Creating DB {} since it was missing.", dbname_);
      if (s = NewDB(); !s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(dbname_,
                                     "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  s = file_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    auto msg = fmt::format("{} missing files; e.g.", expected.size());
    return Status::Corruption(msg,
                              SSTTableFileName(dbname_, *(expected.begin())));
  }
  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }
    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }
  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }
  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  // mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  auto file = file_->NewSequentialFile(fname);
  log::Reader reader(std::move(file));
  Logger::Log("Recovering log {}", log_number);

  // Read all the records and add to a memtable
  Status st;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;
  while (reader.ReadRecord(&record) && st.ok()) {
    if (record.size() < 12) {
      Logger::Log("{}: dropping {} bytes; {}", fname, record.size(),
                  Status::Corruption("log record too small").ToString());
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    st = WriteBatchInternal::InsertInto(&batch, mem);
    if (!st.ok()) {
      Logger::Log("Ignoring error {}", st.ToString());
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      st = WriteLevel0Table(mem, edit, nullptr);
      mem->Unref();
      mem = nullptr;
      if (!st.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  if (mem != nullptr) {
    // mem did not get reused; compact it.
    if (st.ok()) {
      *save_manifest = true;
      st = WriteLevel0Table(mem, edit, nullptr);
    }
    mem->Unref();
  }
  return st;
}

// =================================================================
// Compaction
// =================================================================

void DBImpl::CompactMemTable() {
  // mutex_.AssertHeld();
  assert(imm_ != nullptr);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_); // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.store(false, std::memory_order_release);
    RemoveObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::MaybeScheduleCompaction() {
  // mutex_.AssertHeld();
  if (bg_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == nullptr && !versions_->NeedsCompaction()) {
    // No work to be done
  } else {
    auto bg_word = [this]() {
      std::unique_lock<std::mutex> l(mutex_);
      assert(bg_compaction_scheduled_);
      if (shutting_down_.load(std::memory_order_acquire)) {
        // No more background work when shutting down.
      } else if (!bg_error_.ok()) {
        // No more background work after a background error.
      } else {
        BackgroundCompaction();
      }
      bg_compaction_scheduled_ = false;
      // Trigger the next level compaction if needed.
      MaybeScheduleCompaction();
      bg_work_finished_signal_.notify_all();
    };

    bg_compaction_scheduled_ = true;
    pool_.Submit(bg_word);
  }
}

void DBImpl::BackgroundCompaction() {
  if (imm_ != nullptr) {
    CompactMemTable();
    return;
  }

  InternalKey manual_end;
  auto c = versions_->PickCompaction();

  Status st;
  if (c == nullptr) {
    // Nothing to do
  } else if (c->IsTrivialMove()) {
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->RemoveFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest,
                       f->largest);
    st = versions_->LogAndApply(c->edit(), mutex_);
    if (!st.ok()) {
      RecordBackgroundError(st);
    }
    Logger::Log("Moved {} to level-{} {} bytes {}: {}", f->number,
                c->level() + 1, f->file_size, st.ToString(),
                versions_->LevelSummary());
  } else {
    auto compact = std::make_unique<CompactionState>(std::move(c));
    st = DoCompactionWork(compact.get());
    if (!st.ok()) {
      RecordBackgroundError(st);
    }
    for (auto out : compact->outputs) {
      pending_outputs_.erase(out.number);
    }
    RemoveObsoleteFiles();
  }

  if (st.ok()) {
    // Done
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // Ignore compaction errors found during shutting down
  } else {
    Logger::Log("Compaction error: {}", st.ToString());
  }
}

void DBImpl::RemoveObsoleteFiles() {
  // mutex_.AssertHeld();
  if (!bg_error_.ok()) {
    return;
  }
  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  file_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;
  for (std::string& filename : filenames) {
    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        files_to_delete.push_back(std::move(filename));
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Logger::Log("Delete type={}, file number={}", static_cast<int>(type),
                    number);
      }
    }
  }
  mutex_.unlock();
  for (const std::string& filename : files_to_delete) {
    file_->RemoveFile(dbname_ + "/" + filename);
  }
  mutex_.lock();
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  auto input = versions_->MakeInputIterator(compact->compaction.get());

  // Release mutex while we're actually doing the compaction work
  mutex_.unlock();

  input->SeekToFirst();
  Status st;
  InternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
    // Prioritize immutable compaction work
    if (has_imm_.load(std::memory_order_relaxed)) {
      mutex_.lock();
      if (imm_ != nullptr) {
        CompactMemTable();
        // Wake up MakeRoomForWrite() if necessary.
        bg_work_finished_signal_.notify_all();
      }
      mutex_.unlock();
    }

    bool drop = false;
    Slice key = input->key();
    if (!ikey.DecodeFrom(key)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.UserKey(), Slice(current_user_key)) !=
              0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.UserKey().data(), ikey.UserKey().size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true; // (A)
      } else if (ikey.Type() == kTypeDeletion &&
                 ikey.Sequence() <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.UserKey())) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.Sequence();
    }

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == nullptr) {
        st = OpenCompactionOutputFile(compact);
        if (!st.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output().smallest.DecodeFrom(key);
      }
      compact->current_output().largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());
      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        st = FinishCompactionOutputFile(compact, input.get());
        if (!st.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (st.ok() && shutting_down_.load(std::memory_order_acquire)) {
    st = Status::IOError("Deleting DB during compaction");
  }
  if (st.ok() && compact->builder != nullptr) {
    st = FinishCompactionOutputFile(compact, input.get());
  }
  if (st.ok()) {
    st = input->status();
  }

  mutex_.lock();
  if (st.ok()) {
    st = InstallCompactionResults(compact);
  }
  if (!st.ok()) {
    RecordBackgroundError(st);
  }
  Logger::Log("compacted to: {}", versions_->LevelSummary());
  return st;
}

// =================================================================
// Read
// =================================================================

std::unique_ptr<Iterator> DBImpl::NewInternalIterator(
    const ReadOptions& options, SequenceNumber* latest_snapshot) {
  mutex_.lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<std::unique_ptr<Iterator>> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  auto version_cur = versions_->current();
  version_cur->AddIterators(options, &list);
  auto internal_iter =
      NewMergingIterator(&internal_comparator_, std::move(list));
  version_cur->Ref();

  internal_iter->RegisterCleanup([this, version_cur] {
    this->mutex_.lock();
    this->mem_->Unref();
    if (this->imm_) this->imm_->Unref();
    version_cur->Unref();
    this->mutex_.unlock();
  });

  mutex_.unlock();
  return internal_iter;
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  Status s;
  std::unique_ptr<std::mutex> lock(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot) {
    snapshot = options.snapshot->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  // Unlock while reading from files and memtables
  {
    mutex_.unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      s = current->Get(options, lkey, value);
      have_stat_update = true;
    }
    mutex_.lock();
  }

  if (have_stat_update) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != nullptr) imm->Unref();
  current->Unref();
  return s;
}

std::unique_ptr<Iterator> DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  auto iter = NewInternalIterator(options, &latest_snapshot);
  return NewDBIterator(
      this, user_comparator(), std::move(iter),
      (options.snapshot != nullptr ? options.snapshot->sequence_number()
                                   : latest_snapshot));
}

// =================================================================
// Write
// =================================================================

// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      s = bg_error_;
      break;
    } else if (allow_delay && versions_->NumLevelFiles(0) >=
                                  config::kL0_SlowdownWritesTrigger) {
      mutex_.unlock();
      std::this_thread::sleep_for(std::chrono::microseconds(1000));
      allow_delay = false;
      mutex_.lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != nullptr) {
      // The memtable one is still being compacted
      Logger::Log("Current memtable full; waiting...");
      // TODO condition_variable
      std::this_thread::sleep_for(std::chrono::microseconds(1000));
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files
      Logger::Log("Too many L0 files; waiting...");
      std::this_thread::sleep_for(std::chrono::microseconds(1000));
    } else {
      // Attempt to switch to a new memtable and trigger compaction
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      auto lfile = file_->NewWritableFile(LogFileName(dbname_, new_log_number));
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }

      logfile_number_ = new_log_number;
      log_ = std::make_unique<log::Writer>(std::move(lfile));
      imm_ = mem_;
      has_imm_.store(true, std::memory_order_release);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false; // Do not force another compaction if have room
      MaybeScheduleCompaction();
    }
  }
  return s;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  uint64_t file_number;
  {
    mutex_.lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.unlock();
  }
  // Make the output file
  std::string fname = SSTTableFileName(dbname_, file_number);
  compact->outfile = std::move(file_->NewWritableFile(fname));
  compact->builder = std::move(
      std::make_unique<TableBuilder>(options_, std::move(compact->outfile)));
  return Status::OK();
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  const uint64_t output_number = compact->current_output().number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output().file_size = current_bytes;
  compact->total_bytes += current_bytes;
  compact->builder->Flush();

  // Finish and check for file errors
  if (s.ok()) {
    if (current_entries > 0) {
      // Verify that the table is usable
      auto iter = table_cache_->NewIterator(ReadOptions(), output_number,
                                            current_bytes);
      if (iter->status().ok()) {
        Logger::Log("Generated table {}@{}: {} keys, {} bytes ", output_number,
                    compact->compaction->level(), current_entries,
                    current_bytes);
      }
    }
  }
  return s;
}

Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  // mutex_.AssertHeld();
  Logger::Log("Compacted {}@{} + {}@{} files => {} bytes",
              compact->compaction->num_input_files(0),
              compact->compaction->level(),
              compact->compaction->num_input_files(1),
              compact->compaction->level() + 1, compact->total_bytes);

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  auto level = compact->compaction->level();
  for (auto out : compact->outputs) {
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                         out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), mutex_);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  Writer w(&mutex_);
  w.batch = updates;
  w.sync = options.sync;
  w.done = false;

  std::unique_lock<std::mutex> lock(mutex_);
  auto& writers = writers_;
  writers.push_back(&w);
  w.cv.wait(lock, [&w, &writers] { return w.done || &w == writers.front(); });

  if (w.done) {
    return w.st;
  }

  // May temporarily unlock and wait.
  Status st = MakeRoomForWrite(updates == nullptr);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (st.ok() && updates != nullptr) { // nullptr batch is for compactions
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(write_batch);

    // Add to log and apply to memtable
    {
      mutex_.unlock();
      st = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
      bool sync_error = false;
      if (st.ok() && options.sync) {
        st = log_->Sync();
        if (!st.ok()) {
          sync_error = true;
        }
      }
      if (st.ok()) {
        st = WriteBatchInternal::InsertInto(write_batch, mem_);
      }
      mutex_.lock();
      if (sync_error) {
        RecordBackgroundError(st);
      }
    }
    if (write_batch == tmp_batch_) tmp_batch_->Clear();
    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->st = st;
      ready->done = true;
      ready->cv.notify_one();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.notify_one();
  }

  return st;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter; // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }
    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }
      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// =================================================================
// Convenience methods
// =================================================================

const Snapshot* DBImpl::GetSnapshot() {
  std::unique_lock<std::mutex> lock(mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  std::unique_lock<std::mutex> lock(mutex_);
  snapshots_.Delete(snapshot);
}

Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  *dbptr = nullptr;

  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  if (s.ok() && impl->mem_ == nullptr) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    auto lfile =
        options.file->NewWritableFile(LogFileName(dbname, new_log_number));
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_number_ = new_log_number;
      impl->log_ = std::move(std::make_unique<log::Writer>(std::move(lfile)));
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0); // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, impl->mutex_);
  }
  if (s.ok()) {
    impl->RemoveObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.unlock();
  if (s.ok()) {
    assert(impl->mem_);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  File* file = options.file;
  std::vector<std::string> filenames;
  Status result = file->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type) &&
        type != kDBLockFile) { // Lock file will be deleted at end
      Status del = file->RemoveFile(dbname + "/" + filenames[i]);
      if (result.ok() && !del.ok()) {
        result = del;
      }
    }
  }
  file->RemoveDir(dbname); // Ignore error in case dir contains other files
  return result;
}

} // namespace tinydb
