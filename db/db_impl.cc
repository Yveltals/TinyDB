#include "db/db_impl.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "db/db.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "log/log_reader.h"
#include "log/log_writer.h"
#include "log/logging.h"
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

  explicit CompactionState(Compaction* c)
      : compaction(c),
        smallest_snapshot(0),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {}

  Compaction* const compaction;
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
      seed_(0),
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

void DBImpl::RecordBackgroundError(const Status& s) {
  if (bg_error_.ok()) {
    bg_error_ = s;
    bg_work_finished_signal_.notify_all();
  }
}

void DBImpl::MaybeScheduleCompaction() {
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

  Compaction* c;
  InternalKey manual_end;
  c = versions_->PickCompaction();

  Status status;
  if (c == nullptr) {
    // Nothing to do
  } else if (c->IsTrivialMove()) {
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->RemoveFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest,
                       f->largest);
    status = versions_->LogAndApply(c->edit(), mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    // Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
    //     static_cast<unsigned long long>(f->number), c->level() + 1,
    //     static_cast<unsigned long long>(f->file_size),
    //     status.ToString().c_str(), versions_->LevelSummary());
  } else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();
    RemoveObsoleteFiles();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // Ignore compaction errors found during shutting down
  } else {
    std::cout << "Compaction error: " << status.ToString() << std::endl;
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
        std::cout << "Delete type=" << static_cast<int>(type) << " #" << number
                  << std::endl;
      }
    }
  }

  // While deleting all files unblock other threads. All files being deleted
  // have unique names which will not collide with newly created files and
  // are therefore safe to delete while allowing other threads to proceed.
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

  auto input = versions_->MakeInputIterator(compact->compaction);

  // Release mutex while we're actually doing the compaction work
  mutex_.unlock();

  input->SeekToFirst();
  Status status;
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

    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      status = FinishCompactionOutputFile(compact, input.get());
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
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
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input.get());
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input.get());
  }
  if (status.ok()) {
    status = input->status();
  }

  mutex_.lock();
  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  std::cout << "compacted to: " << versions_->LevelSummary();
  return status;
}

// REQUIRES: mutex_ is held
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
      std::cout << "Current memtable full; waiting..." << std::endl;
      // TODO condition_variable
      std::this_thread::sleep_for(std::chrono::microseconds(1000));
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files
      std::cout << "Too many L0 files; waiting..." << std::endl;
      std::this_thread::sleep_for(std::chrono::microseconds(1000));
    } else {
      // Attempt to switch to a new memtable and trigger compaction
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      std::unique_ptr<WritableFile> lfile(
          file_->NewWritableFile(LogFileName(dbname_, new_log_number)));
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

void DBImpl::CleanupCompaction(CompactionState* compact) {
  // mutex_.AssertHeld();
  if (compact->builder) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
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
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    auto iter =
        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    s = iter->status();
    if (s.ok()) {
      // Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld
      // bytes",
      //     (unsigned long long)output_number, compact->compaction->level(),
      //     (unsigned long long)current_entries,
      //     (unsigned long long)current_bytes);
    }
  }
  return s;
}

Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  // mutex_.AssertHeld();
  // Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
  //     compact->compaction->num_input_files(0), compact->compaction->level(),
  //     compact->compaction->num_input_files(1), compact->compaction->level() +
  //     1, static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                         out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), mutex_);
}

} // namespace tinydb
