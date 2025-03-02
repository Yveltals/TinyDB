#include "db/version_set.h"
#include <algorithm>
#include <cstdio>
#include <iostream>
#include "log/log_reader.h"
#include "log/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version.h"
#include "table/table_builder.h"
#include "util/coding.h"
#include "util/file.h"
#include "util/filename.h"
#include "log/logging.h"

namespace tinydb {

static bool AfterFile(const Comparator* ucmp, const Slice* user_key,
                      const FileMetaData* f) {
  return (user_key && ucmp->Compare(*user_key, f->largest.UserKey()) > 0);
}

static bool BeforeFile(const Comparator* ucmp, const Slice* user_key,
                       const FileMetaData* f) {
  return (user_key && ucmp->Compare(*user_key, f->smallest.UserKey()) < 0);
}

bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  if (!disjoint_sorted_files) {
    // Need to check against all files
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small_key(*smallest_user_key, kMaxSequenceNumber,
                          kValueTypeForSeek);
    index = FindFile(icmp, files, small_key.Data());
  }
  if (index >= files.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }
  return !BeforeFile(ucmp, largest_user_key, files[index]);
}


// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;
    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      return r ? r < 0 : (f1->number < f2->number);
    }
  };

  using FileSet = std::set<FileMetaData*, BySmallestKey>;
  // Files in levels
  struct LevelState {
    std::set<uint64_t> deleted_files;
    FileSet* added_files;
  };

  VersionSet* vset_;
  Version* base_;
  LevelState levels_[config::kNumLevels];

 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base) : vset_(vset), base_(base) {
    base_->Ref();
    BySmallestKey cmp{&vset_->icmp_};
    for (int level = 0; level < config::kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp);
    }
  }

  ~Builder() {
    for (int level = 0; level < config::kNumLevels; level++) {
      auto added = levels_[level].added_files;
      for (auto f : *added) {
        f->refs--;
        if (f->refs <= 0) {
          delete f;
        }
      }
      delete added;
    }
    base_->Unref();
  }

  // Apply edit to the current state
  void Apply(const VersionEdit* edit) {
    // Delete files
    for (auto& [level, number] : edit->deleted_files_) {
      levels_[level].deleted_files.insert(number);
    }
    // Add new files
    for (auto& [level, file_meta] : edit->new_files_) {
      FileMetaData* f = new FileMetaData(file_meta);
      f->refs = 1;
      levels_[level].deleted_files.erase(f->number);
      levels_[level].added_files->insert(f);
    }
  }

  // Save the current state in *v
  void SaveTo(Version* v) {
    BySmallestKey cmp{&vset_->icmp_};
    for (int level = 0; level < config::kNumLevels; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const std::vector<FileMetaData*>& base_files = base_->files_[level];
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
      const FileSet* added_files = levels_[level].added_files;
      v->files_[level].reserve(base_files.size() + added_files->size());
      for (const auto& added_file : *added_files) {
        // Add all smaller files listed in base_
        for (std::vector<FileMetaData*>::const_iterator bpos =
                 std::upper_bound(base_iter, base_end, added_file, cmp);
             base_iter != bpos; ++base_iter) {
          MaybeAddFile(v, level, *base_iter);
        }

        MaybeAddFile(v, level, added_file);
      }

      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }
    }
  }

  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
    } else {
      auto files = &v->files_[level];
      if (level > 0 && !files->empty()) {
        // Must not overlap
        assert(vset_->icmp_.Compare(files->back()->largest, f->smallest) < 0);
      }
      f->refs++;
      files->push_back(f);
    }
  }
};

VersionSet::VersionSet(const std::string& dbname, const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->file),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_log_(nullptr),
      current_(nullptr) {
  AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
  current_->Unref();
}

void VersionSet::AppendVersion(Version* v) {
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_) {
    current_->Unref();
  }
  versions_.push_back(v);
  current_ = v;
  v->Ref();
}

Status VersionSet::LogAndApply(VersionEdit* edit, std::mutex& mu) {
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }

  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);

  Version* v = new Version(this);
  {
    Builder builder(this, current_);
    builder.Apply(edit);
    builder.SaveTo(v);
  }
  Finalize(v);  // TODO impl

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  std::string new_manifest_file;
  Status s;
  if (descriptor_log_ == nullptr) {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    std::unique_ptr<WritableFile> file(env_->NewWritableFile(new_manifest_file));
    descriptor_log_ = std::make_unique<log::Writer>(std::move(file));
    s = WriteSnapshot(descriptor_log_.get());
  }

  // Unlock during expensive MANIFEST log write
  {
    mu.unlock();
    // Write new record to MANIFEST log
    std::string record;
    edit->EncodeTo(&record);
    s = descriptor_log_->AddRecord(record);
    if (s.ok()) {
      s = descriptor_log_->Sync();
      // Set to CURRENT
      if (!new_manifest_file.empty()) {
        s = SetCurrentFile(env_, dbname_, manifest_file_number_);
      }
    } else {
      std::cout << "MANIFEST write: %s\n" << s.ToString() << std::endl;
    }
    mu.lock();
  }

  // Install the new version
  if (s.ok()) {
    AppendVersion(v);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
  } else {
    delete v;
    if (!new_manifest_file.empty()) {
      descriptor_log_.reset();
      env_->RemoveFile(new_manifest_file);
    }
  }

  return s;
}

Status VersionSet::Recover(bool* save_manifest) {
  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current.back() != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  std::string dscname = dbname_ + "/" + current;
  std::unique_ptr<SequentialFile> file(env_->NewSequentialFile(dscname));
  // TODO: check not found file

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_);
  int read_records = 0;

  {
    log::Reader reader(std::move(file));
    Slice record;
    while (reader.ReadRecord(&record) && s.ok()) {
      ++read_records;
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
        builder.Apply(&edit);
      }
      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }
      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }
      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }
      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }
  }

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }
    if (!have_prev_log_number) {
      prev_log_number = 0;
    }
    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    Version* v = new Version(this);
    builder.SaveTo(v);
    // Install recovered version
    Finalize(v);
    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;
    *save_manifest = true;
  } else {
    std::cout << "Error recovering version set with" << read_records
              << "records:" << s.ToString();
  }
  return s;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // Save metadata
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save files
  for (int level = 0; level < config::kNumLevels; level++) {
    auto files = current_->files_[level];
    for (auto file_meta : files) {
      edit.AddFile(level, file_meta->number, file_meta->file_size,
                   file_meta->smallest, file_meta->largest);
    }
  }

  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

std::string VersionSet::LevelSummary() const {
  std::string info = "files[ ";
  for (auto& files_vec : current_->files_) {
    info += std::to_string(files_vec.size()) + " ";
  }
  return info + "]";
}

uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    auto level_files = v->files_[level];
    for (auto file : level_files) {
      if (icmp_.Compare(file->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += file->file_size;
      } else if (icmp_.Compare(file->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for "ikey".
          break;
        }
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        auto iter(table_cache_->NewIterator(ReadOptions(), file->number,
                                            file->file_size, &tableptr));
        if (tableptr) {
          result += tableptr->ApproximateOffsetOf(ikey.Data());
        }
      }
    }
  }
  return result;
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  for (auto v : versions_) {
    for (auto& level_files : v->files_) {
      for (auto file : level_files) {
        live->insert(file->number);
      }
    }
  }
}

int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) {
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      current_->GetOverlappingInputs(level + 1, &f->smallest, &f->largest,
                                     &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest, InternalKey* largest) {
  assert(!inputs.empty());
  *smallest = inputs[0]->smallest;
  *largest = inputs[0]->largest;
  for (auto file : inputs) {
    if (icmp_.Compare(file->smallest, *smallest) < 0) {
      *smallest = file->smallest;
    }
    if (icmp_.Compare(file->largest, *largest) > 0) {
      *largest = file->largest;
    }
  }
}

void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest, InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

bool FindLargestKey(const InternalKeyComparator& icmp,
                    const std::vector<FileMetaData*>& files,
                    InternalKey* largest_key) {
  if (files.empty()) {
    return false;
  }
  *largest_key = files[0]->largest;
  for (auto file : files) {
    if (icmp.Compare(file->largest, *largest_key) > 0) {
      *largest_key = file->largest;
    }
  }
  return true;
}

// Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
// user_key(l2) = user_key(u1)
FileMetaData* FindSmallestBoundaryFile(
    const InternalKeyComparator& icmp,
    const std::vector<FileMetaData*>& level_files,
    const InternalKey& largest_key) {
  const Comparator* user_cmp = icmp.user_comparator();
  FileMetaData* smallest_boundary_file = nullptr;
  for (auto f : level_files) {
    if (icmp.Compare(f->smallest, largest_key) > 0 &&
        user_cmp->Compare(f->smallest.UserKey(), largest_key.UserKey()) ==
            0) {
      if (smallest_boundary_file == nullptr ||
          icmp.Compare(f->smallest, smallest_boundary_file->smallest) < 0) {
        smallest_boundary_file = f;
      }
    }
  }
  return smallest_boundary_file;
}

}  // namespace tinydb
