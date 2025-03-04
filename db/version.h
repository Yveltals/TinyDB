#pragma once
#include "common/iterator.h"
#include "common/options.h"
#include "db/version_edit.h"
#include "iterator/iterator_base.h"

namespace tinydb {

class VersionSet;

// Callback from TableCache::Get()
enum GetState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};

// Return the smallest i that files[i]->largest >= key.
// REQUIRES: "files" is a sorted list of non-overlapping files.
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key);

int64_t TotalFileSize(const std::vector<FileMetaData*>& files);
uint64_t MaxFileSizeForLevel(const Options* options, int level);
int64_t MaxGrandParentOverlapBytes(const Options* options);
int64_t ExpandedCompactionByteSizeLimit(const Options* options);

class Version {
 public:
  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  void AddIterators(const ReadOptions&,
                    std::vector<std::unique_ptr<Iterator>>* iters);

  // Lookup the value for key.
  // REQUIRES: lock is not held
  Status Get(const ReadOptions&, const LookupKey& key, std::string* val);

  void Ref();
  void Unref();

  void GetOverlappingInputs(
      int level,
      const InternalKey* begin, // nullptr means before all keys
      const InternalKey* end,   // nullptr means after all keys
      std::vector<FileMetaData*>* inputs);

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==nullptr represents a key smaller than all the DB's keys.
  // largest_user_key==nullptr represents a key largest than all the DB's keys.
  bool OverlapInLevel(int level, const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].
  int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);

  int NumFiles(int level) const { return files_[level].size(); }
  std::string DebugString() const;

 private:
  friend class Compaction;
  friend class VersionSet;

  explicit Version(VersionSet* vset)
      : vset_(vset), refs_(0), compaction_score_(-1), compaction_level_(-1) {}
  Version(const Version&) = delete;
  Version& operator=(const Version&) = delete;
  ~Version();

  // Call func(arg, level, f) for every file that overlaps user_key.
  // If an invocation of func returns false, makes no more calls.
  void ForEachOverlapping(Slice internal_key,
                          std::function<bool(FileMetaData*)>);

  VersionSet* vset_; // VersionSet to which this Version belongs
  int refs_;
  // List of files per level
  std::vector<FileMetaData*> files_[config::kNumLevels];

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.
  double compaction_score_;
  int compaction_level_;
};

} // namespace tinydb
