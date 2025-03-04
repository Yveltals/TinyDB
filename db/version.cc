#include "db/version.h"

#include "db/table_cache.h"
#include "db/version_set.h"
#include "iterator/iterator_level_files.h"
#include "iterator/iterator_two_level.h"

namespace tinydb {

static size_t TargetFileSize(const Options* options) {
  return options->max_file_size;
}

int64_t MaxGrandParentOverlapBytes(const Options* options) {
  return 10 * TargetFileSize(options);
}

int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
  return 25 * TargetFileSize(options);
}

static double MaxBytesForLevel(const Options* options, int level) {
  double result = 10. * 1048576.0;
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}

uint64_t MaxFileSizeForLevel(const Options* options, int level) {
  return TargetFileSize(options);
}

int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (icmp.InternalKeyComparator::Compare(f->largest.Data(), key) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return right;
}

Version::~Version() {
  assert(refs_ == 0);
  // Drop references to files
  for (auto& level_files : files_) {
    for (auto file : level_files) {
      assert(file->refs > 0);
      file->refs--;
      if (file->refs <= 0) {
        delete file;
      }
    }
  }
}

void Version::AddIterators(const ReadOptions& options,
                           std::vector<std::unique_ptr<Iterator>>* iters) {
  const auto table_cache = vset_->table_cache_;
  // Merge all level zero files together since they may overlap
  for (auto f : files_[0]) {
    iters->push_back(
        table_cache->NewIterator(options, f->number, f->file_size));
  }
  // For levels > 0, use a concatenating iterator that sequentially walks
  // through the non-overlapping files in the level, opening them lazily
  for (int level = 1; level < config::kNumLevels; level++) {
    if (!files_[level].empty()) {
      // Iterator of level-files
      auto file_iter =
          std::make_unique<IteratorLevelFiles>(vset_->icmp_, &files_[level]);

      // Callback to build data_block iterator, return Table::Iterator
      auto table_iterator = [&table_cache](const ReadOptions& options,
                                           const Slice& file_value) {
        if (file_value.size() != 16) {
          return NewErrorIterator(
              Status::Corruption("FileReader invoked with unexpected value"));
        } else {
          return table_cache->NewIterator(options,
                                          DecodeFixed64(file_value.data()),
                                          DecodeFixed64(file_value.data() + 8));
        }
      };

      iters->push_back(
          NewTwoLevelIterator(std::move(file_iter), table_iterator, options));
    }
  }
}

void Version::ForEachOverlapping(Slice internal_key,
                                 std::function<bool(FileMetaData*)> func) {
  Slice user_key = ExtractUserKey(internal_key);
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  // Search level-0 in order from newest to oldest.
  std::vector<FileMetaData*> tmp;
  tmp.reserve(files_[0].size());
  for (auto file : files_[0]) {
    if (ucmp->Compare(user_key, file->smallest.UserKey()) >= 0 &&
        ucmp->Compare(user_key, file->largest.UserKey()) <= 0) {
      tmp.push_back(file);
    }
  }
  if (!tmp.empty()) {
    std::sort(tmp.begin(), tmp.end(), [](FileMetaData* a, FileMetaData* b) {
      return a->number > b->number;
    });
    for (auto file : tmp) {
      if (!func(file)) {
        return;
      }
    }
  }

  // Search other levels.
  for (int level = 1; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;
    // Binary search to find earliest index whose largest key >= internal_key.
    uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
    if (index < num_files) {
      FileMetaData* file = files_[level][index];
      if (ucmp->Compare(user_key, file->smallest.UserKey()) < 0) {
        // All of "f" is past any data for user_key
      } else {
        if (!func(file)) {
          return;
        }
      }
    }
  }
}

Status Version::Get(const ReadOptions& options, const LookupKey& key,
                    std::string* value) {
  Status s;
  auto ucmp = vset_->icmp_.user_comparator();
  auto state = kNotFound;
  auto found = false;
  auto ikey = key.internal_key();
  auto user_key = key.user_key();

  // Seek key in SSTables
  auto match = [&](FileMetaData* file) {
    // Save value to *value
    auto save_value = [&state, ucmp, user_key, value](const Slice& ikey,
                                                      const Slice& v) {
      InternalKey key;
      if (!key.DecodeFrom(ikey)) {
        state = kCorrupt;
        return;
      }
      if (ucmp->Compare(key.UserKey(), user_key) == 0) {
        state = (key.Type() == kTypeValue) ? kFound : kDeleted;
        if (state == kFound) {
          value->assign(v.data(), v.size());
        }
      }
    };

    s = vset_->table_cache_->Get(options, file->number, file->file_size, ikey,
                                 save_value);
    if (!s.ok()) {
      found = true;
      return false;
    }
    switch (state) {
      case kNotFound:
        return true; // Keep searching in other files
      case kFound:
        found = true;
        return false;
      case kDeleted:
        return false;
      case kCorrupt:
        s = Status::Corruption("corrupted key for ", user_key);
        found = true;
        return false;
    }
    return false;
  };
  ForEachOverlapping(ikey, match);
  return found ? s : Status::NotFound(Slice());
}

void Version::Ref() { ++refs_; }

void Version::Unref() {
  assert(refs_ >= 1);
  if (--refs_ == 0) {
    delete this;
  }
}

bool Version::OverlapInLevel(int level, const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                               smallest_user_key, largest_user_key);
}

int Version::PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                        const Slice& largest_user_key) {
  int level = 0;
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;
    while (level < config::kMaxMemCompactLevel) {
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        break;
      }
      if (level + 2 < config::kNumLevels) {
        // Check that file does not overlap too many grandparent bytes.
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
        const int64_t sum = TotalFileSize(overlaps);
        if (sum > MaxGrandParentOverlapBytes(vset_->options_)) {
          break;
        }
      }
      level++;
    }
  }
  return level;
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
void Version::GetOverlappingInputs(int level, const InternalKey* begin,
                                   const InternalKey* end,
                                   std::vector<FileMetaData*>* inputs) {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  inputs->clear();
  Slice user_begin, user_end;
  if (begin) user_begin = begin->UserKey();
  if (end) user_end = end->UserKey();
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  for (size_t i = 0; i < files_[level].size();) {
    FileMetaData* f = files_[level][i++];
    const Slice file_start = f->smallest.UserKey();
    const Slice file_limit = f->largest.UserKey();
    if (begin && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range
    } else if (end && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range
    } else {
      inputs->push_back(f);
      if (level == 0) {
        // Level-0 files may overlap each other
        if (begin && user_cmp->Compare(file_start, user_begin) < 0) {
          user_begin = file_start;
          inputs->clear();
          i = 0;
        } else if (end && user_cmp->Compare(file_limit, user_end) > 0) {
          user_end = file_limit;
          inputs->clear();
          i = 0;
        }
      }
    }
  }
}

std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    for (auto file : files_[level]) {
      r.push_back(' ');
      AppendNumberTo(&r, file->number);
      r.push_back(':');
      AppendNumberTo(&r, file->file_size);
      r.append("[");
      r.append(file->smallest.DebugString());
      r.append(" .. ");
      r.append(file->largest.DebugString());
      r.append("]\n");
    }
  }
  return r;
}

} // namespace tinydb
