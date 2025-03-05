#pragma once
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/filter_policy.h"
#include "common/slice.h"
#include "util/hash.h"

namespace tinydb {

class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);
  FilterBlockBuilder(const FilterBlockBuilder&) = delete;
  FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);
  Slice Finish();

 private:
  void GenerateFilter();

  const FilterPolicy* policy_;
  std::string keys_;            // Flattened key contents
  std::vector<size_t> start_;   // Starting index in keys_ of each key
  std::string result_;          // Filter data computed so far
  std::vector<Slice> tmp_keys_; // policy_->CreateFilter() argument
  std::vector<uint32_t> filter_offsets_;
};

class FilterBlockReader {
 public:
  // Data in BlockContents will move to FilterBlockReader
  FilterBlockReader(const FilterPolicy* policy, std::unique_ptr<char[]> data,
                    size_t size);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

 private:
  const FilterPolicy* policy_;
  std::unique_ptr<char[]> data_;
  const char* offset_; // Pointer to beginning of offset array (at block-end)
  size_t num_;         // Number of entries in offset array
  size_t base_lg_;     // Encoding parameter (kFilterBaseLg)
};

} // namespace tinydb
