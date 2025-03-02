#pragma once
#include <string>
#include "common/slice.h"

namespace tinydb {

class FilterPolicy {
 public:
  virtual ~FilterPolicy();

  virtual const char* Name() const = 0;
  virtual void CreateFilter(const Slice* keys, int n,
                            std::string* dst) const = 0;
  virtual bool KeyMayMatch(const Slice& key, const Slice& filter) const = 0;
};

// Callers must delete the result after any database that is using the
// result has been closed.
const FilterPolicy* NewBloomFilterPolicy(int bits_per_key);

}  // namespace tinydb