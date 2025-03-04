#pragma once
#include <string>

#include "common/slice.h"

namespace tinydb {

class Comparator {
 public:
  virtual ~Comparator();

  virtual int Compare(const Slice& a, const Slice& b) const = 0;

  virtual const char* Name() const = 0;
};

} // namespace tinydb