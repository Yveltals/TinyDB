#include "table/block.h"

#include <algorithm>
#include <cstdint>
#include <vector>

#include "common/comparator.h"
#include "iterator/iterator_block.h"
#include "log/logging.h"
#include "util/coding.h"

namespace tinydb {

std::unique_ptr<Iterator> Block::NewIterator(const Comparator* comparator) {
  assert(size_);
  return std::make_unique<IteratorBlock>(comparator, data_, size_);
}

} // namespace tinydb
