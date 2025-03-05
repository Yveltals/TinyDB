#pragma once

#include <any>
#include <functional>

#include "common/slice.h"

namespace tinydb {

using delete_handler = std::function<void(const Slice& key, std::any value)>;

class Cache {
 public:
  Cache() = default;
  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;

  // Destroys all existing entries by calling the "deleter"
  // function that was passed to the constructor.
  virtual ~Cache() = default;

  // Opaque handle to an entry stored in the cache
  struct Handle {};

  // Insert a mapping from key->value into the cache.
  // Returns a handle that corresponds to the mapping.
  // The caller must call this->Release(handle) when no longer needed
  virtual Handle* Insert(const Slice& key, std::any value, size_t charge,
                         delete_handler deleter) = 0;

  // Return a handle that corresponds to the mapping.
  // The caller must call this->Release(handle) when no longer needed
  virtual Handle* Lookup(const Slice& key) = 0;

  // Release a mapping returned by a previous Lookup()
  virtual void Release(Handle* handle) = 0;

  // Return the value encapsulated in a handle returned by Lookup()
  virtual std::any Value(Handle* handle) = 0;

  // If the cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  virtual void Erase(const Slice& key) = 0;

  // The client will allocate a new id at startup and
  // prepend the id to its cache keys.
  virtual uint64_t NewId() = 0;

  // Return an estimate of the combined charges of all elements stored in the
  // cache.
  virtual size_t TotalCharge() = 0;
};

Cache* NewLRUCache(size_t capacity);

} // namespace tinydb
