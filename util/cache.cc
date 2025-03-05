#include "util/cache.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <unordered_map>

#include "util/hash.h"

namespace tinydb {

namespace {

struct LRUHandle {
  std::any value;
  delete_handler deleter;
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
  size_t charge;
  size_t key_length;
  bool in_cache; // whether entry is in the cache
  uint32_t refs;
  char key_data[1]; // beginning of key

  Slice key() const {
    assert(next != this); // empty list
    return Slice(key_data, key_length);
  }
};

struct LRUHandleHash {
  std::size_t operator()(const Slice s) const {
    return Hash(s.data(), s.size(), 0);
  }
};

struct LRUHandleEqual {
  bool operator()(const Slice a, const Slice b) const { return a == b; }
};

class LRUCache {
 public:
  LRUCache() : capacity_(0), usage_(0) {
    lru_.next = &lru_;
    lru_.prev = &lru_;
    in_use_.next = &in_use_;
    in_use_.prev = &in_use_;
  }
  ~LRUCache() {
    assert(in_use_.next == &in_use_);
    for (auto e = lru_.next; e != &lru_;) {
      auto next = e->next;
      assert(e->in_cache);
      e->in_cache = false;
      assert(e->refs == 1);
      Unref(e);
      e = next;
    }
  }

  void SetCapacity(size_t capacity) { capacity_ = capacity; }
  Cache::Handle* Insert(const Slice& key, std::any value, size_t charge,
                        delete_handler deleter);
  Cache::Handle* Lookup(const Slice& key);
  void Release(Cache::Handle* e);
  void Erase(const Slice& key);
  size_t TotalCharge() {
    std::unique_lock<std::mutex> l(mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* list, LRUHandle* e);
  void Ref(LRUHandle* e);
  void Unref(LRUHandle* e);
  void FinishErase(LRUHandle* e);

  size_t capacity_;
  std::mutex mutex_;
  size_t usage_;

  // Dummy head of LRU list.
  // Lru.prev is newest entry, lru.next is oldest entry.
  // Entries have refs==1 and in_cache==true.
  LRUHandle lru_;

  // Dummy head of in-use list
  // Entries are in use by clients, and have refs >= 2 and in_cache == true.
  LRUHandle in_use_;

  std::unordered_map<Slice, LRUHandle*, LRUHandleHash, LRUHandleEqual> table_;
};

void LRUCache::Ref(LRUHandle* e) {
  if (e->refs == 1 && e->in_cache) { // if on lru_, move to in_use_ list
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) { // Deallocate
    assert(!e->in_cache);
    e->deleter(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {
    // no longer in use, move to lru_ list
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  // make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const Slice& key) {
  std::unique_lock<std::mutex> l(mutex_);
  auto e = table_.find(key);
  if (e != table_.end()) {
    Ref(e->second);
  }
  return reinterpret_cast<Cache::Handle*>(e->second);
}

void LRUCache::Release(Cache::Handle* handle) {
  std::unique_lock<std::mutex> l(mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::Insert(const Slice& key, std::any value, size_t charge,
                                delete_handler deleter) {
  std::unique_lock<std::mutex> l(mutex_);
  auto e =
      reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));
  e->value = value;
  e->deleter = std::move(deleter);
  e->charge = charge;
  e->key_length = key.size();
  e->in_cache = false;
  e->refs = 1; // for the returned handle
  std::memcpy(e->key_data, key.data(), key.size());

  if (capacity_ > 0) {
    e->refs++;
    e->in_cache = true;
    LRU_Append(&in_use_, e);
    usage_ += charge;
    table_[key] = e;
  } else {
    e->next = nullptr;
  }
  while (usage_ > capacity_ && lru_.next != &lru_) {
    auto old = lru_.next;
    assert(old->refs == 1);
    FinishErase(old);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

// Finish removing *e from the cache.
// It has already been removed from the hash table
void LRUCache::FinishErase(LRUHandle* e) {
  if (e != nullptr) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
}

void LRUCache::Erase(const Slice& key) {
  std::unique_lock<std::mutex> l(mutex_);
  auto it = table_.find(key);
  if (it != table_.end()) {
    auto e = it->second;
    table_.erase(key);
    FinishErase(e);
  }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_[kNumShards];
  std::mutex mutex_;
  uint64_t last_id_;

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

 public:
  explicit ShardedLRUCache(size_t capacity) : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  ~ShardedLRUCache() override {}
  Handle* Insert(const Slice& key, std::any value, size_t charge,
                 delete_handler deleter) override {
    auto hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, value, charge, deleter);
  }
  Handle* Lookup(const Slice& key) override {
    auto hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key);
  }
  void Release(Handle* handle) override {
    auto h = reinterpret_cast<LRUHandle*>(handle);
    auto hash = HashSlice(h->key());
    shard_[Shard(hash)].Release(handle);
  }
  void Erase(const Slice& key) override {
    auto hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key);
  }
  std::any Value(Handle* handle) override {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  uint64_t NewId() override {
    std::unique_lock<std::mutex> l(mutex_);
    return ++(last_id_);
  }
  size_t TotalCharge() override {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};

} //  namespace

Cache* NewLRUCache(size_t capacity) { return new ShardedLRUCache(capacity); }

} // namespace tinydb
