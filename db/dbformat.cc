#include "db/dbformat.h"
#include <cstdio>
#include <sstream>
#include "util/coding.h"

namespace tinydb {

std::string InternalKey::DebugString() const {
  std::ostringstream ss;
  ss << '\'' << EscapeString(user_key_.ToString()) << "' @ " << sequence_
     << " : " << static_cast<int>(type_);
  return ss.str();
}

bool InternalKey::DecodeFrom(const Slice& s) {
  const size_t n = s.size();
  if (n < 8) return false;
  uint64_t num = DecodeFixed64(s.data() + n - 8);
  uint8_t c = num & 0xff;
  sequence_ = num >> 8;
  type_ = static_cast<ValueType>(c);
  user_key_ = Slice(s.data(), n - 8);
  data_.assign(s.data(), s.size());
  return (c <= static_cast<uint8_t>(kTypeValue));
}

int InternalKeyComparator::Compare(const Slice& akey, const Slice& bkey) const {
  int r = user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
  if (r == 0) {
    const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
    const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
    if (anum > bnum) {
      r = -1;
    } else if (anum < bnum) {
      r = +1;
    }
  }
  return r;
}

void InternalFilterPolicy::CreateFilter(const Slice* keys, int n,
                                        std::string* dst) const {
  Slice* mkey = const_cast<Slice*>(keys);
  for (int i = 0; i < n; i++) {
    mkey[i] = ExtractUserKey(keys[i]);
  }
  user_policy_->CreateFilter(keys, n, dst);
}

bool InternalFilterPolicy::KeyMayMatch(const Slice& key, const Slice& f) const {
  return user_policy_->KeyMayMatch(ExtractUserKey(key), f);
}

LookupKey::LookupKey(const Slice& user_key, SequenceNumber s) {
  size_t usize = user_key.size();
  size_t needed = usize + 13;  // A conservative estimate
  char* dst = new char[needed];
  start_ = dst;
  dst = EncodeVarint32(dst, usize + 8);
  kstart_ = dst;
  std::memcpy(dst, user_key.data(), usize);
  dst += usize;
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
  dst += 8;
  end_ = dst;
}

} // namespace tinydb
