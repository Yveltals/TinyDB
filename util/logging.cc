#include "util/logging.h"


#include "common/slice.h"

namespace tinydb {

void AppendNumberTo(std::string* str, uint64_t num) {
  char buf[30];
  std::snprintf(buf, sizeof(buf), "%llu", static_cast<unsigned long long>(num));
  str->append(buf);
}

void AppendEscapedStringTo(std::string* str, const Slice& value) {
  for (size_t i = 0; i < value.size(); i++) {
    char c = value[i];
    if (c >= ' ' && c <= '~') {
      str->push_back(c);
    } else {
      char buf[10];
      std::snprintf(buf, sizeof(buf), "\\x%02x",
                    static_cast<unsigned int>(c) & 0xff);
      str->append(buf);
    }
  }
}

std::string NumberToString(uint64_t num) {
  std::string r;
  AppendNumberTo(&r, num);
  return r;
}

std::string EscapeString(const Slice& value) {
  std::string r;
  AppendEscapedStringTo(&r, value);
  return r;
}

bool ConsumeDecimalNumber(Slice* in, uint64_t* val) {
  // Constants that will be optimized away.
  constexpr const char kLastDigitOfMaxUint64 =
      '0' + static_cast<char>(UINT64_MAX % 10);

  uint64_t value = 0;
  const uint8_t* start = reinterpret_cast<const uint8_t*>(in->data());
  const uint8_t* end = start + in->size();
  const uint8_t* current = start;
  for (; current != end; ++current) {
    const uint8_t ch = *current;
    if (ch < '0' || ch > '9') break;
    // Overflow check.
    if (value > UINT64_MAX / 10 ||
        (value == UINT64_MAX / 10 && ch > kLastDigitOfMaxUint64)) {
      return false;
    }
    value = (value * 10) + (ch - '0');
  }
  *val = value;
  const size_t digits_consumed = current - start;
  in->remove_prefix(digits_consumed);
  return digits_consumed != 0;
}

} // namespace tinydb
