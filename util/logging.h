#pragma once

#include <cstdint>
#include <cstdio>
#include <string>

namespace tinydb {

class Slice;

void AppendNumberTo(std::string* str, uint64_t num);

void AppendEscapedStringTo(std::string* str, const Slice& value);

std::string NumberToString(uint64_t num);

std::string EscapeString(const Slice& value);

bool ConsumeDecimalNumber(Slice* in, uint64_t* val);

} // namespace tinydb
