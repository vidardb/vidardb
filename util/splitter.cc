//  Copyright (c) 2020-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <string>
#include <vector>
#include "string_util.h"
#include "coding.h"
#include "vidardb/splitter.h"

namespace vidardb {

std::vector<std::string> PipeSplitter::Split(const std::string& s) const {
  return StringSplit(s, delim);
}

std::string PipeSplitter::Stitch(const std::vector<std::string>& v) const {
  return StringStitch(v, delim);
}

void PipeSplitter::Append(std::string& ss, const Slice& s, bool last) const {
  ss.append(s.data_, s.size_);
  if (!last) {
    ss.append(1, delim);
  }
}

std::vector<std::string> EncodingSplitter::Split(const std::string& s) const {
  std::vector<std::string> result;
  for (auto i = 0u; i < s.size();) {
    assert(i + 4 <= s.size());
    uint32_t length = DecodeFixed32(s.data() + i);
    assert(i + 4 + length <= s.size());
    result.emplace_back(s.substr(i + 4, length));
    i += 4 + length;
  }
  return result;
}

std::string EncodingSplitter::Stitch(const std::vector<std::string>& v) const {
  std::string result;
  for (const auto& s: v) {
    Append(result, s, false);
  }
  return result;
}

void EncodingSplitter::Append(std::string& ss, const Slice& s, bool last) const {
  std::string length;
  PutFixed32(&length, s.size());
  ss.append(length);
  ss.append(s.data_, s.size_);
}

}  // namespace vidardb
