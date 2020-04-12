//  Copyright (c) 2020-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "vidardb/splitter.h"

#include <string.h>

#include <string>
#include <vector>

#include "coding.h"
#include "vidardb/slice.h"

namespace vidardb {

std::vector<Slice> PipeSplitter::Split(const Slice& s) const {
  const char* p = s.data();
  std::vector<Slice> res;
  for (auto i = 0u, j = 0u; i < s.size();) {
    if (s[i] != delim && i + 1 < s.size()) {
      i++;
      continue;
    }

    size_t n = i - j;
    if (i + 1 == s.size()) {
      n = i + 1 - j;  // last
    }

    res.emplace_back(Slice(p + j, n));
    j = ++i;
  }
  return res;
}

std::string PipeSplitter::Stitch(const std::vector<Slice>& v) const {
  std::string res;  // RVO/NRVO/move
  for (auto i = 0u; i < v.size(); i++) {
    Append(res, v[i], i + 1 == v.size());
  }
  return res;
}

Slice PipeSplitter::Stitch(const std::vector<Slice>& v,
                           std::string& buf) const {
  for (auto i = 0u; i < v.size(); i++) {
    Append(buf, v[i], i + 1 == v.size());
  }
  return buf;
}

void PipeSplitter::Append(std::string& ss, const Slice& s, bool last) const {
  ss.append(s.data_, s.size_);
  if (!last) {
    ss.append(1, delim);
  }
}

Splitter* NewPipeSplitter() { return new PipeSplitter(); }

std::vector<Slice> EncodingSplitter::Split(const Slice& s) const {
  Slice val, ss(s.data_, s.size_);
  std::vector<Slice> res;
  while (GetLengthPrefixedSlice(&ss, &val)) {
    res.emplace_back(val);
  }
  return res;
}

std::string EncodingSplitter::Stitch(const std::vector<Slice>& v) const {
  std::string res;  // RVO/NRVO/move
  for (const auto& s: v) {
    Append(res, s, false);
  }
  return res;
}

Slice EncodingSplitter::Stitch(const std::vector<Slice>& v,
                               std::string& buf) const {
  for (const auto& s : v) {
    Append(buf, s, false);
  }
  return buf;
}

void EncodingSplitter::Append(std::string& ss, const Slice& s, bool last) const {
  PutLengthPrefixedSlice(&ss, s);
}

Splitter* NewEncodingSplitter() { return new EncodingSplitter(); }

}  // namespace vidardb
