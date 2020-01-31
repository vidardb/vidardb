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

Splitter::~Splitter() { }

namespace {
class PipeSplitterImpl : public Splitter {
 public:
  PipeSplitterImpl() { }

  virtual const std::string Name() const override {
    return "vidardb.PipeSplitter";
  }

  virtual std::vector<std::string> Split(const std::string& s) const override {
    return StringSplit(s, delim);
  }

  virtual std::string Join(const std::vector<std::string>& v) const override {
    return StringJoin(v, delim);
  }

  virtual void Append(std::string& ss, const std::string& s, bool last) const 
    override {
    StringAppend(ss, s, delim, !last);
  }

 private:
  const char delim = '|';
};

class EncodingSplitterImpl : public Splitter {
 public:
  EncodingSplitterImpl() { }

  virtual const std::string Name() const override {
    return "vidardb.EncodingSplitter";
  }

  virtual std::vector<std::string> Split(const std::string& s) const override {
    std::vector<std::string> result;
    for (auto i = 0u; i < s.size();) {
      assert(i + 4 < s.size());
      uint32_t length = DecodeFixed32BigEndian(s.data() + i);
      assert(i + 4 + length < s.size());
      result.push_back(s.substr(i + 4, length));
      i = i + 4 + length;
    }
    return result;
  }

  virtual std::string Join(const std::vector<std::string>& v) const override {
    std::string result;
    for (auto s: v) {
      Append(result, s, false);
    }
    return result;
  }

  virtual void Append(std::string& ss, const std::string& s, bool last) const 
    override {
    std::string length;
    PutFixed32BigEndian(&length, s.size());
    ss.append(length);
    ss.append(s);
  }
};

} // namespace

const Splitter* PipeSplitter() {
  static PipeSplitterImpl pipe;
  return &pipe;
}

const Splitter* EncodingSplitter() {
  static EncodingSplitterImpl encoding;
  return &encoding;
}

}  // namespace vidardb
