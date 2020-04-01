//  Copyright (c) 2020-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef STORAGE_VIDARDB_INCLUDE_SPLITTER_H_
#define STORAGE_VIDARDB_INCLUDE_SPLITTER_H_

#include <string>
#include <vector>

namespace vidardb {

// A Splitter object provides the function of splitting a string to multiple
// sub-strings and stitching multiple sub-strings.  A Splitter implementation
// must be thread-safe since vidardb may invoke its methods concurrently
// from multiple threads.
class Splitter {
 public:
  virtual ~Splitter() {}

  // The name of the splitter.  Used to check for splitter
  // mismatches (i.e., a DB created with one splitter is
  // accessed using a different splitter.
  //
  // Names starting with "vidardb." are reserved and should not be used
  // by any clients of this package.
  virtual const char* Name() const = 0;

  // Split a string to multiple sub-strings.
  virtual std::vector<std::string> Split(const std::string& s) const = 0;

  // Stitch multiple sub-strings to a string.
  virtual std::string Stitch(const std::vector<std::string>& v) const = 0;

  // Append a sub-string.
  virtual void Append(std::string& ss, const Slice& s, bool last) const = 0;
};

// A builtin pipe splitter that uses '|' to split a string.
// For example: s1|s2|s3...
class PipeSplitter : public Splitter {
 public:
  PipeSplitter() { }

  virtual const char* Name() const override { return "vidardb.PipeSplitter"; }

  virtual std::vector<std::string> Split(const std::string& s) const override;

  virtual std::string Stitch(const std::vector<std::string>& v) const override;

  virtual void Append(std::string& ss, const Slice& s, bool last) const override;

 private:
  const char delim = '|';
};

// Create default pipe splitter.
extern Splitter* NewPipeSplitter();

// A builtin encoding splitter that uses the following format:
// entry1entry2entry3...
//
// Each entry contains:
//   length (variable)
//   string
class EncodingSplitter : public Splitter {
 public:
  EncodingSplitter() { }

  virtual const char* Name() const override {
    return "vidardb.EncodingSplitter";
  }

  virtual std::vector<std::string> Split(const std::string& s) const override;

  virtual std::string Stitch(const std::vector<std::string>& v) const override;

  virtual void Append(std::string& ss, const Slice& s, bool last) const override;
};

// Create default encoding splitter
extern Splitter* NewEncodingSplitter();

}  // namespace vidardb

#endif  // STORAGE_VIDARDB_INCLUDE_SPLITTER_H_
