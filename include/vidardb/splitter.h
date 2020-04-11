//  Copyright (c) 2020-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef STORAGE_VIDARDB_INCLUDE_SPLITTER_H_
#define STORAGE_VIDARDB_INCLUDE_SPLITTER_H_

#include <string>
#include <vector>

#include "vidardb/slice.h"

namespace vidardb {

// A Splitter object provides the function of splitting a slice to multiple
// sub-slices and stitching multiple sub-slices.  A Splitter implementation
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

  // Split a slice to multiple sub-slices.
  virtual std::vector<Slice> Split(const Slice& s) const = 0;

  // Stitch multiple sub-slices to a string.
  virtual std::string Stitch(const std::vector<Slice>& v) const = 0;

  // Stitch multiple sub-slices to a slice using buf as storage.
  // Note: buf must exist as long as the returned Slice exists.
  virtual Slice Stitch(const std::vector<Slice>& v, std::string* buf) const = 0;

  // Append a sub-slice.
  virtual void Append(std::string& ss, const Slice& s, bool last) const = 0;
};

// A built-in pipe splitter that uses '|' to split a slice.
// For example: s1|s2|s3...
class PipeSplitter : public Splitter {
 public:
  PipeSplitter() { }

  virtual const char* Name() const override { return "vidardb.PipeSplitter"; }

  virtual std::vector<Slice> Split(const Slice& s) const override;

  virtual std::string Stitch(const std::vector<Slice>& v) const override;

  virtual Slice Stitch(const std::vector<Slice>& v,
                       std::string* buf) const override;

  virtual void Append(std::string& ss, const Slice& s, bool last) const override;

 private:
  const char delim = '|';
};

// Create a default pipe splitter.
extern Splitter* NewPipeSplitter();

// A built-in encoding splitter that uses the following format:
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

  virtual std::vector<Slice> Split(const Slice& s) const override;

  virtual std::string Stitch(const std::vector<Slice>& v) const override;

  virtual Slice Stitch(const std::vector<Slice>& v,
                       std::string* buf) const override;

  virtual void Append(std::string& ss, const Slice& s, bool last) const override;
};

// Create a default encoding splitter
extern Splitter* NewEncodingSplitter();

}  // namespace vidardb

#endif  // STORAGE_VIDARDB_INCLUDE_SPLITTER_H_
