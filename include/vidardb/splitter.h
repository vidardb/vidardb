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
// sub-strings and joining multiple sub-strings.  A Splitter implementation
// must be thread-safe since vidardb may invoke its methods concurrently
// from multiple threads.
class Splitter {
 public:
  virtual ~Splitter();

  // The name of the splitter.  Used to check for splitter
  // mismatches (i.e., a DB created with one splitter is
  // accessed using a different splitter.
  //
  // Names starting with "vidardb." are reserved and should not be used
  // by any clients of this package.
  virtual const std::string Name() const = 0;

  // Split a string to multiple sub-strings.
  virtual std::vector<std::string> Split(const std::string& s) const = 0;

  // Join multiple sub-strings to a string.
  virtual std::string Join(const std::vector<std::string>& v) const = 0;

  // Append a sub-string.
  virtual void Append(std::string& ss, const std::string& s, bool last) const = 0;
};

// Return a builtin splitter that uses '|' to split a string.
// For example: s1|s2|s3...
extern const Splitter* PipeSplitter();

// Return a builtin splitter that uses the following encoding format:
// entry1entry2entry3...
// Every entry contains:
//   length (4B)
//   string
extern const Splitter* EncodingSplitter();

}  // namespace vidardb

#endif  // STORAGE_VIDARDB_INCLUDE_SPLITTER_H_
