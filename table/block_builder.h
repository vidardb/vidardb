//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stdint.h>

#include <vector>

#include "vidardb/slice.h"

namespace vidardb {

class BlockBuilder {
 public:
  BlockBuilder(const BlockBuilder&) = delete;
  void operator=(const BlockBuilder&) = delete;

  explicit BlockBuilder(int block_restart_interval);

  virtual ~BlockBuilder() {}

  // Reset the contents as if the BlockBuilder was just constructed.
  virtual void Reset();

  // REQUIRES: Finish() has not been callled since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  virtual void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  virtual Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  virtual size_t CurrentSizeEstimate() const;

  // Returns an estimated block size after appending key and value.
  virtual size_t EstimateSizeAfterKV(const Slice& key,
                                     const Slice& value) const;

  // Return true iff no entries have been added since the last Reset()
  virtual bool empty() const { return buffer_.empty(); }

  // Called after Add
  virtual bool IsKeyStored() const { return true; }

 protected:
  const int block_restart_interval_;

  std::string buffer_;              // Destination buffer
  std::vector<uint32_t> restarts_;  // Restart points
  int counter_;                     // Number of entries emitted since restart
  bool finished_;                   // Has Finish() been called?
  std::string last_key_;
};

}  // namespace vidardb
