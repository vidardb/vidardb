//  Copyright (c) 2021-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include "table/block_builder.h"

namespace vidardb {

// index block with mix and max
class MinMaxBlockBuilder : public BlockBuilder {
 public:
  MinMaxBlockBuilder(const MinMaxBlockBuilder&) = delete;
  void operator=(const MinMaxBlockBuilder&) = delete;

  explicit MinMaxBlockBuilder(int block_restart_interval)
    : BlockBuilder(block_restart_interval) {}

  virtual ~MinMaxBlockBuilder() {}

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  using BlockBuilder::Add;
  virtual void Add(const Slice& key, const Slice& value,
                   const Slice& min, const Slice& max);

  // Returns an estimated block size after appending key and value and min_max.
  virtual size_t EstimateSizeAfterKVM(const Slice& key, const Slice& value,
                                      const Slice& min, const Slice& max) const;
};

}  // namespace vidardb
