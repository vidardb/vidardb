//  Copyright (c) 2021-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#pragma once

#include "table/block_builder.h"

namespace vidardb {

class MainColumnBlockBuilder : public BlockBuilder {
 public:
  MainColumnBlockBuilder(const MainColumnBlockBuilder&) = delete;
  void operator=(const MainColumnBlockBuilder&) = delete;

  explicit MainColumnBlockBuilder(int block_restart_interval)
    : BlockBuilder(block_restart_interval) {}

  virtual ~MainColumnBlockBuilder() {}

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  virtual void Add(const Slice& key, const Slice& value) override;

  // Returns an estimated block size after appending key and value.
  virtual size_t EstimateSizeAfterKV(const Slice& key,
                                     const Slice& value) const override;
};

}  // namespace vidardb
