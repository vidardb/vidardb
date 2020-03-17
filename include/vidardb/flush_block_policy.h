//  Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <string>

#include "vidardb/table.h"

namespace vidardb {

class Slice;
class BlockBuilder;
struct Options;

// FlushBlockPolicy provides a configurable way to determine when to flush a
// block in the block based tables,
class FlushBlockPolicy {
 public:
  // Keep track of the key/value sequences and return the boolean value to
  // determine if table builder should flush current data block.
  virtual bool Update(const Slice& key, const Slice& value) = 0;

  virtual ~FlushBlockPolicy() {}
};

class FlushBlockPolicyFactory {
 public:
  // Return the name of the flush block policy.
  virtual const char* Name() const = 0;

  // Return a new block flush policy that flushes data blocks by data size.
  // FlushBlockPolicy may need to access the metadata of the data block
  // builder to determine when to flush the blocks.
  //
  // Callers must delete the result after any database that is using the
  // result has been closed.
  virtual FlushBlockPolicy* NewFlushBlockPolicy(
      const BlockBasedTableOptions& table_options,
      const BlockBuilder& data_block_builder) const = 0;

  /***************************** Shichao ******************************/
  virtual FlushBlockPolicy* NewFlushBlockPolicy(
      const ColumnTableOptions& table_options,
      const BlockBuilder& data_block_builder) const = 0;
  /***************************** Shichao ******************************/

  virtual ~FlushBlockPolicyFactory() {}
};

class FlushBlockBySizePolicyFactory : public FlushBlockPolicyFactory {
 public:
  FlushBlockBySizePolicyFactory() {}

  virtual const char* Name() const override {
    return "FlushBlockBySizePolicyFactory";
  }

  virtual FlushBlockPolicy* NewFlushBlockPolicy(
      const BlockBasedTableOptions& table_options,
      const BlockBuilder& data_block_builder) const override;

  /***************************** Shichao ******************************/
  virtual FlushBlockPolicy* NewFlushBlockPolicy(
      const ColumnTableOptions& table_options,
      const BlockBuilder& data_block_builder) const override;
  /***************************** Shichao ******************************/
};

}  // namespace vidardb
