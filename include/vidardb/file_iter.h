//  Copyright (c) 2021-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#pragma once

#include <vector>

#include "vidardb/iterator.h"
#include "vidardb/slice.h"
#include "vidardb/types.h"

namespace vidardb {

struct MinMax;
struct RangeQueryKeyVal;
class InternalIterator;

// File level iterator of picking up the next file (memtable, block based table,
// column table)
class FileIter : public Iterator {
 public:
  FileIter(SequenceNumber s) : sequence_(s), cur_(0) {}

  virtual ~FileIter();

  bool Valid() const override;

  void SeekToFirst() override;

  // not support
  void SeekToLast() override { return; }

  // TODO: not support now, but should support in the future for parallel scan
  void Seek(const Slice& target) override { return; }

  void Next() override;

  // not support
  void Prev() override { return; }

  // not support
  Slice key() const override { return Slice(); }

  // not support
  Slice value() const override { return Slice(); }

  Status status() const override;

  std::vector<InternalIterator*>* GetInternalIterators() { return &children_; }

  // Return the targeted columns' block min and max. If key is in the target
  // set, return its block min and max as well, but be cautious about its
  // different max. We have to use the type first to identify the file type
  // since not all target columns' min & max might be returned.
  //
  // For MemTable, if our interest set includes key, then min & max key is
  // returned. If not, nothing we can do here, meaning v is empty.
  //
  // For BlockBasedTable, if our interest set includes key, then a bunch of min
  // & max block keys are returned. If not, v is empty.
  //
  // For ColumnTable, min & max block keys and (or) values are always returned.
  //
  // NotFound status might be returned because table might be empty, note at
  // this case v is also empty, and a full scan should not be executed later.
  Status GetMinMax(std::vector<std::vector<MinMax>>& v) const;

  // According to the calculated block bits, fetch the partial tuples.
  // If key is not in the target, set it empty. Sometimes the block_bits is
  // empty, implying a full scan since no useful filters get from GetMinMax.
  // Empty table has already been recognized by NotFound status in GetMinMax;
  Status RangeQuery(const std::vector<bool>& block_bits,
                    std::vector<RangeQueryKeyVal>& res) const;

 private:
  std::vector<InternalIterator*> children_;
  SequenceNumber sequence_;
  size_t cur_;
};

}  // vidardb
