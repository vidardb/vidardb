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
class InternalIterator;

// File level iterator of picking up the next file (memtable, block based table,
// column table)
class FileIter : public Iterator {
 public:
  FileIter(SequenceNumber s);

  virtual ~FileIter();

  bool Valid() const override;

  void SeekToFirst() override;

  void Next() override;

  Status status() const override;

  std::vector<InternalIterator*>* GetInternalIterators();

  // Return the targeted columns' block min and max. If key is in the target
  // set, return its block min and max as well, but be cautious about its
  // different max.
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

  // Estimate the size of current range query buffer to store required data
  // blocks and meta data.
  uint64_t EstimateRangeQueryBufSize() { /* not implemented */ return 0; }

  // According to the calculated block bits, fetch the relevant attributes.
  // Sometimes the block_bits is empty, implying a full scan since no useful
  // filters get from GetMinMax. Empty table has already been recognized by
  // NotFound status in GetMinMax;
  //
  // Both valid_count and total_count record the tuple-wise number.
  Status RangeQuery(const std::vector<bool>& block_bits, char* buf,
                    uint64_t capacity, uint64_t* valid_count,
                    uint64_t* total_count) const;

 private:
  std::vector<InternalIterator*> children_;
  SequenceNumber sequence_;
  size_t cur_;
};

}  // namespace vidardb
