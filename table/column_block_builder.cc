//  Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// SubColumnBlockBuilder generates blocks where keys are recorded every
// block_restart_interval:
//
// The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key. Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     key_length: varint32        [every interval start]
//     key: char[key_length]       [every interval start]
//     value_length: varint32
//     value: char[value_length]
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include <algorithm>
#include <assert.h>
#include "vidardb/comparator.h"

#include "column_block_builder.h"
#include "db/dbformat.h"
#include "util/coding.h"

namespace vidardb {

size_t ColumnBlockBuilder::EstimateSizeAfterKV(const Slice& key,
                                               const Slice& value) const {
  size_t estimate = CurrentSizeEstimate();
  estimate += value.size();
  if (counter_ >= block_restart_interval_) {
    estimate += sizeof(uint32_t); // a new restart entry.
    estimate += key.size();
    estimate += VarintLength(key.size());
  }

  estimate += VarintLength(value.size()); // varint for value length.

  return estimate;
}

void ColumnBlockBuilder::Add(const Slice& key, const Slice& value) {
  assert(!finished_);
  assert(counter_ <= block_restart_interval_);
  if (counter_ >= block_restart_interval_) {
    // Restart compression
    restarts_.push_back(static_cast<uint32_t>(buffer_.size()));
    counter_ = 0;
  }

  // key is actually an increasing number sequence, so we only store the key in
  // start of every restart array to save space
  if (counter_ == 0) {
    PutVarint32(&buffer_, static_cast<uint32_t>(key.size()));
    buffer_.append(key.data(), key.size());
  }
  PutVarint32(&buffer_, static_cast<uint32_t>(value.size()));
  buffer_.append(value.data(), value.size());

  // Update state
  counter_++;
}

}  // namespace vidardb
