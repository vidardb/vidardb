//  Copyright (c) 2021-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// MainColumnBlockBuilder generates blocks where values are recorded every
// block_restart_interval:
//
// The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.
//
// An entry for a particular key-value pair has the form:
//     key_length: varint32
//     key: char[value_length]
//     value: char[value_length]      [every interval start]
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/main_column_block_builder.h"
#include "util/coding.h"

namespace vidardb {

size_t MainColumnBlockBuilder::EstimateSizeAfterKV(const Slice& key,
                                                   const Slice& value) const {
  size_t estimate = CurrentSizeEstimate();
  estimate += VarintLength(key.size()); // varint for key length.
  estimate += key.size();

  if (counter_ >= block_restart_interval_) {
    estimate += sizeof(uint32_t); // a new restart entry.
    estimate += value.size(); // store value
  }

  return estimate;
}

void MainColumnBlockBuilder::Add(const Slice& key, const Slice& value) {
  assert(!finished_);
  assert(counter_ <= block_restart_interval_);

  if (counter_ >= block_restart_interval_) {
    // Restart compression
    restarts_.push_back(static_cast<uint32_t>(buffer_.size()));
    counter_ = 0;
  }

  // Add "<key_size>" to buffer_ and append key
  PutVarint32(&buffer_, static_cast<uint32_t>(key.size()));
  buffer_.append(key.data(), key.size());

  // Only store the value in each restart
  // Since we know the size of value (32bits), don't store its size.
  if (counter_ == 0) {
    buffer_.append(value.data(), value.size());
  }

  // Update state
  last_key_.assign(key.data(), key.size());
  assert(Slice(last_key_) == key);
  counter_++;
}

}  // namespace vidardb
