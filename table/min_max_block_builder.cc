//  Copyright (c) 2021-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "min_max_block_builder.h"

#include "util/coding.h"

namespace vidardb {

size_t MinMaxBlockBuilder::EstimateSizeAfterKVM(const Slice& key,
                                                const Slice& value,
                                                const Slice& min,
                                                const Slice& max) const {
  size_t estimate = BlockBuilder::EstimateSizeAfterKV(key, value);
  estimate += min.size() + max.size();
  estimate += sizeof(int32_t); // varint for shared prefix length.
  estimate += VarintLength(min.size()); // varint for min length.
  estimate += VarintLength(max.size()); // varint for max length.
  return estimate;
}

void MinMaxBlockBuilder::Add(const Slice& key, const Slice& value,
                             const Slice& min, const Slice& max) {
  BlockBuilder::Add(key, value);

  PutVarint32(&buffer_, static_cast<uint32_t>(min.size()));
  buffer_.append(min.data(), min.size());

  size_t shared = 0;  // number of bytes shared with prev min or max

  // See how much sharing to do with previous string
  size_t min_length = std::min(min.size(), max.size());
  while ((shared < min_length) && (min[shared] == max[shared])) {
    shared++;
  }
  const size_t non_shared = max.size() - shared;

  // Add "<shared><non_shared>" to buffer_
  PutVarint32(&buffer_, static_cast<uint32_t>(shared));
  PutVarint32(&buffer_, static_cast<uint32_t>(non_shared));

  // Add string delta to buffer_ followed by value
  buffer_.append(max.data() + shared, non_shared);
}

}  // namespace vidardb
