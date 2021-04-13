//  Copyright (c) 2021-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include "vidardb/file_iter.h"
#include "table/internal_iterator.h"

namespace vidardb {

bool FileIter::Valid() const {
  if (children_.empty()) {
    return false;
  }
  return children_[cur_]->Valid();
}

void FileIter::SeekToFirst() {
  if (children_.empty()) {
    return;
  }
  cur_ = 0;
}

void FileIter::Next() {
  cur_++;
}

Status FileIter::status() const {
  if (children_.empty()) {
    return Status::NotFound();
  }
  return children_[cur_]->status();
}

Status FileIter::GetMinMax(std::vector<std::vector<MinMax>>& v) const {
  return children_[cur_]->GetMinMax(v);
}

Status FileIter::RangeQuery(const std::vector<bool>& block_bits,
                            std::vector<RangeQueryKeyVal>& res) const {
  return children_[cur_]->RangeQuery(block_bits, res);
}

}  // namespace vidardb
