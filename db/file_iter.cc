//  Copyright (c) 2021-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include "vidardb/file_iter.h"
#include "table/internal_iterator.h"

namespace vidardb {

FileIter::~FileIter() {
  for (auto it : children_) {
    delete it;
  }
}

bool FileIter::Valid() const {
  if (cur_ >= children_.size()) {
    // empty or out of index
    return false;
  }
  return children_[cur_]->Valid();
}

void FileIter::SeekToFirst() {
  if (cur_ >= children_.size()) {
    return;
  }
  // Since mutable MemTable might be empty, we have to seek next one.
  // We don't need loop here, but just be conservative
  for (cur_ = 0; cur_ < children_.size(); cur_++) {
    children_[cur_]->SeekToFirst();
    if (children_[cur_]->Valid()) {
      // not empty
      return;
    }
  }
}

void FileIter::Next() {
  cur_++;
  if (cur_ < children_.size()) {
    children_[cur_]->SeekToFirst();
    // no empty table anymore
    assert(children_[cur_]->Valid());
  }
}

Status FileIter::status() const {
  if (cur_ >= children_.size()) {
    return Status::NotFound();
  }
  return children_[cur_]->status();
}

Status FileIter::GetMinMax(std::vector<std::vector<MinMax>>& v) const {
  if (cur_ >= children_.size()) {
    return Status::NotFound();
  }
  return children_[cur_]->GetMinMax(v);
}

Status FileIter::RangeQuery(const std::vector<bool>& block_bits,
                            std::vector<RangeQueryKeyVal>& res) const {
  if (cur_ >= children_.size()) {
    return Status::NotFound();
  }
  // children_[cur_]->Valid() is false
  return children_[cur_]->RangeQuery(block_bits, res);
}

}  // namespace vidardb
