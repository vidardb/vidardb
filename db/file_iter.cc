//  Copyright (c) 2021-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include "vidardb/file_iter.h"
#include "table/internal_iterator.h"

namespace vidardb {

FileIter::FileIter(SequenceNumber s) : sequence_(s), cur_(0) {}

FileIter::~FileIter() {
  for (auto it : children_) {
    delete it;
  }
}

bool FileIter::Valid() const { return cur_ < children_.size(); }

void FileIter::SeekToFirst() { cur_ = 0; }

void FileIter::Next() { ++cur_; }

Status FileIter::status() const {
  if (cur_ >= children_.size()) {
    return Status::NotFound("out of bound");
  }
  return Status::OK();
}

std::vector<InternalIterator*>* FileIter::GetInternalIterators() {
  return &children_;
}

Status FileIter::GetMinMax(std::vector<std::vector<MinMax>>& v) const {
  if (cur_ >= children_.size()) {
    return Status::NotFound("out of bound");
  }
  return children_[cur_]->GetMinMax(v);
}

Status FileIter::RangeQuery(const std::vector<bool>& block_bits, char* buf,
                            uint64_t capacity, uint64_t* valid_count,
                            uint64_t* total_count) const {
  if (cur_ >= children_.size()) {
    return Status::NotFound("out of bound");
  }
  return children_[cur_]->RangeQuery(block_bits, buf, capacity, valid_count,
                                     total_count);
}

}  // namespace vidardb
