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

struct MinMax {
  std::string min_;
  std::string max_;

  MinMax(const std::string& min = {}, const std::string& max = {})
    : min_(min), max_(max) {}
};

class InternalIterator;

// Flie level iterator of picking up the next file
class FileIter : public Iterator {
 public:
  FileIter(SequenceNumber s) : sequence_(s), cur_(0) {}

  virtual ~FileIter() {}

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
  // different max.
  Status GetMinMax(std::vector<std::vector<MinMax>>& v) const;

  // According to the calculated block bits, fetch the partial tuples. If key is
  // not in the target set, don't bother to return it.
  Status RangeQuery(const std::vector<bool>& block_bits,
                    std::vector<std::string>& res) const;

 private:
  std::vector<InternalIterator*> children_;
  SequenceNumber sequence_;
  size_t cur_;
};

}  // vidardb