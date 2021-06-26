//  Copyright (c) 2021-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#pragma once

#include "table/block.h"

namespace vidardb {

// Similar to TwoLevelIterator, but with increased performance due to direct
// function call.
class SubColumnTableIterator {
 public:
  explicit SubColumnTableIterator(ColumnTable::BlockEntryIteratorState* state)
      : state_(state),
        valid_second_level_iter_(false),
        area_(nullptr),
        last_area_(nullptr) {
    state_->NewIndexIterator(&first_level_iter_);
  }
  virtual ~SubColumnTableIterator() { delete state_; }

  void SetArea(char* area) { area_ = area; }
  char* GetArea() { return area_; }

  void SeekToFirst() {
    first_level_iter_.SeekToFirst();
    InitDataBlock();
    if (valid_second_level_iter_) {
      second_level_iter_.SeekToFirst();
    }
    SkipEmptyDataBlocksForward();
  }

  bool Valid() const {
    return valid_second_level_iter_ ? second_level_iter_.Valid() : false;
  }

  // the following 3 are optimized for speed without touching the disk due to
  // second level access otherwise
  void FirstLevelSeekToFirst() { first_level_iter_.SeekToFirst(); }
  bool FirstLevelValid() const { return first_level_iter_.Valid(); }
  void FirstLevelNext(bool prepare_second_level) {
    first_level_iter_.Next();

    if (prepare_second_level) {
      InitDataBlock();
      if (valid_second_level_iter_) {
        second_level_iter_.SeekToFirst();
      }
    }
  }
  void SecondLevelNext() {
    assert(FirstLevelValid());
    second_level_iter_.Next();
  }
  Slice FirstLevelKey() const {
    assert(FirstLevelValid());
    return first_level_iter_.key();
  }
  Slice FirstLevelMin() const {
    assert(FirstLevelValid());
    return first_level_iter_.min();
  }
  Slice FirstLevelMax() const {
    assert(FirstLevelValid());
    return first_level_iter_.max();
  }

  Slice value() {
    assert(Valid());
    return second_level_iter_.value();
  }
  Status status() const {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!first_level_iter_.status().ok()) {
      return first_level_iter_.status();
    } else if (valid_second_level_iter_ &&
               !second_level_iter_.status().ok()) {
      return second_level_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SetSecondLevelIteratorInvalid() {
    if (valid_second_level_iter_) {
      SaveError(second_level_iter_.status());
    }
    valid_second_level_iter_ = false;
  }
  void InitDataBlock() {
    if (!first_level_iter_.Valid()) {
      SetSecondLevelIteratorInvalid();
    } else {
      Slice handle = first_level_iter_.value();
      if (valid_second_level_iter_ &&
          !second_level_iter_.status().IsIncomplete() &&
          handle.compare(data_block_handle_) == 0 && last_area_ == area_) {
        // second_level_iter is already constructed with this iterator, so
        // no need to change anything
      } else {
        data_block_handle_.assign(handle.data(), handle.size());
        last_area_ = area_;
        second_level_iter_.DoCleanup();  // release previous resource
        state_->NewDataIterator(handle, &second_level_iter_,
                                (area_ ? &area_ : nullptr));
        valid_second_level_iter_ = true;
      }
    }
  }
  void SkipEmptyDataBlocksForward() {
    while (!valid_second_level_iter_ ||
           (!second_level_iter_.Valid() &&
           !second_level_iter_.status().IsIncomplete())) {
      // Move to next block
      if (!first_level_iter_.Valid()) {
        SetSecondLevelIteratorInvalid();
        return;
      }
      first_level_iter_.Next();
      InitDataBlock();
      if (valid_second_level_iter_) {
        second_level_iter_.SeekToFirst();
      }
    }
  }

  ColumnTable::BlockEntryIteratorState* state_;
  MinMaxBlockIter first_level_iter_;
  SubColumnBlockIter second_level_iter_;  // May be not valid
  bool valid_second_level_iter_;
  Status status_;
  char* area_;  // Load the data blocks in this specified area consecutively
  // If second_level_iter is non-nullptr, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the second_level_iter.
  std::string data_block_handle_;
  char* last_area_;  // together with data_block_handle_ to identify the block
};

}  // namespace vidardb
