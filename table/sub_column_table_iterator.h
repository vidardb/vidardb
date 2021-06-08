//  Copyright (c) 2021-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#pragma once

#include "table/block.h"
#include "table/two_level_iterator.h"


namespace vidardb {

// Similar to TwoLevelIterator, but with increased performance due to direct
// function call.
class SubColumnTableIterator {
 public:
  explicit SubColumnTableIterator(TwoLevelIteratorState* state,
                                  MinMaxBlockIter* first_level_iter)
      : state_(state), first_level_iter_(first_level_iter) {}

  virtual ~SubColumnTableIterator() {
    delete first_level_iter_;
    delete state_;
  }

  void SeekToFirst() {
    first_level_iter_->SeekToFirst();
    InitDataBlock();
    second_level_iter_.SeekToFirst();
    SkipEmptyDataBlocksForward();
  }

  void Next() {
    assert(Valid());
    second_level_iter_.Next();
    if ((!second_level_iter_.Valid() &&
         !second_level_iter_.status().IsIncomplete())) {
      SkipEmptyDataBlocksForward();
    } else {
      return;
    }
  }

  /******************** Shichao ******************/
  // the following 3 are optimized for speed without touching the disk due to
  // second level access otherwise
  void FirstLevelSeekToFirst() { first_level_iter_->SeekToFirst(); }
  bool FirstLevelValid() const { return first_level_iter_->Valid(); }
  void FirstLevelNext(bool prepare_second_level) {
    first_level_iter_->Next();

    if (prepare_second_level) {
      InitDataBlock();
      second_level_iter_.SeekToFirst();
    }
  }
  void SecondLevelNext() {
    assert(first_level_iter_->Valid());
    second_level_iter_.Next();
  }
  Slice FirstLevelKey() const {
    assert(Valid());
    return first_level_iter_->key();
  }
  Slice FirstLevelMin() const {
    assert(Valid());
    return first_level_iter_->min();
  }
  Slice FirstLevelMax() const {
    assert(Valid());
    return first_level_iter_->max();
  }
  /******************** Shichao ******************/

  bool Valid() const {
    return second_level_iter_.Valid();
  }
  Slice key() const {
    assert(Valid());
    return second_level_iter_.key();
  }
  Slice value() {
    assert(Valid());
    return second_level_iter_.value();
  }
  Status status() const {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!first_level_iter_->status().ok()) {
      return first_level_iter_->status();
    } else if (!second_level_iter_.status().ok()) {
      return second_level_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward() {
    while ((!second_level_iter_.Valid() &&
           !second_level_iter_.status().IsIncomplete())) {
      // Move to next block
      if (!first_level_iter_->Valid()) {
        SetSecondLevelIterator(nullptr);
        return;
      }
      first_level_iter_->Next();
      InitDataBlock();
      second_level_iter_.SeekToFirst();
    }
  }
  void SetSecondLevelIterator(ColumnBlockIter* iter);
  void InitDataBlock();

  TwoLevelIteratorState* state_;
  MinMaxBlockIter* first_level_iter_;
  ColumnBlockIter second_level_iter_;  // May be nullptr
  Status status_;
  // If second_level_iter is non-nullptr, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the second_level_iter.
  std::string data_block_handle_;
};

void SubColumnTableIterator::SetSecondLevelIterator(ColumnBlockIter* iter) {
  SaveError(second_level_iter_.status());

//    delete second_level_iter_;
//    second_level_iter_ = iter;
}

void SubColumnTableIterator:: InitDataBlock() {
    if (!first_level_iter_->Valid()) {
      SetSecondLevelIterator(nullptr);
    } else {
      Slice handle = first_level_iter_->value();
      if (!second_level_iter_.status().IsIncomplete() &&
          handle.compare(data_block_handle_) == 0) {
        // second_level_iter is already constructed with this iterator, so
        // no need to change anything
      } else {
//        InternalIterator* iter = state_->NewSecondaryIterator(handle);
        data_block_handle_.assign(handle.data(), handle.size());
//        SetSecondLevelIterator(dynamic_cast<ColumnBlockIter*>(iter));
        state_->NewIterator(handle, &second_level_iter_);
      }
    }
  }

}  // namespace vidardb
