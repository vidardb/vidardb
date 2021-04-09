//  Copyright (c) 2021-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "db/pinned_iterators_manager.h"
#include "vidardb/options.h"
#include "vidardb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "util/arena.h"

namespace vidardb {

TwoLevelIterator::TwoLevelIterator(TwoLevelIteratorState* state,
                                   InternalIterator* first_level_iter,
                                   bool need_free_iter_and_state)
    : state_(state),
      first_level_iter_(first_level_iter),
      need_free_iter_and_state_(need_free_iter_and_state),
      pinned_iters_mgr_(nullptr) {}

TwoLevelIterator::~TwoLevelIterator() {
  // Assert that the TwoLevelIterator is never deleted while Pinning is
  // Enabled.
  assert(!pinned_iters_mgr_ ||
         (pinned_iters_mgr_ && !pinned_iters_mgr_->PinningEnabled()));
  first_level_iter_.DeleteIter(!need_free_iter_and_state_);
  second_level_iter_.DeleteIter(false);
  if (need_free_iter_and_state_) {
    delete state_;
  } else {
    state_->~TwoLevelIteratorState();
  }
}

void TwoLevelIterator::Seek(const Slice& target) {
  first_level_iter_.Seek(target);

  InitDataBlock();
  if (second_level_iter_.iter() != nullptr) {
    second_level_iter_.Seek(target);
  }
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
  first_level_iter_.SeekToFirst();
  InitDataBlock();
  if (second_level_iter_.iter() != nullptr) {
    second_level_iter_.SeekToFirst();
  }
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  first_level_iter_.SeekToLast();
  InitDataBlock();
  if (second_level_iter_.iter() != nullptr) {
    second_level_iter_.SeekToLast();
  }
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
  assert(Valid());
  second_level_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  second_level_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}

/**************************** Shichao *****************************/
void TwoLevelIterator::NextBlock() {
  first_level_iter_.Next();
  InitDataBlock();
  if (second_level_iter_.iter() != nullptr) {
    second_level_iter_.SeekToFirst();
  }
}

void TwoLevelIterator::NextInBlock() {
  assert(first_level_iter_.Valid());
  second_level_iter_.Next();
}
/**************************** Shichao *****************************/

Status TwoLevelIterator::status() const {
  // It'd be nice if status() returned a const Status& instead of a Status
  if (!first_level_iter_.status().ok()) {
    return first_level_iter_.status();
  } else if (second_level_iter_.iter() != nullptr &&
             !second_level_iter_.status().ok()) {
    return second_level_iter_.status();
  } else {
    return status_;
  }
}

void TwoLevelIterator::SetPinnedItersMgr(
    PinnedIteratorsManager* pinned_iters_mgr) {
  pinned_iters_mgr_ = pinned_iters_mgr;
  first_level_iter_.SetPinnedItersMgr(pinned_iters_mgr);
  if (second_level_iter_.iter()) {
    second_level_iter_.SetPinnedItersMgr(pinned_iters_mgr);
  }
}

bool TwoLevelIterator::IsKeyPinned() const {
  return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
         second_level_iter_.iter() && second_level_iter_.IsKeyPinned();
}

void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (second_level_iter_.iter() == nullptr ||
         (!second_level_iter_.Valid() &&
         !second_level_iter_.status().IsIncomplete())) {
    // Move to next block
    if (!first_level_iter_.Valid()) {
      SetSecondLevelIterator(nullptr);
      return;
    }
    first_level_iter_.Next();
    InitDataBlock();
    if (second_level_iter_.iter() != nullptr) {
      second_level_iter_.SeekToFirst();
    }
  }
}

void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (second_level_iter_.iter() == nullptr ||
         (!second_level_iter_.Valid() &&
         !second_level_iter_.status().IsIncomplete())) {
    // Move to next block
    if (!first_level_iter_.Valid()) {
      SetSecondLevelIterator(nullptr);
      return;
    }
    first_level_iter_.Prev();
    InitDataBlock();
    if (second_level_iter_.iter() != nullptr) {
      second_level_iter_.SeekToLast();
    }
  }
}

void TwoLevelIterator::SetSecondLevelIterator(InternalIterator* iter) {
  if (second_level_iter_.iter() != nullptr) {
    SaveError(second_level_iter_.status());
  }

  if (pinned_iters_mgr_ && iter) {
    iter->SetPinnedItersMgr(pinned_iters_mgr_);
  }

  InternalIterator* old_iter = second_level_iter_.Set(iter);
  if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
    pinned_iters_mgr_->PinIteratorIfNeeded(old_iter);
  } else {
    delete old_iter;
  }
}

void TwoLevelIterator::InitDataBlock() {
  if (!first_level_iter_.Valid()) {
    SetSecondLevelIterator(nullptr);
  } else {
    Slice handle = first_level_iter_.value();
    if (second_level_iter_.iter() != nullptr &&
        !second_level_iter_.status().IsIncomplete() &&
        handle.compare(data_block_handle_) == 0) {
      // second_level_iter is already constructed with this iterator, so
      // no need to change anything
    } else {
      InternalIterator* iter = state_->NewSecondaryIterator(handle);
      data_block_handle_.assign(handle.data(), handle.size());
      SetSecondLevelIterator(iter);
    }
  }
}

InternalIterator* NewTwoLevelIterator(TwoLevelIteratorState* state,
                                      InternalIterator* first_level_iter,
                                      Arena* arena,
                                      bool need_free_iter_and_state) {
  if (arena == nullptr) {
    return new TwoLevelIterator(state, first_level_iter,
                                need_free_iter_and_state);
  } else {
    auto mem = arena->AllocateAligned(sizeof(TwoLevelIterator));
    return new (mem)
        TwoLevelIterator(state, first_level_iter, need_free_iter_and_state);
  }
}

}  // namespace vidardb
