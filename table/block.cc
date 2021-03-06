//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Decodes the blocks generated by block_builder.cc.

#include "table/block.h"

#include <algorithm>
#include <string>
#include <unordered_map>
#include <vector>

#include "vidardb/comparator.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/perf_context_imp.h"

namespace vidardb {

BlockIter::BlockIter()
    : comparator_(nullptr),
      data_(nullptr),
      restarts_(0),
      num_restarts_(0),
      current_(0),
      restart_index_(0),
      status_(Status::OK()) {}

BlockIter::BlockIter(const Comparator* comparator, const char* data,
                     uint32_t restarts, uint32_t num_restarts)
    : BlockIter() {
  Initialize(comparator, data, restarts, num_restarts);
}

void BlockIter::Initialize(const Comparator* comparator, const char* data,
                           uint32_t restarts, uint32_t num_restarts) {
  //  assert(data_ == nullptr);  // Now we allow it to get called multiple times
  // as long as its resource is released
  assert(num_restarts > 0);  // Ensure the param is valid

  comparator_ = comparator;
  data_ = data;
  restarts_ = restarts;
  num_restarts_ = num_restarts;
  current_ = restarts_;
  restart_index_ = num_restarts_;
}

#ifndef NDEBUG
BlockIter::~BlockIter() {
  // Assert that the BlockIter is never deleted while Pinning is Enabled.
  assert(!pinned_iters_mgr_ ||
         (pinned_iters_mgr_ && !pinned_iters_mgr_->PinningEnabled()));
}
#endif

void BlockIter::Prev() {
  assert(Valid());

  // Scan backwards to a restart point before current_
  const uint32_t original = current_;
  while (GetRestartPoint(restart_index_) >= original) {
    if (restart_index_ == 0) {
      // No more entries
      current_ = restarts_;
      restart_index_ = num_restarts_;
      return;
    }
    restart_index_--;
  }

  SeekToRestartPoint(restart_index_);
  do {
    // Loop until end of current entry hits the start of original entry
  } while (ParseNextKey() && NextEntryOffset() < original);
}

void BlockIter::Seek(const Slice& target) {
  PERF_TIMER_GUARD(block_seek_nanos);
  if (data_ == nullptr) {  // Not init yet
    return;
  }
  uint32_t index = 0;
  bool ok = BinarySeek(target, 0, num_restarts_ - 1, &index);
  if (!ok) {
    return;
  }
  SeekToRestartPoint(index);
  // Linear search (within restart area) for first key >= target

  while (true) {
    if (!ParseNextKey() || Compare(key_.GetKey(), target) >= 0) {
      return;
    }
  }
}

void BlockIter::SeekToFirst() {
  if (data_ == nullptr) {  // Not init yet
    return;
  }
  SeekToRestartPoint(0);
  ParseNextKey();
}

void BlockIter::SeekToLast() {
  if (data_ == nullptr) {  // Not init yet
    return;
  }
  SeekToRestartPoint(num_restarts_ - 1);
  while (ParseNextKey() && NextEntryOffset() < restarts_) {
    // Keep skipping
  }
}

void BlockIter::CorruptionError() {
  current_ = restarts_;
  restart_index_ = num_restarts_;
  status_ = Status::Corruption("bad entry in block");
  key_.Clear();
  value_.clear();
}

// Binary search in restart array to find the first restart point
// with a key >= target (TODO: this comment is inaccurate)
bool BlockIter::BinarySeek(const Slice& target, uint32_t left, uint32_t right,
                           uint32_t* index) {
  assert(left <= right);

  while (left < right) {
    uint32_t mid = (left + right + 1) / 2;
    uint32_t region_offset = GetRestartPoint(mid);
    uint32_t shared, non_shared, value_length;
    const char* key_ptr =
        DecodeEntry(data_ + region_offset, data_ + restarts_, &shared,
                    &non_shared, &value_length);
    if (key_ptr == nullptr || (shared != 0)) {
      CorruptionError();
      return false;
    }
    Slice mid_key(key_ptr, non_shared);
    int cmp = Compare(mid_key, target);
    if (cmp < 0) {
      // Key at "mid" is smaller than "target". Therefore all
      // blocks before "mid" are uninteresting.
      left = mid;
    } else if (cmp > 0) {
      // Key at "mid" is >= "target". Therefore all blocks at or
      // after "mid" are uninteresting.
      right = mid - 1;
    } else {
      left = right = mid;
    }
  }

  *index = left;
  return true;
}

SubColumnBlockIter::SubColumnBlockIter(const Comparator* comparator,
                                       const char* data, uint32_t restarts,
                                       uint32_t num_restarts)
    : SubColumnBlockIter() {
  Initialize(comparator, data, restarts, num_restarts);
}

void SubColumnBlockIter::Seek(const Slice& target) {
  PERF_TIMER_GUARD(block_seek_nanos);
  if (data_ == nullptr) {  // Not init yet
    return;
  }
  uint32_t index = 0;
  bool ok = BinarySeek(target, 0, num_restarts_ - 1, &index);
  if (!ok) {
    return;
  }

  SeekToRestartPoint(index);

  if (!ParseNextKey()) {
    return;
  }

  Slice key = key_.GetKey();
  uint32_t restart_pos = 0;
  GetFixed32BigEndian(&key, &restart_pos);

  uint32_t target_pos = 0;
  GetFixed32BigEndian(&target, &target_pos);

  uint32_t step = target_pos - restart_pos;

  // Linear search (within restart area) for first key >= target
  for (uint32_t i = 0u; i < step; i++) {
    if (!ParseNextKey()) {
      return;
    }
  }
}

// Binary search in restart array to find the first restart point
// with a key >= target (TODO: this comment is inaccurate)
bool SubColumnBlockIter::BinarySeek(const Slice& target, uint32_t left,
                                    uint32_t right, uint32_t* index) {
  assert(left <= right);

  while (left < right) {
    uint32_t mid = (left + right + 1) / 2;
    uint32_t region_offset = GetRestartPoint(mid);
    uint32_t key_length;
    const char* key_ptr =
        DecodeKeyOrValue(data_ + region_offset, data_ + restarts_, &key_length);
    if (key_ptr == nullptr) {
      CorruptionError();
      return false;
    }
    Slice mid_key(key_ptr, key_length);
    int cmp = Compare(mid_key, target);
    if (cmp < 0) {
      // Key at "mid" is smaller than "target". Therefore all
      // blocks before "mid" are uninteresting.
      left = mid;
    } else if (cmp > 0) {
      // Key at "mid" is >= "target". Therefore all blocks at or
      // after "mid" are uninteresting.
      right = mid - 1;
    } else {
      left = right = mid;
    }
  }

  *index = left;
  return true;
}

MainColumnBlockIter::MainColumnBlockIter(const Comparator* comparator,
                                         const char* data, uint32_t restarts,
                                         uint32_t num_restarts)
    : MainColumnBlockIter() {
  Initialize(comparator, data, restarts, num_restarts);
}

void MainColumnBlockIter::CorruptionError() {
  BlockIter::CorruptionError();
  has_val_ = false;
  int_val_ = 0;
  str_val_.empty();
}

// Binary search in restart array to find the first restart point
// with a key >= target
bool MainColumnBlockIter::BinarySeek(const Slice& target, uint32_t left,
                                     uint32_t right, uint32_t* index) {
  assert(left <= right);

  while (left < right) {
    uint32_t mid = (left + right + 1) / 2;
    uint32_t region_offset = GetRestartPoint(mid);
    uint32_t key_length = 0;
    const char* key_ptr = SubColumnBlockIter::DecodeKeyOrValue(
        data_ + region_offset, data_ + restarts_, &key_length);
    if (key_ptr == nullptr) {
      CorruptionError();
      return false;
    }
    Slice mid_key(key_ptr, key_length);
    int cmp = Compare(mid_key, target);
    if (cmp < 0) {
      // Key at "mid" is smaller than "target". Therefore all
      // blocks before "mid" are uninteresting.
      left = mid;
    } else if (cmp > 0) {
      // Key at "mid" is >= "target". Therefore all blocks at or
      // after "mid" are uninteresting.
      right = mid - 1;
    } else {
      left = right = mid;
    }
  }

  *index = left;
  return true;
}

MinMaxBlockIter::MinMaxBlockIter(const Comparator* comparator, const char* data,
                                 uint32_t restarts, uint32_t num_restarts)
    : MinMaxBlockIter() {
  Initialize(comparator, data, restarts, num_restarts);
}

void MinMaxBlockIter::CorruptionError() {
  BlockIter::CorruptionError();
  min_.clear();
  max_.clear();
  max_storage_len_ = 0;
}

uint32_t Block::NumRestarts() const {
  assert(size_ >= 2 * sizeof(uint32_t));
  return DecodeFixed32(data_ + size_ - sizeof(uint32_t));
}

Block::Block(BlockContents&& contents)
    : contents_(std::move(contents)),
      data_(contents_.data.data()),
      size_(contents_.data.size()) {
  if (size_ < sizeof(uint32_t)) {
    size_ = 0;  // Error marker
  } else {
    restart_offset_ =
        static_cast<uint32_t>(size_) - (1 + NumRestarts()) * sizeof(uint32_t);
    if (restart_offset_ > size_ - sizeof(uint32_t)) {
      // The size is too small for NumRestarts() and therefore
      // restart_offset_ wrapped around.
      size_ = 0;
    }
  }
}

InternalIterator* Block::NewIterator(const Comparator* cmp, BlockIter* iter,
                                     BlockType type) {
  if (size_ < 2*sizeof(uint32_t)) {
    if (iter != nullptr) {
      iter->SetStatus(Status::Corruption("bad block contents"));
      return iter;
    } else {
      return NewErrorInternalIterator(Status::Corruption("bad block contents"));
    }
  }
  const uint32_t num_restarts = NumRestarts();
  if (num_restarts == 0) {
    if (iter != nullptr) {
      iter->SetStatus(Status::OK());
      return iter;
    } else {
      return NewEmptyInternalIterator();
    }
  } else {
    if (iter != nullptr) {
      iter->Initialize(cmp, data_, restart_offset_, num_restarts);
    } else {
      switch (type) {
        case kTypeBlock:
          return new BlockIter(cmp, data_, restart_offset_, num_restarts);
        case kTypeMainColumn:
          return new MainColumnBlockIter(cmp, data_, restart_offset_,
                                         num_restarts);
        case kTypeSubColumn:
          return new SubColumnBlockIter(cmp, data_, restart_offset_,
                                        num_restarts);
        case kTypeMinMax:
          return new MinMaxBlockIter(cmp, data_, restart_offset_, num_restarts);
        default:
          return NewErrorInternalIterator(
              Status::Corruption("unknown block type"));
      }
    }
  }

  return iter;
}

size_t Block::ApproximateMemoryUsage() const {
  return usable_size();
}

}  // namespace vidardb
