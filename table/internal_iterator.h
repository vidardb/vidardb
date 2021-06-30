//  Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

#pragma once

#include <string>

#include "vidardb/file_iter.h"  // Shichao
#include "vidardb/iterator.h"
#include "vidardb/status.h"

namespace vidardb {

class PinnedIteratorsManager;

/*********************** Shichao **************************/
struct RangeQueryKeyVal;
struct MinMax;
/*********************** Shichao **************************/

class InternalIterator : public Cleanable {
 public:
  InternalIterator() {}
  virtual ~InternalIterator() {}

  // An iterator is either positioned at a key/value pair, or
  // not valid.  This method returns true iff the iterator is valid.
  virtual bool Valid() const { return false; }

  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  virtual void SeekToFirst() { return; }

  // Position at the last key in the source.  The iterator is
  // Valid() after this call iff the source is not empty.
  virtual void SeekToLast() { return; }

  // Position at the first key in the source that at or past target
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  virtual void Seek(const Slice& target) { return; }

  // Moves to the next entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual void Next() { return; }

  // Moves to the previous entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the first entry in source.
  // REQUIRES: Valid()
  virtual void Prev() { return; }

  // Return the key for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual Slice key() const { return Slice(); }

  // Return the value for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: !AtEnd() && !AtStart()
  virtual Slice value() { return Slice(); }

  // If an error has occurred, return it.  Else return an ok status.
  // If non-blocking IO is requested and this operation cannot be
  // satisfied without doing some IO, then this returns Status::Incomplete().
  virtual Status status() const { return Status(); }

  /***************************** Shichao ******************************/
  // Implemented in MinMaxBlock iterator
  virtual Slice min() const { return Slice(); }

  // Implemented in MinMaxBlock iterator
  virtual Slice max() const { return Slice(); }

  // See comments in file_iter.h
  virtual Status GetMinMax(std::vector<std::vector<MinMax>>& v) const {
    return Status::NotSupported(Slice("GetMinMax is not implemented"));
  }

  // See comments in file_iter.h
  virtual Status RangeQuery(const std::vector<bool>& block_bits, char* buf,
                            uint64_t capacity, uint64_t* valid_count,
                            uint64_t* total_count) const {
    return Status::NotSupported(Slice("RangeQuery is not implemented"));
  }
  /***************************** Shichao ******************************/

  // Pass the PinnedIteratorsManager to the Iterator, most Iterators dont
  // communicate with PinnedIteratorsManager so default implementation is no-op
  // but for Iterators that need to communicate with PinnedIteratorsManager
  // they will implement this function and use the passed pointer to communicate
  // with PinnedIteratorsManager.
  virtual void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) {}

  // If true, this means that the Slice returned by key() is valid as long as
  // PinnedIteratorsManager::ReleasePinnedIterators is not called and the
  // Iterator is not deleted.
  //
  // IsKeyPinned() is guaranteed to always return true if
  //  - Iterator is created with ReadOptions::pin_data = true
  //  - DB tables were created with BlockBasedTableOptions::use_delta_encoding
  //    set to false.
  virtual bool IsKeyPinned() const { return false; }

  virtual Status GetProperty(std::string prop_name, std::string* prop) {
    return Status::NotSupported("");
  }

 private:
  // No copying allowed
  InternalIterator(const InternalIterator&) = delete;
  InternalIterator& operator=(const InternalIterator&) = delete;
};

// Return an empty iterator (yields nothing).
extern InternalIterator* NewEmptyInternalIterator();

// Return an empty iterator with the specified status.
extern InternalIterator* NewErrorInternalIterator(const Status& status);

}  // namespace vidardb
