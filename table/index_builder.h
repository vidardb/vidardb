//  Copyright (c) 2021-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include "table/min_max_block_builder.h"
#include "table/format.h"
#include "vidardb/comparator.h"

namespace vidardb {

// The interface for building index.
// Instruction for adding a new concrete IndexBuilder:
//  1. Create a subclass instantiated from IndexBuilder.
//  2. Add a new entry associated with that subclass in TableOptions::IndexType.
//  3. Add a create function for the new subclass in CreateIndexBuilder.
// Note: we can devise more advanced design to simplify the process for adding
// new subclass, which will, on the other hand, increase the code complexity and
// catch unwanted attention from readers. Given that we won't add/change
// indexes frequently, it makes sense to just embrace a more straightforward
// design that just works.
class IndexBuilder {
 public:
  // Index builder will construct a set of blocks which contain:
  //  1. One primary index block.
  struct IndexBlocks {
    Slice index_block_contents;
  };
  explicit IndexBuilder(const Comparator* comparator)
      : comparator_(comparator) {}

  virtual ~IndexBuilder() {}

  // Add a new index entry to index block.
  // To allow further optimization, we provide `last_key_in_current_block` and
  // `first_key_in_next_block`, based on which the specific implementation can
  // determine the best index key to be used for the index block.
  // @last_key_in_current_block: this parameter maybe overridden with the value
  //                             "substitute key".
  // @first_key_in_next_block: it will be nullptr if the entry being added is
  //                           the last one in the table
  //
  // REQUIRES: Finish() has not yet been called.
  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) = 0;

  // This method will be called whenever a key is added. The subclasses may
  // override OnKeyAdded() if they need to collect additional information.
  virtual void OnKeyAdded(const Slice& key) {}

  // Inform the index builder that all entries has been written. Block builder
  // may therefore perform any operation required for block finalization.
  //
  // REQUIRES: Finish() has not yet been called.
  virtual Status Finish(IndexBlocks* index_blocks) = 0;

  // Get the estimated size for index block.
  virtual size_t EstimatedSize() const = 0;

 protected:
  const Comparator* comparator_;
};

// This index builder builds space-efficient index block.
//
// Optimizations:
//  1. Made block's `block_restart_interval` to be 1, which will avoid linear
//     search when doing index lookup (can be disabled by setting
//     index_block_restart_interval).
//  2. Shorten the key length for index block. Other than honestly using the
//     last key in the data block as the index key, we instead find a shortest
//     substitute key that serves the same function.
class ShortenedIndexBuilder : public IndexBuilder {
 public:
  explicit ShortenedIndexBuilder(const Comparator* comparator,
                                 int index_block_restart_interval)
      : IndexBuilder(comparator),
        index_block_builder_(index_block_restart_interval) {}

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) override {
    if (first_key_in_next_block != nullptr) {
      comparator_->FindShortestSeparator(last_key_in_current_block,
                                         *first_key_in_next_block);
    } else {
      comparator_->FindShortSuccessor(last_key_in_current_block);
    }

    std::string handle_encoding;
    block_handle.EncodeTo(&handle_encoding);
    index_block_builder_.Add(*last_key_in_current_block, handle_encoding);
  }

  virtual Status Finish(IndexBlocks* index_blocks) override {
    index_blocks->index_block_contents = index_block_builder_.Finish();
    return Status::OK();
  }

  virtual size_t EstimatedSize() const override {
    return index_block_builder_.CurrentSizeEstimate();
  }

 private:
  BlockBuilder index_block_builder_;
};

// This index builder builds space-efficient index block with min & max values.
//
// Optimizations:
//  1. Made block's `block_restart_interval` to be 1, which will avoid linear
//     search when doing index lookup (can be disabled by setting
//     index_block_restart_interval).
//  2. Shorten the key length for index block. Other than honestly using the
//     last key in the data block as the index key, we instead find a shortest
//     substitute key that serves the same function.
class MinMaxShortenedIndexBuilder : public IndexBuilder {
 public:
  explicit MinMaxShortenedIndexBuilder(const Comparator* comparator,
                                       int index_block_restart_interval,
                                       const Comparator* value_comparator)
      : IndexBuilder(comparator),
        index_block_builder_(index_block_restart_interval),
        value_comparator_(value_comparator) {}

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) override {
    if (first_key_in_next_block != nullptr) {
      comparator_->FindShortestSeparator(last_key_in_current_block,
                                         *first_key_in_next_block);
    } else {
      comparator_->FindShortSuccessor(last_key_in_current_block);
    }

    std::string handle_encoding;
    block_handle.EncodeTo(&handle_encoding);

    index_block_builder_.Add(*last_key_in_current_block, handle_encoding,
                             min_block_value_, max_block_value_);
    min_block_value_.clear();
    max_block_value_.clear();
  }

  virtual void OnKeyAdded(const Slice& value) override {
    if (min_block_value_.empty() ||
        value_comparator_->Compare(value, min_block_value_) < 0) {
      min_block_value_.assign(value.data(), value.size());
    }
    if (max_block_value_.empty() ||
        value_comparator_->Compare(value, max_block_value_) > 0) {
      max_block_value_.assign(value.data(), value.size());
    }
  }

  virtual Status Finish(IndexBlocks* index_blocks) override {
    index_blocks->index_block_contents = index_block_builder_.Finish();
    return Status::OK();
  }

  virtual size_t EstimatedSize() const override {
    return index_block_builder_.CurrentSizeEstimate();
  }

 private:
  MinMaxBlockBuilder index_block_builder_;
  std::string min_block_value_;
  std::string max_block_value_;
  const Comparator* value_comparator_;
};

}  // namespace vidardb
