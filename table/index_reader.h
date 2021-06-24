//  Copyright (c) 2021-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include "table/block.h"
#include "table/internal_iterator.h"

namespace vidardb {

namespace {

// Read the block identified by "handle" from "file".
// The only relevant option is options.verify_checksums for now.
// On failure return non-OK.
// On success fill *result and return OK - caller owns *result
// @param compression_dict Data for presetting the compression library's
//    dictionary.
Status ReadBlockFromFile(RandomAccessFileReader* file,
                         const ReadOptions& options, const BlockHandle& handle,
                         std::unique_ptr<Block>* result, Env* env,
                         bool do_uncompress, const Slice& compression_dict,
                         Logger* info_log, char** area = nullptr) {  // Shichao
  BlockContents contents;
  Status s =
      ReadBlockContents(file, options, handle, &contents, env, do_uncompress,
                        compression_dict, info_log, area);  // Shichao
  if (s.ok()) {
    result->reset(new Block(std::move(contents)));
  }

  return s;
}

}  // namespace

// -- IndexReader and its subclasses
// IndexReader is the interface that provide the functionality for index access.
class IndexReader {
 public:
  explicit IndexReader(const Comparator* comparator, Statistics* stats)
      : comparator_(comparator), statistics_(stats) {}

  virtual ~IndexReader() {}

  // Create an iterator for index access.
  // An iter is passed in, if it is not null, update this one and return it
  // If it is null, create a new Iterator
  virtual InternalIterator* NewIterator(BlockIter* iter = nullptr) = 0;

  // The size of the index.
  virtual size_t size() const = 0;
  // Memory usage of the index block
  virtual size_t usable_size() const = 0;
  // return the statistics pointer
  virtual Statistics* statistics() const { return statistics_; }
  // Report an approximation of how much memory has been used other than memory
  // that was allocated in block cache.
  virtual size_t ApproximateMemoryUsage() const = 0;

 protected:
  const Comparator* comparator_;

 private:
  Statistics* statistics_;
};

// Index that allows binary search lookup for the first key of each block.
// This class can be viewed as a thin wrapper for `Block` class which already
// supports binary search.
class BinarySearchIndexReader : public IndexReader {
 public:
  // Read index from the file and create an instance for
  // `BinarySearchIndexReader`.
  // On success, index_reader will be populated; otherwise it will remain
  // unmodified.
  static Status Create(RandomAccessFileReader* file,
                       const BlockHandle& index_handle, Env* env,
                       const Comparator* comparator, IndexReader** index_reader,
                       Statistics* statistics) {
    std::unique_ptr<Block> index_block;
    auto s = ReadBlockFromFile(file, ReadOptions(), index_handle, &index_block,
                               env, true /* decompress */,
                               Slice() /*compression dict*/,
                               /*info_log*/ nullptr);

    if (s.ok()) {
      *index_reader = new BinarySearchIndexReader(
          comparator, std::move(index_block), statistics);
    }

    return s;
  }

  virtual InternalIterator* NewIterator(BlockIter* iter = nullptr) override {
    return index_block_->NewIterator(comparator_, iter, Block::kTypeBlock);
  }

  virtual size_t size() const override { return index_block_->size(); }
  virtual size_t usable_size() const override {
    return index_block_->usable_size();
  }

  virtual size_t ApproximateMemoryUsage() const override {
    assert(index_block_);
    return index_block_->ApproximateMemoryUsage();
  }

 private:
  BinarySearchIndexReader(const Comparator* comparator,
                          std::unique_ptr<Block>&& index_block,
                          Statistics* stats)
      : IndexReader(comparator, stats), index_block_(std::move(index_block)) {
    assert(index_block_ != nullptr);
  }
  std::unique_ptr<Block> index_block_;
};

// Index that allows binary search lookup for the first key of each block.
// This class can be viewed as a thin wrapper for `Block` class which already
// supports binary search.
class MinMaxBinarySearchIndexReader : public IndexReader {
 public:
  // Read index from the file and create an instance for
  // `BinarySearchIndexReader`.
  // On success, index_reader will be populated; otherwise it will remain
  // unmodified.
  static Status Create(RandomAccessFileReader* file,
                       const BlockHandle& index_handle, Env* env,
                       const Comparator* comparator, IndexReader** index_reader,
                       Statistics* statistics,
                       const Comparator* value_comparator) {
    std::unique_ptr<Block> index_block;
    auto s = ReadBlockFromFile(file, ReadOptions(), index_handle, &index_block,
                               env, true /* decompress */,
                               Slice() /*compression dict*/,
                               /*info_log*/ nullptr);

    if (s.ok()) {
      *index_reader = new MinMaxBinarySearchIndexReader(
          comparator, std::move(index_block), statistics, value_comparator);
    }

    return s;
  }

  virtual InternalIterator* NewIterator(BlockIter* iter = nullptr) override {
    return index_block_->NewIterator(comparator_, iter, Block::kTypeMinMax);
  }

  virtual size_t size() const override { return index_block_->size(); }
  virtual size_t usable_size() const override {
    return index_block_->usable_size();
  }

  virtual size_t ApproximateMemoryUsage() const override {
    assert(index_block_);
    return index_block_->ApproximateMemoryUsage();
  }

 private:
  MinMaxBinarySearchIndexReader(const Comparator* comparator,
                                std::unique_ptr<Block>&& index_block,
                                Statistics* stats,
                                const Comparator* value_comparator)
      : IndexReader(comparator, stats),
        index_block_(std::move(index_block)),
        value_comparator_(value_comparator) {
    assert(index_block_ != nullptr);
  }

  std::unique_ptr<Block> index_block_;
  const Comparator* value_comparator_;
};
}  // namespace vidardb
