//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stddef.h>
#include <stdint.h>
#ifdef VIDARDB_MALLOC_USABLE_SIZE
#include <malloc.h>
#endif

#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"
#include "vidardb/iterator.h"
#include "vidardb/options.h"
#include "table/internal_iterator.h"

#include "format.h"

namespace vidardb {

struct BlockContents;
class Comparator;
class BlockIter;

class Block {
 public:
  // Initialize the block with the specified contents.
  explicit Block(BlockContents&& contents);

  ~Block() = default;

  size_t size() const { return size_; }

  const char* data() const { return data_; }

  bool cachable() const { return contents_.cachable; }

  size_t usable_size() const {
#ifdef VIDARDB_MALLOC_USABLE_SIZE
    if (contents_.allocation.get() != nullptr) {
      return malloc_usable_size(contents_.allocation.get());
    }
#endif  // VIDARDB_MALLOC_USABLE_SIZE
    return size_;
  }

  uint32_t NumRestarts() const;

  CompressionType compression_type() const {
    return contents_.compression_type;
  }

  enum BlockType : unsigned char {
    kTypeBlock = 0x0,
    kTypeMainColumn = 0x1,
    kTypeSubColumn =0x2,
    kTypeMinMax = 0x3
  };

  // If iter is null, return new Iterator
  // If iter is not null, update this one and return it as Iterator*
  InternalIterator* NewIterator(const Comparator* comparator,
                                BlockIter* iter = nullptr,
                                BlockType type = kTypeBlock);

  // Report an approximation of how much memory has been used.
  size_t ApproximateMemoryUsage() const;

 protected:
  BlockContents contents_;
  const char* data_;            // contents_.data.data()
  size_t size_;                 // contents_.data.size()
  uint32_t restart_offset_;     // Offset in data_ of restart array

  // No copying allowed
  Block(const Block&);
  void operator=(const Block&);
};

class BlockIter : public InternalIterator {
 public:
  BlockIter();

  BlockIter(const Comparator* comparator, const char* data, uint32_t restarts,
            uint32_t num_restarts);

  void Initialize(const Comparator* comparator, const char* data,
                  uint32_t restarts, uint32_t num_restarts);

  void SetStatus(Status s) { status_ = s; }

  virtual bool Valid() const override final { return current_ < restarts_; }

  virtual Status status() const override final { return status_; }

  virtual Slice key() const override final {
    assert(Valid());
    return key_.GetKey();
  }

  virtual Slice value() override final {
    assert(Valid());
    return value_;
  }

  virtual void Next() override {
    assert(Valid());
    ParseNextKey();
  }

  virtual void Prev() override;

  virtual void Seek(const Slice& target) override;

  virtual void SeekToFirst() override;

  virtual void SeekToLast() override;

#ifndef NDEBUG
  virtual ~BlockIter();

  virtual void SetPinnedItersMgr(
      PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
  }

  PinnedIteratorsManager* pinned_iters_mgr_ = nullptr;
#endif

  virtual bool IsKeyPinned() const override { return key_.IsKeyPinned(); }

 protected:
  const Comparator* comparator_;
  const char* data_;       // underlying block contents
  uint32_t restarts_;      // Offset of restart array (list of fixed32)
  uint32_t num_restarts_;  // Number of uint32_t entries in restart array

  // current_ is offset in data_ of current entry.  >= restarts_ if !Valid
  uint32_t current_;
  uint32_t restart_index_;  // Index of restart block in which current_ falls
  IterKey key_;
  Slice value_;
  Status status_;

  int Compare(const Slice& a, const Slice& b) const {
    return comparator_->Compare(a, b);
  }

  // Return the offset in data_ just past the end of the current entry.
  virtual uint32_t NextEntryOffset() const {
    // NOTE: We don't support files bigger than 2GB
    return static_cast<uint32_t>((value_.data() + value_.size()) - data_);
  }

  uint32_t GetRestartPoint(uint32_t index) {
    assert(index < num_restarts_);
    return DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
  }

  virtual void SeekToRestartPoint(uint32_t index) {
    key_.Clear();
    restart_index_ = index;
    // current_ will be fixed by ParseNextKey();

    // ParseNextKey() starts at the end of value_, so set value_ accordingly
    uint32_t offset = GetRestartPoint(index);
    value_ = Slice(data_ + offset, 0);
  }

  virtual void CorruptionError();

  virtual bool ParseNextKey() {
    current_ = NextEntryOffset();
    const char* p = data_ + current_;
    const char* limit = data_ + restarts_;  // Restarts come right after data
    if (p >= limit) {
      // No more entries to return.  Mark as invalid.
      current_ = restarts_;
      restart_index_ = num_restarts_;
      return false;
    }

    // Decode next entry
    uint32_t shared, non_shared, value_length;
    p = DecodeEntry(p, limit, &shared, &non_shared, &value_length);
    if (p == nullptr || key_.Size() < shared) {
      CorruptionError();
      return false;
    } else {
      if (shared == 0) {
        // If this key don't share any bytes with prev key then we don't need
        // to decode it and can use it's address in the block directly.
        key_.SetKey(Slice(p, non_shared), false /* copy */);
      } else {
        // This key share `shared` bytes with prev key, we need to decode it
        key_.TrimAppend(shared, p, non_shared);
      }

      value_ = Slice(p + non_shared, value_length);
      while (restart_index_ + 1 < num_restarts_ &&
             GetRestartPoint(restart_index_ + 1) < current_) {
        ++restart_index_;
      }
      return true;
    }
  }

  virtual bool BinarySeek(const Slice& target, uint32_t left, uint32_t right,
                          uint32_t* index);

  // Helper routine: decode the next block entry starting at "p",
  // storing the number of shared key bytes, non_shared key bytes,
  // and the length of the value in "*shared", "*non_shared", and
  // "*value_length", respectively.  Will not derefence past "limit".
  //
  // If any errors are detected, returns nullptr.  Otherwise, returns a
  // pointer to the key delta (just past the three decoded values).
  static const char* DecodeEntry(const char* p, const char* limit,
                                 uint32_t* shared, uint32_t* non_shared,
                                 uint32_t* value_length) {
    if (limit - p < 3) return nullptr;
    *shared = reinterpret_cast<const unsigned char*>(p)[0];
    *non_shared = reinterpret_cast<const unsigned char*>(p)[1];
    *value_length = reinterpret_cast<const unsigned char*>(p)[2];
    if ((*shared | *non_shared | *value_length) < 128) {
      // Fast path: all three values are encoded in one byte each
      p += 3;
    } else {
      if ((p = GetVarint32Ptr(p, limit, shared)) == nullptr) return nullptr;
      if ((p = GetVarint32Ptr(p, limit, non_shared)) == nullptr) return nullptr;
      if ((p = GetVarint32Ptr(p, limit, value_length)) == nullptr)
        return nullptr;
    }

    if (static_cast<uint32_t>(limit - p) < (*non_shared + *value_length)) {
      return nullptr;
    }
    return p;
  }
};

// Sub-column block iterator, used in sub columns' data block
class SubColumnBlockIter final : public BlockIter {
 public:
  SubColumnBlockIter() : BlockIter() {}
  SubColumnBlockIter(const Comparator* comparator, const char* data,
                     uint32_t restarts, uint32_t num_restarts);

  virtual void Seek(const Slice& target) override;

  // Helper routine: decode the next block entry starting at "p",
  // storing the number of the length of the key or value in "key_length"
  // or "*value_length". Will not derefence past "limit".
  //
  // If any errors are detected, returns nullptr. Otherwise, returns a
  // pointer to the key delta (just past the decoded values).
  static const char* DecodeKeyOrValue(const char* p, const char* limit,
                                      uint32_t* length) {
    if (limit - p < 1) return nullptr;
    *length = reinterpret_cast<const unsigned char*>(p)[0];
    if (*length < 128) {
      // Fast path: key_length is encoded in one byte each
      p++;
    } else {
      if ((p = GetVarint32Ptr(p, limit, length)) == nullptr) return nullptr;
    }

    if (static_cast<uint32_t>(limit - p) < *length) {
      return nullptr;
    }
    return p;
  }

 private:
  virtual bool ParseNextKey() override {
    current_ = NextEntryOffset();
    const char* p = data_ + current_;
    const char* limit = data_ + restarts_;  // Restarts come right after data
    if (p >= limit) {
      // No more entries to return.  Mark as invalid.
      current_ = restarts_;
      restart_index_ = num_restarts_;
      return false;
    }

    while (restart_index_ + 1 < num_restarts_ &&
           GetRestartPoint(restart_index_ + 1) <= current_) {
      ++restart_index_;
    }

    uint32_t restart_offset = GetRestartPoint(restart_index_);
    // within the restart area, key is not stored because it is merely sequence
    bool has_key = (restart_offset == current_);

    // Decode next entry
    uint32_t key_length = 0;
    if (has_key) {
      p = DecodeKeyOrValue(p, limit, &key_length);
      if (p == nullptr) {
        CorruptionError();
        return false;
      }
      key_.SetKey(Slice(p, key_length), false /* copy */);
    }
    p += key_length;
    uint32_t value_length = 0;
    p = DecodeKeyOrValue(p, limit, &value_length);
    if (p == nullptr) {
      CorruptionError();
      return false;
    }

    value_ = Slice(p, value_length);
    return true;
  }

  virtual bool BinarySeek(const Slice& target, uint32_t left, uint32_t right,
                          uint32_t* index) override;
};

// Main column block iterator, used in main columns' data block
class MainColumnBlockIter final : public BlockIter {
 public:
  MainColumnBlockIter() : BlockIter(), has_val_(false), int_val_(0) {}
  MainColumnBlockIter(const Comparator* comparator, const char* data,
                      uint32_t restarts, uint32_t num_restarts);

 private:
  // Return the offset in data_ just past the end of the current entry.
  virtual uint32_t NextEntryOffset() const override {
    // NOTE: We don't support files bigger than 2GB
    return static_cast<uint32_t>(key_.GetKey().data() + key_.Size() - data_ +
                                 (has_val_ ? 4 : 0));
  }

  virtual void SeekToRestartPoint(uint32_t index) override {
    restart_index_ = index;
    // current_ will be fixed by ParseNextKey();

    // ParseNextKey() starts at the end of value_ or key_
    uint32_t offset = GetRestartPoint(index);
    value_ = Slice(data_ + offset, 0);
    key_.SetKey(value_, false);

    has_val_ = false;
    int_val_ = 0;
    str_val_.empty();
  }

  virtual void CorruptionError() override;

  virtual bool ParseNextKey() override {
    current_ = NextEntryOffset();
    const char* p = data_ + current_;
    const char* limit = data_ + restarts_;  // Restarts come right after data
    if (p >= limit) {
      // No more entries to return.  Mark as invalid.
      current_ = restarts_;
      restart_index_ = num_restarts_;
      return false;
    }

    // Decode next entry
    uint32_t key_length = 0;
    p = SubColumnBlockIter::DecodeKeyOrValue(p, limit, &key_length);
    if (p == nullptr) {
      CorruptionError();
      return false;
    }
    key_.SetKey(Slice(p, key_length), false /* copy */);

    while (restart_index_ + 1 < num_restarts_ &&
           GetRestartPoint(restart_index_ + 1) <= current_) {
      ++restart_index_;
    }

    uint32_t restart_offset = GetRestartPoint(restart_index_);
    // within the restart area, val is not stored because it is merely sequence
    has_val_ = (restart_offset == current_);

    uint32_t value_length = 4;  // fixed 32 bits
    if (has_val_) {
      value_ = Slice(p + key_length, value_length);
      GetFixed32BigEndian(&value_, &int_val_);
    } else {
      PutFixed32BigEndian(&str_val_, ++int_val_);
      value_ = Slice(str_val_);
    }

    return true;
  }

  virtual bool BinarySeek(const Slice& target, uint32_t left, uint32_t right,
                          uint32_t* index) override;

  bool has_val_;
  uint32_t int_val_;     // integer representation of sequence value
  std::string str_val_;  // big endian representation of sequence value
};

// Min max block iterator, used in sub columns' index block
class MinMaxBlockIter final : public BlockIter {
 public:
  MinMaxBlockIter() : BlockIter(), max_storage_len_(0) {}
  MinMaxBlockIter(const Comparator* comparator, const char* data,
                  uint32_t restarts, uint32_t num_restarts);

  virtual Slice min() const override {
    assert(Valid());
    return min_;
  }

  virtual Slice max() const override {
    assert(Valid());
    return max_;
  }

 private:
  // Return the offset in data_ just past the end of the current entry.
  virtual uint32_t NextEntryOffset() const override {
    // NOTE: We don't support files bigger than 2GB
    return static_cast<uint32_t>(min_.data() + min_.size() + max_storage_len_ -
                                 data_);
  }

  virtual void SeekToRestartPoint(uint32_t index) override {
    key_.Clear();
    value_.clear();
    max_.clear();
    restart_index_ = index;
    max_storage_len_ = 0;
    // current_ will be fixed by ParseNextKey();

    // ParseNextKey() starts at the end of max_, but max is not continuously
    // stored, so set min_accordingly
    uint32_t offset = GetRestartPoint(index);
    min_ = Slice(data_ + offset, 0);
  }

  virtual void CorruptionError() override;

  virtual bool ParseNextKey() override {
    bool ret = BlockIter::ParseNextKey();
    if (!ret) {
      return false;
    }

    const char* p = value_.data_ + value_.size_;
    const char* limit = data_ + restarts_;  // Restarts come right after data
    // Decode min
    uint32_t shared, non_shared;
    p = SubColumnBlockIter::DecodeKeyOrValue(p, limit, &non_shared);
    if (p == nullptr) {
      CorruptionError();
      return false;
    }
    min_ = Slice(p, non_shared);
    p += non_shared;
    const char* max_start = p;

    // Decode max
    p = DecodeMax(p, limit, &shared, &non_shared);
    if (p == nullptr || min_.size() < shared) {
      CorruptionError();
      return false;
    }
    max_.clear();
    max_.assign(min_.data(), shared);
    max_.append(p, non_shared);

    p += non_shared;
    max_storage_len_ = p - max_start;
    return true;
  }

  // Helper routine: decode the next block max starting at "p",
  // storing the number of shared bytes, non_shared bytes, in "*shared",
  // "*non_shared" respectively.  Will not derefence past "limit".
  //
  // If any errors are detected, returns nullptr.  Otherwise, returns a
  // pointer to the key delta (just past the three decoded values).
  static const char* DecodeMax(const char* p, const char* limit,
                               uint32_t* shared, uint32_t* non_shared) {
    if (limit - p < 2) return nullptr;
    *shared = reinterpret_cast<const unsigned char*>(p)[0];
    *non_shared = reinterpret_cast<const unsigned char*>(p)[1];
    if ((*shared | *non_shared) < 128) {
      // Fast path: all three values are encoded in one byte each
      p += 2;
    } else {
      if ((p = GetVarint32Ptr(p, limit, shared)) == nullptr) return nullptr;
      if ((p = GetVarint32Ptr(p, limit, non_shared)) == nullptr) return nullptr;
    }

    if (static_cast<uint32_t>(limit - p) < *non_shared) {
      return nullptr;
    }
    return p;
  }

  Slice min_;
  std::string max_;
  uint32_t max_storage_len_;
};

}  // namespace vidardb
