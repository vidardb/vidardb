//  Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
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

#pragma once
#include <stdio.h>
#include <string>
#include <unordered_map>  // Shichao
#include "vidardb/comparator.h"
#include "vidardb/db.h"
#include "vidardb/slice.h"
#include "vidardb/table.h"
#include "vidardb/types.h"
#include "util/coding.h"
#include "util/logging.h"

namespace vidardb {

class InternalKey;
class SuperVersion;
class ColumnFamilyData;

// Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
// data structures.
// The highest bit of the value type needs to be reserved to SST tables
// for them to do more flexible encoding.
enum ValueType : unsigned char {
  kTypeDeletion = 0x0,
  kTypeValue = 0x1,
  kTypeLogData = 0x3,               // WAL only.
  kTypeColumnFamilyDeletion = 0x4,  // WAL only.
  kTypeColumnFamilyValue = 0x5,     // WAL only.
  kTypeSingleDeletion = 0x7,
  kTypeBeginPrepareXID = 0x9,             // WAL only.
  kTypeEndPrepareXID = 0xA,               // WAL only.
  kTypeCommitXID = 0xB,                   // WAL only.
  kTypeRollbackXID = 0xC,                 // WAL only.
  kTypeNoop = 0xD,                        // WAL only.
  kMaxValue = 0x7F                        // Not used for storing records.
};

// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
static const ValueType kValueTypeForSeek = kTypeSingleDeletion;

// Checks whether a type is a value type (i.e. a type used in memtables and sst
// files).
inline bool IsValueType(ValueType t) {
  return t < kTypeLogData || t == kTypeSingleDeletion;
}

// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
static const SequenceNumber kMaxSequenceNumber =
  ((0x1ull << 56) - 1);

struct ParsedInternalKey {
  Slice user_key;
  SequenceNumber sequence;
  ValueType type;

  ParsedInternalKey() { }  // Intentionally left uninitialized (for speed)
  ParsedInternalKey(const Slice& u, const SequenceNumber& seq, ValueType t)
    : user_key(u), sequence(seq), type(t) { }
  std::string DebugString(bool hex = false) const;
};

// Return the length of the encoding of "key".
inline size_t InternalKeyEncodingLength(const ParsedInternalKey& key) {
  return key.user_key.size() + 8;
}

// Pack a sequence number and a ValueType into a uint64_t
extern uint64_t PackSequenceAndType(uint64_t seq, ValueType t);

// Given the result of PackSequenceAndType, store the sequence number in *seq
// and the ValueType in *t.
extern void UnPackSequenceAndType(uint64_t packed, uint64_t* seq, ValueType* t);

// Append the serialization of "key" to *result.
extern void AppendInternalKey(std::string* result,
                              const ParsedInternalKey& key);

// Attempt to parse an internal key from "internal_key".  On success,
// stores the parsed data in "*result", and returns true.
//
// On error, returns false, leaves "*result" in an undefined state.
extern bool ParseInternalKey(const Slice& internal_key,
                             ParsedInternalKey* result);

// Returns the user key portion of an internal key.
inline Slice ExtractUserKey(const Slice& internal_key) {
  assert(internal_key.size() >= 8);
  return Slice(internal_key.data(), internal_key.size() - 8);
}

inline ValueType ExtractValueType(const Slice& internal_key) {
  assert(internal_key.size() >= 8);
  const size_t n = internal_key.size();
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  unsigned char c = num & 0xff;
  return static_cast<ValueType>(c);
}

// A comparator for internal keys that uses a specified comparator for
// the user key portion and breaks ties by decreasing sequence number.
class InternalKeyComparator : public Comparator {
 protected:
  const Comparator* user_comparator_;
  std::string name_;
 public:
  explicit InternalKeyComparator(const Comparator* c) : user_comparator_(c),
    name_("vidardb.InternalKeyComparator:" +
          (c != nullptr ? std::string(user_comparator_->Name()): "")) {
  }
  virtual ~InternalKeyComparator() {}

  virtual const char* Name() const override;
  virtual int Compare(const Slice& a, const Slice& b) const override;
  virtual void FindShortestSeparator(std::string* start,
                                     const Slice& limit) const override;
  virtual void FindShortSuccessor(std::string* key) const override;

  const Comparator* user_comparator() const {
    return user_comparator_;
  }

  int Compare(const InternalKey& a, const InternalKey& b) const;
  int Compare(const ParsedInternalKey& a, const ParsedInternalKey& b) const;
};

// A comparator for sub column key.
class ColumnKeyComparator : public InternalKeyComparator {
 private:
 public:
  explicit ColumnKeyComparator() :
    InternalKeyComparator(nullptr) {
    name_ = "vidardb.ColumnKeyComparator";
  }
  virtual ~ColumnKeyComparator() {}

  virtual int Compare(const Slice& a, const Slice& b) const override {
    assert(a.size() == b.size());
    return a.compare(b);
  }
  virtual void FindShortestSeparator(std::string* start,
                                     const Slice& limit) const override {}
  virtual void FindShortSuccessor(std::string* key) const override {}

  const Comparator* user_comparator() const {
    assert(false);
    return nullptr;
  }

  int Compare(const InternalKey& a, const InternalKey& b) const {
    assert(false);
    return -1;
  }
  int Compare(const ParsedInternalKey& a, const ParsedInternalKey& b) const {
    assert(false);
    return -1;
  }
};

// Modules in this directory should keep internal keys wrapped inside
// the following class instead of plain strings so that we do not
// incorrectly use string comparisons instead of an InternalKeyComparator.
class InternalKey {
 private:
  std::string rep_;
 public:
  InternalKey() { }   // Leave rep_ as empty to indicate it is invalid
  InternalKey(const Slice& _user_key, SequenceNumber s, ValueType t) {
    AppendInternalKey(&rep_, ParsedInternalKey(_user_key, s, t));
  }

  // sets the internal key to be bigger or equal to all internal keys with this
  // user key
  void SetMaxPossibleForUserKey(const Slice& _user_key) {
    AppendInternalKey(&rep_, ParsedInternalKey(_user_key, kMaxSequenceNumber,
                      kValueTypeForSeek));
  }

  // sets the internal key to be smaller or equal to all internal keys with this
  // user key
  void SetMinPossibleForUserKey(const Slice& _user_key) {
    AppendInternalKey(
      &rep_, ParsedInternalKey(_user_key, 0, static_cast<ValueType>(0)));
  }

  bool Valid() const {
    ParsedInternalKey parsed;
    return ParseInternalKey(Slice(rep_), &parsed);
  }

  void DecodeFrom(const Slice& s) {
    rep_.assign(s.data(), s.size());
  }
  Slice Encode() const {
    assert(!rep_.empty());
    return rep_;
  }

  Slice user_key() const {
    return ExtractUserKey(rep_);
  }
  size_t size() {
    return rep_.size();
  }

  void SetFrom(const ParsedInternalKey& p) {
    rep_.clear();
    AppendInternalKey(&rep_, p);
  }

  void Clear() {
    rep_.clear();
  }

  std::string DebugString(bool hex = false) const;
};

inline int InternalKeyComparator::Compare(
  const InternalKey& a, const InternalKey& b) const {
  return Compare(a.Encode(), b.Encode());
}

inline bool ParseInternalKey(const Slice& internal_key,
                             ParsedInternalKey* result) {
  const size_t n = internal_key.size();
  if (n < 8) return false;
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  unsigned char c = num & 0xff;
  result->sequence = num >> 8;
  result->type = static_cast<ValueType>(c);
  assert(result->type <= ValueType::kMaxValue);
  result->user_key = Slice(internal_key.data(), n - 8);
  return IsValueType(result->type);
}

// Update the sequence number in the internal key.
// Guarantees not to invalidate ikey.data().
inline void UpdateInternalKey(std::string* ikey, uint64_t seq, ValueType t) {
  size_t ikey_sz = ikey->size();
  assert(ikey_sz >= 8);
  uint64_t newval = (seq << 8) | t;

  // Note: Since C++11, strings are guaranteed to be stored contiguously and
  // string::operator[]() is guaranteed not to change ikey.data().
  EncodeFixed64(&(*ikey)[ikey_sz - 8], newval);
}

// Get the sequence number from the internal key
inline uint64_t GetInternalKeySeqno(const Slice& internal_key) {
  const size_t n = internal_key.size();
  assert(n >= 8);
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  return num >> 8;
}


// A helper class useful for DBImpl::Get()
class LookupKey {
 public:
  // Initialize *this for looking up user_key at a snapshot with
  // the specified sequence number.
  LookupKey(const Slice& _user_key, SequenceNumber sequence);

  ~LookupKey();

  // Return a key suitable for lookup in a MemTable.
  Slice memtable_key() const {
    return Slice(start_, static_cast<size_t>(end_ - start_));
  }

  // Return an internal key (suitable for passing to an internal iterator)
  Slice internal_key() const {
    return Slice(kstart_, static_cast<size_t>(end_ - kstart_));
  }

  // Return the user key
  Slice user_key() const {
    return Slice(kstart_, static_cast<size_t>(end_ - kstart_ - 8));
  }

 private:
  // We construct a char array of the form:
  //    klength  varint32               <-- start_
  //    userkey  char[klength]          <-- kstart_
  //    tag      uint64
  //                                    <-- end_
  // The array is a suitable MemTable key.
  // The suffix starting with "userkey" can be used as an InternalKey.
  const char* start_;
  const char* kstart_;
  const char* end_;
  char space_[200];      // Avoid allocation for short keys

  // No copying allowed
  LookupKey(const LookupKey&);
  void operator=(const LookupKey&);
};

inline LookupKey::~LookupKey() {
  if (start_ != space_) delete[] start_;
}

/**************** Shichao ******************/
// A range of LookupKeys
struct LookupRange {
  LookupKey* start_;          // Included in the range
  LookupKey* limit_;          // Included in the range

  LookupRange(): start_(NULL), limit_(NULL) { }
  LookupRange(LookupKey* start, LookupKey* limit) :
    start_(start), limit_(limit) { }
  SequenceNumber SequenceNum() const {
    assert(start_ != NULL);
    const Slice& ikey = start_->internal_key();
    uint64_t num = DecodeFixed64(ikey.data() + ikey.size() - 8);
    return num >> 8;
  }
};

struct SeqTypeVal {
  SequenceNumber seq_;
  ValueType type_;
  typedef std::list<RangeQueryKeyVal>::iterator ListIterator;
  ListIterator iter_;

  SeqTypeVal() : seq_(0), type_(kTypeDeletion) { }

  SeqTypeVal(SequenceNumber seq, ValueType type, const ListIterator& iter) :
    seq_(seq), type_(type), iter_(iter) { }

  SeqTypeVal(SequenceNumber seq, ValueType type, ListIterator&& iter) :
    seq_(seq), type_(type), iter_(std::move(iter)) { }

  SeqTypeVal(const SeqTypeVal& stv) :
    seq_(stv.seq_), type_(stv.type_), iter_(stv.iter_) { }

  SeqTypeVal(SeqTypeVal&& stv) :
    seq_(stv.seq_), type_(stv.type_), iter_(std::move(stv.iter_)) { }

  SeqTypeVal& operator=(const SeqTypeVal& stv) {
    seq_ = stv.seq_;
    type_ = stv.type_;
    iter_ = stv.iter_;
    return *this;
  }

  SeqTypeVal& operator=(SeqTypeVal&& stv) {
    seq_ = stv.seq_;
    type_ = stv.type_;
    iter_ = std::move(stv.iter_);
    return *this;
  }
};

/**************** Shichao ******************/

/**************************** Quanzhao *****************************/
struct RangeQueryMeta {
  ColumnFamilyData* column_family_data;      // Column family data
  SuperVersion* super_version;               // Super version
  SequenceNumber snapshot;                   // Current snapshot
  LookupKey* current_limit_key;              // Current limit key
  SequenceNumber limit_sequence;             // Limit sequence
  std::string next_start_key;                // Next start key
  std::map<std::string, SeqTypeVal> map_res; // Temp result map
  std::unordered_map<SequenceNumber,
      std::list<RangeQueryKeyVal>::iterator> del_keys;  // store delete keys

  RangeQueryMeta(ColumnFamilyData* cfd, SuperVersion* sv, SequenceNumber snap,
                 LookupKey* limit_key = nullptr, SequenceNumber limit_seq = 0):
    column_family_data(cfd), super_version(sv), snapshot(snap),
    current_limit_key(limit_key), limit_sequence(limit_seq) {}
};

// Ensure the result size is no more than the expected capacity
// which maybe include an extra limit user key.
// Return true if the size has reached the capacity, else false.
inline bool CompressResultList(std::list<RangeQueryKeyVal>* res,
                               ReadOptions& read_options) {
  if (read_options.batch_capacity <= 0) {  // infinite
    return false;
  }

  // reserve the next start key which is also the current limit key
  RangeQueryMeta* meta =
    static_cast<RangeQueryMeta*>(read_options.range_query_meta);
  size_t ok_size = read_options.batch_capacity + 1;
  if (meta->map_res.size() <= ok_size) {
    return false;
  }

  size_t diff_size = meta->map_res.size() - ok_size;
  for (size_t i = 0u; i < diff_size; i++) {
    auto it = --(meta->map_res.end());
    size_t delta_size = it->second.iter_->user_key.size() +
                        it->second.iter_->user_val.size();
    res->erase(it->second.iter_);  // remove from list
    assert(read_options.result_size >= delta_size);
    read_options.result_size -= delta_size;

    if (it->second.type_ == kTypeDeletion) {
      // remove from unordered_map
      meta->del_keys.erase(it->second.seq_);
    }
    meta->map_res.erase(it);       // remove from map
  }

  // update the current range limit key
  delete meta->current_limit_key;
  Slice limit_key(meta->map_res.rbegin()->first);
  meta->current_limit_key = new LookupKey(limit_key, meta->limit_sequence);
  return true;
}

inline int CompareRangeLimit(const InternalKeyComparator& comparator,
                             const Slice& internal_key,
                             const LookupKey* limit) {
  if (limit->user_key().compare(kRangeQueryMax) == 0) {
    return -1;
  }

  return comparator.Compare(internal_key, limit->internal_key());
}
/**************************** Quanzhao *****************************/

class IterKey {
 public:
  IterKey()
    : buf_(space_), buf_size_(sizeof(space_)), key_(buf_), key_size_(0) {}

  ~IterKey() {
    ResetBuffer();
  }

  Slice GetKey() const {
    return Slice(key_, key_size_);
  }

  Slice GetUserKey() const {
    assert(key_size_ >= 8);
    return Slice(key_, key_size_ - 8);
  }

  size_t Size() const {
    return key_size_;
  }

  void Clear() {
    key_size_ = 0;
  }

  // Append "non_shared_data" to its back, from "shared_len"
  // This function is used in Block::Iter::ParseNextKey
  // shared_len: bytes in [0, shard_len-1] would be remained
  // non_shared_data: data to be append, its length must be >= non_shared_len
  void TrimAppend(const size_t shared_len, const char* non_shared_data,
                  const size_t non_shared_len) {
    assert(shared_len <= key_size_);
    size_t total_size = shared_len + non_shared_len;

    if (IsKeyPinned() /* key is not in buf_ */) {
      // Copy the key from external memory to buf_ (copy shared_len bytes)
      EnlargeBufferIfNeeded(total_size);
      memcpy(buf_, key_, shared_len);
    } else if (total_size > buf_size_) {
      // Need to allocate space, delete previous space
      char* p = new char[total_size];
      memcpy(p, key_, shared_len);

      if (buf_ != space_) {
        delete[] buf_;
      }

      buf_ = p;
      buf_size_ = total_size;
    }

    memcpy(buf_ + shared_len, non_shared_data, non_shared_len);
    key_ = buf_;
    key_size_ = total_size;
  }

  Slice SetKey(const Slice& key, bool copy = true) {
    size_t size = key.size();
    if (copy) {
      // Copy key to buf_
      EnlargeBufferIfNeeded(size);
      memcpy(buf_, key.data(), size);
      key_ = buf_;
    } else {
      // Update key_ to point to external memory
      key_ = key.data();
    }
    key_size_ = size;
    return Slice(key_, key_size_);
  }

  // Copies the content of key, updates the reference to the user key in ikey
  // and returns a Slice referencing the new copy.
  Slice SetKey(const Slice& key, ParsedInternalKey* ikey) {
    size_t key_n = key.size();
    assert(key_n >= 8);
    SetKey(key);
    ikey->user_key = Slice(key_, key_n - 8);
    return Slice(key_, key_n);
  }

  // Update the sequence number in the internal key.  Guarantees not to
  // invalidate slices to the key (and the user key).
  void UpdateInternalKey(uint64_t seq, ValueType t) {
    assert(!IsKeyPinned());
    assert(key_size_ >= 8);
    uint64_t newval = (seq << 8) | t;
    EncodeFixed64(&buf_[key_size_ - 8], newval);
  }

  bool IsKeyPinned() const {
    return (key_ != buf_);
  }

  void SetInternalKey(const Slice& key_prefix, const Slice& user_key,
                      SequenceNumber s,
                      ValueType value_type = kValueTypeForSeek) {
    size_t psize = key_prefix.size();
    size_t usize = user_key.size();
    EnlargeBufferIfNeeded(psize + usize + sizeof(uint64_t));
    if (psize > 0) {
      memcpy(buf_, key_prefix.data(), psize);
    }
    memcpy(buf_ + psize, user_key.data(), usize);
    EncodeFixed64(buf_ + usize + psize, PackSequenceAndType(s, value_type));

    key_ = buf_;
    key_size_ = psize + usize + sizeof(uint64_t);
  }

  void SetInternalKey(const Slice& user_key, SequenceNumber s,
                      ValueType value_type = kValueTypeForSeek) {
    SetInternalKey(Slice(), user_key, s, value_type);
  }

  void Reserve(size_t size) {
    EnlargeBufferIfNeeded(size);
    key_size_ = size;
  }

  void SetInternalKey(const ParsedInternalKey& parsed_key) {
    SetInternalKey(Slice(), parsed_key);
  }

  void SetInternalKey(const Slice& key_prefix,
                      const ParsedInternalKey& parsed_key_suffix) {
    SetInternalKey(key_prefix, parsed_key_suffix.user_key,
                   parsed_key_suffix.sequence, parsed_key_suffix.type);
  }

  void EncodeLengthPrefixedKey(const Slice& key) {
    auto size = key.size();
    EnlargeBufferIfNeeded(size + static_cast<size_t>(VarintLength(size)));
    char* ptr = EncodeVarint32(buf_, static_cast<uint32_t>(size));
    memcpy(ptr, key.data(), size);
    key_ = buf_;
  }

 private:
  char* buf_;
  size_t buf_size_;
  const char* key_;
  size_t key_size_;
  char space_[32];  // Avoid allocation for short keys

  void ResetBuffer() {
    if (buf_ != space_) {
      delete[] buf_;
      buf_ = space_;
    }
    buf_size_ = sizeof(space_);
    key_size_ = 0;
  }

  // Enlarge the buffer size if needed based on key_size.
  // By default, static allocated buffer is used. Once there is a key
  // larger than the static allocated buffer, another buffer is dynamically
  // allocated, until a larger key buffer is requested. In that case, we
  // reallocate buffer and delete the old one.
  void EnlargeBufferIfNeeded(size_t key_size) {
    // If size is smaller than buffer size, continue using current buffer,
    // or the static allocated one, as default
    if (key_size > buf_size_) {
      // Need to enlarge the buffer.
      ResetBuffer();
      buf_ = new char[key_size];
      buf_size_ = key_size;
    }
  }

  // No copying allowed
  IterKey(const IterKey&) = delete;
  void operator=(const IterKey&) = delete;
};

// Read the key of a record from a write batch.
// if this record represent the default column family then cf_record
// must be passed as false, otherwise it must be passed as true.
extern bool ReadKeyFromWriteBatchEntry(Slice* input, Slice* key,
                                       bool cf_record);

// Read record from a write batch piece from input.
// tag, column_family, key, value and blob are return values. Callers own the
// Slice they point to.
// Tag is defined as ValueType.
// input will be advanced to after the record.
extern Status ReadRecordFromWriteBatch(Slice* input, char* tag,
                                       uint32_t* column_family, Slice* key,
                                       Slice* value, Slice* blob, Slice* xid);
}  // namespace vidardb
