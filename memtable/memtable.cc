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

#include "memtable/memtable.h"

#include <string.h>

#include <algorithm>
#include <limits>
#include <memory>

#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"
#include "db/writebuffer.h"
#include "table/column_table_factory.h"
#include "table/internal_iterator.h"
#include "table/merger.h"
#include "util/arena.h"
#include "util/coding.h"
#include "util/murmurhash.h"
#include "util/mutexlock.h"
#include "util/perf_context_imp.h"
#include "util/statistics.h"
#include "util/stop_watch.h"
#include "vidardb/comparator.h"
#include "vidardb/env.h"
#include "vidardb/iterator.h"
#include "vidardb/splitter.h"
#include "vidardb/table.h"

namespace vidardb {

MemTableOptions::MemTableOptions(const ImmutableCFOptions& ioptions,
                                 const MutableCFOptions& mutable_cf_options)
    : write_buffer_size(mutable_cf_options.write_buffer_size),
      arena_block_size(mutable_cf_options.arena_block_size),
      statistics(ioptions.statistics),
      info_log(ioptions.info_log),
      splitter(ioptions.splitter) {}

MemTable::MemTable(const InternalKeyComparator& cmp,
                   const ImmutableCFOptions& ioptions,
                   const MutableCFOptions& mutable_cf_options,
                   WriteBuffer* write_buffer, SequenceNumber earliest_seq)
    : comparator_(cmp),
      moptions_(ioptions, mutable_cf_options),
      refs_(0),
      kArenaBlockSize(OptimizeBlockSize(moptions_.arena_block_size)),
      arena_(moptions_.arena_block_size, 0),
      allocator_(&arena_, write_buffer),
      table_(ioptions.memtable_factory->CreateMemTableRep(
          comparator_, &allocator_, ioptions.info_log)),
      data_size_(0),
      num_entries_(0),
      num_deletes_(0),
      flush_in_progress_(false),
      flush_completed_(false),
      file_number_(0),
      first_seqno_(0),
      earliest_seqno_(earliest_seq),
      mem_next_logfile_number_(0),
      min_prep_log_referenced_(0),
      flush_state_(FLUSH_NOT_REQUESTED),
      env_(ioptions.env) {
  UpdateFlushState();
  // something went wrong if we need to flush before inserting anything
  assert(!ShouldScheduleFlush());
}

MemTable::~MemTable() { assert(refs_ == 0); }

size_t MemTable::ApproximateMemoryUsage() {
  size_t arena_usage = arena_.ApproximateMemoryUsage();
  size_t table_usage = table_->ApproximateMemoryUsage();
  // let MAX_USAGE =  std::numeric_limits<size_t>::max()
  // then if arena_usage + total_usage >= MAX_USAGE, return MAX_USAGE.
  // the following variation is to avoid numeric overflow.
  if (arena_usage >= std::numeric_limits<size_t>::max() - table_usage) {
    return std::numeric_limits<size_t>::max();
  }
  // otherwise, return the actual usage
  return arena_usage + table_usage;
}

bool MemTable::ShouldFlushNow() const {
  // In a lot of times, we cannot allocate arena blocks that exactly matches the
  // buffer size. Thus we have to decide if we should over-allocate or
  // under-allocate.
  // This constant variable can be interpreted as: if we still have more than
  // "kAllowOverAllocationRatio * kArenaBlockSize" space left, we'd try to over
  // allocate one more block.
  const double kAllowOverAllocationRatio = 0.6;

  // If arena still have room for new block allocation, we can safely say it
  // shouldn't flush.
  auto allocated_memory =
      table_->ApproximateMemoryUsage() + arena_.MemoryAllocatedBytes();

  // if we can still allocate one more block without exceeding the
  // over-allocation ratio, then we should not flush.
  if (allocated_memory + kArenaBlockSize <
      moptions_.write_buffer_size +
      kArenaBlockSize * kAllowOverAllocationRatio) {
    return false;
  }

  // if user keeps adding entries that exceeds moptions.write_buffer_size,
  // we need to flush earlier even though we still have much available
  // memory left.
  if (allocated_memory > moptions_.write_buffer_size +
      kArenaBlockSize * kAllowOverAllocationRatio) {
    return true;
  }

  // In this code path, Arena has already allocated its "last block", which
  // means the total allocatedmemory size is either:
  //  (1) "moderately" over allocated the memory (no more than `0.6 * arena
  // block size`. Or,
  //  (2) the allocated memory is less than write buffer size, but we'll stop
  // here since if we allocate a new arena block, we'll over allocate too much
  // more (half of the arena block size) memory.
  //
  // In either case, to avoid over-allocate, the last block will stop allocation
  // when its usage reaches a certain ratio, which we carefully choose "0.75
  // full" as the stop condition because it addresses the following issue with
  // great simplicity: What if the next inserted entry's size is
  // bigger than AllocatedAndUnused()?
  //
  // The answer is: if the entry size is also bigger than 0.25 *
  // kArenaBlockSize, a dedicated block will be allocated for it; otherwise
  // arena will anyway skip the AllocatedAndUnused() and allocate a new, empty
  // and regular block. In either case, we *overly* over-allocated.
  //
  // Therefore, setting the last block to be at most "0.75 full" avoids both
  // cases.
  //
  // NOTE: the average percentage of waste space of this approach can be counted
  // as: "arena block size * 0.25 / write buffer size". User who specify a small
  // write buffer size and/or big arena block size may suffer.
  return arena_.AllocatedAndUnused() < kArenaBlockSize / 4;
}

void MemTable::UpdateFlushState() {
  auto state = flush_state_.load(std::memory_order_relaxed);
  if (state == FLUSH_NOT_REQUESTED && ShouldFlushNow()) {
    // ignore CAS failure, because that means somebody else requested
    // a flush
    flush_state_.compare_exchange_strong(state, FLUSH_REQUESTED,
                                         std::memory_order_relaxed,
                                         std::memory_order_relaxed);
  }
}

int MemTable::KeyComparator::operator()(const char* prefix_len_key1,
                                        const char* prefix_len_key2) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice k1 = GetLengthPrefixedSlice(prefix_len_key1);
  Slice k2 = GetLengthPrefixedSlice(prefix_len_key2);
  return comparator.Compare(k1, k2);
}

int MemTable::KeyComparator::operator()(const char* prefix_len_key,
                                        const Slice& key)
    const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(prefix_len_key);
  return comparator.Compare(a, key);
}

Slice MemTableRep::UserKey(const char* key) const {
  Slice slice = GetLengthPrefixedSlice(key);
  return Slice(slice.data(), slice.size() - 8);
}

KeyHandle MemTableRep::Allocate(const size_t len, char** buf) {
  *buf = allocator_->Allocate(len);
  return static_cast<KeyHandle>(*buf);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, static_cast<uint32_t>(target.size()));
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator : public InternalIterator {
 public:
  MemTableIterator(const MemTable& mem, const std::vector<uint32_t>& columns,
                   Arena* arena)
      : valid_(false), arena_mode_(arena != nullptr), columns_(columns) {
    iter_ = mem.table_->GetIterator(arena);
    splitter_ = mem.GetMemTableOptions()->splitter;
  }

  ~MemTableIterator() {
#ifndef NDEBUG
    // Assert that the MemTableIterator is never deleted while
    // Pinning is Enabled.
    assert(!pinned_iters_mgr_ ||
           (pinned_iters_mgr_ && !pinned_iters_mgr_->PinningEnabled()));
#endif
    if (arena_mode_) {
      iter_->~Iterator();
    } else {
      delete iter_;
    }
  }

#ifndef NDEBUG
  virtual void SetPinnedItersMgr(
      PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
  }
  PinnedIteratorsManager* pinned_iters_mgr_ = nullptr;
#endif

  virtual bool Valid() const override { return valid_; }
  virtual void Seek(const Slice& k) override {
    PERF_TIMER_GUARD(seek_on_memtable_time);
    PERF_COUNTER_ADD(seek_on_memtable_count, 1);
    iter_->Seek(k, nullptr);
    valid_ = iter_->Valid();
  }
  virtual void SeekToFirst() override {
    iter_->SeekToFirst();
    valid_ = iter_->Valid();
  }
  virtual void SeekToLast() override {
    iter_->SeekToLast();
    valid_ = iter_->Valid();
  }
  virtual void Next() override {
    assert(Valid());
    iter_->Next();
    valid_ = iter_->Valid();
  }
  virtual void Prev() override {
    assert(Valid());
    iter_->Prev();
    valid_ = iter_->Valid();
  }
  virtual Slice key() const override {
    assert(Valid());
    return GetLengthPrefixedSlice(iter_->key());
  }
  virtual Slice value() override {
    assert(Valid());
    Slice key_slice = GetLengthPrefixedSlice(iter_->key());
    Slice val_slice =
        GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
    if (columns_.empty() || !splitter_ || val_slice.empty()) {
      return val_slice;  // for flushing
    }
    value_.clear();  // prepare for splitting user value
    return ReformatUserValue(val_slice, columns_, splitter_, value_);
  }

  virtual Status status() const override { return Status::OK(); }

  virtual bool IsKeyPinned() const override {
    // memtable data is always pinned
    return true;
  }

 private:
  MemTableRep::Iterator* iter_;
  bool valid_;
  bool arena_mode_;
  const Splitter* splitter_;
  const std::vector<uint32_t> columns_;
  std::string value_;  // mutable

  // No copying allowed
  MemTableIterator(const MemTableIterator&);
  void operator=(const MemTableIterator&);
};

InternalIterator* MemTable::NewIterator(const ReadOptions& read_options,
                                        Arena* arena) {
  assert(arena != nullptr);
  auto mem = arena->AllocateAligned(sizeof(MemTableIterator));
  return new (mem) MemTableIterator(*this, read_options.columns, arena);
}

uint64_t MemTable::ApproximateSize(const Slice& start_ikey,
                                   const Slice& end_ikey) {
  uint64_t entry_count = table_->ApproximateNumEntries(start_ikey, end_ikey);
  if (entry_count == 0) {
    return 0;
  }
  uint64_t n = num_entries_.load(std::memory_order_relaxed);
  if (n == 0) {
    return 0;
  }
  if (entry_count > n) {
    // table_->ApproximateNumEntries() is just an estimate so it can be larger
    // than actual entries we have. Cap it to entries we have to limit the
    // inaccuracy.
    entry_count = n;
  }
  uint64_t data_size = data_size_.load(std::memory_order_relaxed);
  return entry_count * (data_size / n);
}

void MemTable::Add(SequenceNumber s, ValueType type,
                   const Slice& key, /* user key */
                   const Slice& value, bool allow_concurrent) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  uint32_t key_size = static_cast<uint32_t>(key.size());
  uint32_t val_size = static_cast<uint32_t>(value.size());
  uint32_t internal_key_size = key_size + 8;
  const uint32_t encoded_len = VarintLength(internal_key_size) +
                               internal_key_size + VarintLength(val_size) +
                               val_size;
  char* buf = nullptr;
  KeyHandle handle = table_->Allocate(encoded_len, &buf);

  char* p = EncodeVarint32(buf, internal_key_size);
  memcpy(p, key.data(), key_size);
  p += key_size;
  uint64_t packed = PackSequenceAndType(s, type);
  EncodeFixed64(p, packed);
  p += 8;
  p = EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size);
  assert((unsigned)(p + val_size - buf) == (unsigned)encoded_len);
  if (!allow_concurrent) {
    table_->Insert(handle);

    // this is a bit ugly, but is the way to avoid locked instructions
    // when incrementing an atomic
    num_entries_.store(num_entries_.load(std::memory_order_relaxed) + 1,
                       std::memory_order_relaxed);
    data_size_.store(data_size_.load(std::memory_order_relaxed) + encoded_len,
                     std::memory_order_relaxed);
    if (type == kTypeDeletion) {
      num_deletes_.store(num_deletes_.load(std::memory_order_relaxed) + 1,
                         std::memory_order_relaxed);
    }

    // The first sequence number inserted into the memtable
    assert(first_seqno_ == 0 || s > first_seqno_);
    if (first_seqno_ == 0) {
      first_seqno_.store(s, std::memory_order_relaxed);

      if (earliest_seqno_ == kMaxSequenceNumber) {
        earliest_seqno_.store(GetFirstSequenceNumber(),
                              std::memory_order_relaxed);
      }
      assert(first_seqno_.load() >= earliest_seqno_.load());
    }
  } else {
    table_->InsertConcurrently(handle);

    num_entries_.fetch_add(1, std::memory_order_relaxed);
    data_size_.fetch_add(encoded_len, std::memory_order_relaxed);
    if (type == kTypeDeletion) {
      num_deletes_.fetch_add(1, std::memory_order_relaxed);
    }

    // atomically update first_seqno_ and earliest_seqno_.
    uint64_t cur_seq_num = first_seqno_.load(std::memory_order_relaxed);
    while ((cur_seq_num == 0 || s < cur_seq_num) &&
           !first_seqno_.compare_exchange_weak(cur_seq_num, s)) {
    }
    uint64_t cur_earliest_seqno =
        earliest_seqno_.load(std::memory_order_relaxed);
    while (
        (cur_earliest_seqno == kMaxSequenceNumber || s < cur_earliest_seqno) &&
        !first_seqno_.compare_exchange_weak(cur_earliest_seqno, s)) {
    }
  }

  UpdateFlushState();
}

// Callback from MemTable::Get()
namespace {

struct Saver {
  Status* status;
  const LookupKey* key;
  const LookupRange* range;  // Shichao
  bool* found_final_value;   // Is value set correctly? Used by KeyMayExist
  std::string* get_value;    // User value ptr for Get()
  std::list<RangeQueryKeyVal>* res;  // Shichao
  std::map<std::string, SeqTypeVal>::iterator prev_iter;  // Shichao
  SequenceNumber seq;
  MemTable* mem;
  Logger* logger;
  Statistics* statistics;
  Env* env_;
  ReadOptions* read_options;  // Quanzhao
};
}  // namespace

static bool SaveValue(void* arg, const char* entry) {
  Saver* s = reinterpret_cast<Saver*>(arg);

  assert(s != nullptr);

  // entry format is:
  //    klength  varint32
  //    userkey  char[klength-8]
  //    tag      uint64
  //    vlength  varint32
  //    value    char[vlength]
  // Check that it belongs to same user key.  We do not check the
  // sequence number since the Seek() call above should have skipped
  // all entries with overly large sequence numbers.
  uint32_t key_length;
  const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
  if (s->mem->GetInternalKeyComparator().user_comparator()->Equal(
          Slice(key_ptr, key_length - 8), s->key->user_key())) {
    // Correct user key
    const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
    ValueType type;
    UnPackSequenceAndType(tag, &s->seq, &type);

    switch (type) {
      case kTypeValue: {
        std::string buf;  // prepare for splitting user value
        Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
        Slice user_val(ReformatUserValue(v, s->read_options->columns,
                                         s->mem->GetMemTableOptions()->splitter,
                                         buf));
        *(s->status) = Status::OK();
        if (s->get_value != nullptr) {
          s->get_value->assign(user_val.data(), user_val.size());
        }
        *(s->found_final_value) = true;
        return false;
      }
      case kTypeDeletion:
      case kTypeSingleDeletion: {
        *(s->status) = Status::NotFound();
        *(s->found_final_value) = true;
        return false;
      }
      default:
        assert(false);
        return true;
    }
  }

  // s->state could be Corrupt, merge or notfound
  return false;
}

/***************************** Shichao *****************************/
static bool SaveValueForRangeQuery(void* arg, const char* entry) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  assert(s != nullptr);

  uint32_t key_length;
  const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
  Slice internal_key = Slice(key_ptr, key_length);

  RangeQueryMeta* meta =
      static_cast<RangeQueryMeta*>(s->read_options->range_query_meta);
  if (CompareRangeLimit(s->mem->GetInternalKeyComparator(), internal_key,
                        meta->current_limit_key) > 0) {
    *(s->status) = Status::OK();
    return false;
  }

  const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
  ValueType type;
  UnPackSequenceAndType(tag, &s->seq, &type);
  SequenceNumber sequence_num = s->range->SequenceNum();
  switch (type) {
    case kTypeValue:
    case kTypeDeletion:
    case kTypeSingleDeletion: {
      if (s->seq <= sequence_num) {
        std::string user_key(internal_key.data(), internal_key.size() - 8);
        SeqTypeVal stv(s->seq, type, s->res->end());

        auto it = s->prev_iter;
        if (it != meta->map_res->end()) {
          it++;
        }
        it = meta->map_res->emplace_hint(it, user_key, std::move(stv));
        s->prev_iter = it;

        if (it->second.seq_ <= s->seq) {
          // TODO: might leverage move semantic later
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          std::string buf;  // prepare for splitting user value
          Slice user_val(
              ReformatUserValue(v, s->read_options->columns,
                                s->mem->GetMemTableOptions()->splitter, buf));

          if (it->second.seq_ < s->seq) {
            // replaced
            if (it->second.type_ == kTypeDeletion) {
              meta->del_keys.erase(it->second.seq_);
            }
            assert(s->read_options->result_val_size >=
                it->second.iter_->user_val.size());
            s->read_options->result_val_size -= 
                it->second.iter_->user_val.size();
            it->second.seq_ = s->seq;
            it->second.type_ = type;
            it->second.iter_->user_val = std::move(user_val.ToString());
            s->read_options->result_val_size += 
                it->second.iter_->user_val.size();
            if (type == kTypeDeletion) {
              meta->del_keys.insert({s->seq, it->second.iter_});
            }
          } else {
            // inserted
            size_t delta_key_size = user_key.size();
            size_t delta_val_size = user_val.size();
            s->res->emplace_back(user_key, std::move(user_val.ToString()));
            s->read_options->result_key_size += delta_key_size;
            s->read_options->result_val_size += delta_val_size;
            it->second.iter_ = --(s->res->end());
            if (type == kTypeDeletion) {
              meta->del_keys.insert({s->seq, it->second.iter_});
            }
          }

          auto crl = CompressResultList(s->res, *(s->read_options));
          if (crl.size() > 0 && meta->map_res->rbegin()->first <= user_key) {
            // Reach the batch capacity
            *(s->status) = Status::OK();
            return false;
          }
        }
      }
      *(s->status) = Status::OK();
      return true;
    }
    default:
      *(s->status) = Status::Corruption(Slice());
      return false;
  }
}
/***************************** Shichao *****************************/

bool MemTable::Get(ReadOptions& read_options, const LookupKey& key,
                   std::string* value, Status* s, SequenceNumber* seq) {
  // The sequence number is updated synchronously in version_set.h
  if (IsEmpty()) {
    // Avoiding recording stats for speed.
    return false;
  }
  PERF_TIMER_GUARD(get_from_memtable_time);

  bool found_final_value = false;
  bool merge_in_progress = s->IsMergeInProgress();

  Saver saver;
  saver.status = s;
  saver.found_final_value = &found_final_value;
  saver.key = &key;
  saver.get_value = value;
  saver.seq = kMaxSequenceNumber;
  saver.mem = this;
  saver.logger = moptions_.info_log;
  saver.statistics = moptions_.statistics;
  saver.env_ = env_;
  saver.read_options = &read_options;
  table_->Get(key, &saver, SaveValue);

  *seq = saver.seq;

  // No change to value, since we have not yet found a Put/Delete
  if (!found_final_value && merge_in_progress) {
    *s = Status::MergeInProgress();
  }
  PERF_COUNTER_ADD(get_from_memtable_count, 1);
  return found_final_value;
}

/***************************** Shichao *****************************/
bool MemTable::RangeQuery(ReadOptions& read_options, const LookupRange& range,
                          std::list<RangeQueryKeyVal>& res, Status* s) {
  if (IsEmpty()) {
    *s = Status::NotFound(Slice());
    return true;
  }

  RangeQueryMeta* meta =
      static_cast<RangeQueryMeta*>(read_options.range_query_meta);
  Saver saver;
  saver.status = s;
  saver.range = &range;
  saver.res = &res;
  saver.prev_iter = meta->map_res->end();
  saver.seq = kMaxSequenceNumber;
  saver.mem = this;
  saver.logger = moptions_.info_log;
  saver.statistics = moptions_.statistics;
  saver.env_ = env_;
  saver.read_options = &read_options;

  size_t old_size = res.size();
  table_->RangeQuery(range, res, &saver, SaveValueForRangeQuery);
  if (res.size() == old_size) {
    *s = Status::NotFound(Slice());
  }

  return (s->ok() || s->IsNotFound());
}
/***************************** Shichao *****************************/

void MemTable::Update(SequenceNumber seq, const Slice& key,
                      const Slice& value) {
  LookupKey lkey(key, seq);
  Slice mem_key = lkey.memtable_key();

  std::unique_ptr<MemTableRep::Iterator> iter(
      table_->GetDynamicPrefixIterator());
  iter->Seek(lkey.internal_key(), mem_key.data());

  if (iter->Valid()) {
    // entry format is:
    //    key_length  varint32
    //    userkey  char[klength-8]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter->key();
    uint32_t key_length = 0;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Equal(
            Slice(key_ptr, key_length - 8), lkey.user_key())) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      ValueType type;
      SequenceNumber unused;
      UnPackSequenceAndType(tag, &unused, &type);
      switch (type) {
        case kTypeValue: {
          // Update value, if new value size  <= previous value size
          [[gnu::fallthrough]];
        }
        default:
          // If the latest value is kTypeDeletion, kTypeMerge or kTypeLogData
          // we don't have enough space for update inplace
            Add(seq, kTypeValue, key, value);
            return;
      }
    }
  }

  // key doesn't exist
  Add(seq, kTypeValue, key, value);
}

void MemTableRep::Get(const LookupKey& k, void* callback_args,
                      bool (*callback_func)(void* arg, const char* entry)) {
  auto iter = GetDynamicPrefixIterator();
  for (iter->Seek(k.internal_key(), k.memtable_key().data());
       iter->Valid() && callback_func(callback_args, iter->key());
       iter->Next()) {
  }
}

void MemTable::RefLogContainingPrepSection(uint64_t log) {
  assert(log > 0);
  auto cur = min_prep_log_referenced_.load();
  while ((log < cur || cur == 0) &&
         !min_prep_log_referenced_.compare_exchange_strong(cur, log)) {
    cur = min_prep_log_referenced_.load();
  }
}

uint64_t MemTable::GetMinLogContainingPrepSection() {
  return min_prep_log_referenced_.load();
}

}  // namespace vidardb
