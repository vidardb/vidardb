//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "table/get_context.h"
#include "vidardb/env.h"
#include "vidardb/statistics.h"
#include "util/perf_context_imp.h"
#include "util/statistics.h"

namespace vidardb {

namespace {

void appendToReplayLog(std::string* replay_log, ValueType type, Slice value) {
#ifndef VIDARDB_LITE
  if (replay_log) {
    if (replay_log->empty()) {
      // Optimization: in the common case of only one operation in the
      // log, we allocate the exact amount of space needed.
      replay_log->reserve(1 + VarintLength(value.size()) + value.size());
    }
    replay_log->push_back(type);
    PutLengthPrefixedSlice(replay_log, value);
  }
#endif  // VIDARDB_LITE
}

}  // namespace

GetContext::GetContext(const Comparator* ucmp, GetState init_state,
                       const Slice& user_key, std::string* ret_value,
                       bool* value_found, SequenceNumber* seq)
    : ucmp_(ucmp),
      state_(init_state),
      user_key_(user_key),
      value_(ret_value),
      value_found_(value_found),
      seq_(seq),
      replay_log_(nullptr) {
  if (seq_) {
    *seq_ = kMaxSequenceNumber;
  }
}

// Called from TableCache::Get and Table::Get when file/block in which
// key may exist are not there in TableCache/BlockCache respectively. In this
// case we can't guarantee that key does not exist and are not permitted to do
// IO to be certain.Set the status=kFound and value_found=false to let the
// caller know that key may exist but is not there in memory
void GetContext::MarkKeyMayExist() {
  state_ = kFound;
  if (value_found_ != nullptr) {
    *value_found_ = false;
  }
}

bool GetContext::SaveValue(const ParsedInternalKey& parsed_key,
                           const Slice& value) {
  if (ucmp_->Equal(parsed_key.user_key, user_key_)) {
    appendToReplayLog(replay_log_, parsed_key.type, value);

    if (seq_ != nullptr) {
      // Set the sequence number if it is uninitialized
      if (*seq_ == kMaxSequenceNumber) {
        *seq_ = parsed_key.sequence;
      }
    }

    // Key matches. Process it
    switch (parsed_key.type) {
      case kTypeValue:
        assert(state_ == kNotFound);
        if (kNotFound == state_) {
          state_ = kFound;
          if (value_ != nullptr) {
            value_->assign(value.data(), value.size());
          }
        }
        return false;

      case kTypeDeletion:
        assert(state_ == kNotFound);
        if (kNotFound == state_) {
          state_ = kDeleted;
        }
        return false;

      default:
        assert(false);
        break;
    }
  }

  // state_ could be Corrupt, merge or notfound
  return false;
}

void replayGetContextLog(const Slice& replay_log, const Slice& user_key,
                         GetContext* get_context) {
#ifndef VIDARDB_LITE
  Slice s = replay_log;
  while (s.size()) {
    auto type = static_cast<ValueType>(*s.data());
    s.remove_prefix(1);
    Slice value;
    bool ret = GetLengthPrefixedSlice(&s, &value);
    assert(ret);
    (void)ret;

    // Since SequenceNumber is not stored and unknown, we will use
    // kMaxSequenceNumber.
    get_context->SaveValue(
        ParsedInternalKey(user_key, kMaxSequenceNumber, type), value);
  }
#else   // VIDARDB_LITE
  assert(false);
#endif  // VIDARDB_LITE
}

}  // namespace vidardb
