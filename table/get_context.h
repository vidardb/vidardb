//  Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#include <string>
#include "vidardb/env.h"
#include "vidardb/types.h"
#include "db/dbformat.h"

namespace vidardb {

class GetContext {
 public:
  enum GetState {
    kNotFound,
    kFound,
    kDeleted,
    kCorrupt,
  };

  GetContext(const Comparator* ucmp,
             Logger* logger, Statistics* statistics, GetState init_state,
             const Slice& user_key, std::string* ret_value, bool* value_found,
             Env* env, SequenceNumber* seq = nullptr);

  void MarkKeyMayExist();

  // Records this key, value, and any meta-data (such as sequence number and
  // state) into this GetContext.
  //
  // Returns True if more keys need to be read (due to merges) or
  //         False if the complete value has been found.
  bool SaveValue(const ParsedInternalKey& parsed_key, const Slice& value);

  GetState State() const {
    return state_;
  }

  // If a non-null string is passed, all the SaveValue calls will be
  // logged into the string. The operations can then be replayed on
  // another GetContext with replayGetContextLog.
  void SetReplayLog(std::string* replay_log) {
    replay_log_ = replay_log;
  }

  // Do we need to fetch the SequenceNumber for this key?
  bool NeedToReadSequence() const {
    return (seq_ != nullptr);
  }

  /************************** Shichao *******************************/
  // For column store only
  bool IsEqualToUserKey(const ParsedInternalKey& parsed_key) const {
    return ucmp_->Equal(parsed_key.user_key, user_key_);
  }
  /************************** Shichao *******************************/

 private:
  const Comparator* ucmp_;
  Logger* logger_;
  Statistics* statistics_;

  GetState state_;
  Slice user_key_;
  std::string* value_;
  bool* value_found_;  // Is value set correctly? Used by KeyMayExist
  Env* env_;
  // If a key is found, seq_ will be set to the SequenceNumber of most recent
  // write to the key or kMaxSequenceNumber if unknown
  SequenceNumber* seq_;
  std::string* replay_log_;
};

void replayGetContextLog(const Slice& replay_log, const Slice& user_key,
                         GetContext* get_context);

}  // namespace vidardb
