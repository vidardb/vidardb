//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <stdint.h>

#include <memory>

namespace vidardb {

class Env;
class WriteControllerToken;

// WriteController is controlling write stalls in our write code-path. Write
// stalls happen when compaction can't keep up with write rate.
// All of the methods here (including WriteControllerToken's destructors) need
// to be called while holding DB mutex
class WriteController {
 public:
  explicit WriteController(uint64_t _delayed_write_rate = 1024u * 1024u * 32u)
      : total_stopped_(0),
        total_compaction_pressure_(0) { }
  ~WriteController() = default;

  // When an actor (column family) requests a stop token, all writes will be
  // stopped until the stop token is released (deleted)
  std::unique_ptr<WriteControllerToken> GetStopToken();
  // When an actor (column family) requests a moderate token, compaction
  // threads will be increased
  std::unique_ptr<WriteControllerToken> GetCompactionPressureToken();

  // these three methods are querying the state of the WriteController
  bool IsStopped() const;
  bool NeedSpeedupCompaction() const {
    return IsStopped() || total_compaction_pressure_ > 0;
  }

 private:
  friend class WriteControllerToken;
  friend class StopWriteToken;
  friend class CompactionPressureToken;

  int total_stopped_;
  int total_compaction_pressure_;
};

class WriteControllerToken {
 public:
  explicit WriteControllerToken(WriteController* controller)
      : controller_(controller) {}
  virtual ~WriteControllerToken() {}

 protected:
  WriteController* controller_;

 private:
  // no copying allowed
  WriteControllerToken(const WriteControllerToken&) = delete;
  void operator=(const WriteControllerToken&) = delete;
};

class StopWriteToken : public WriteControllerToken {
 public:
  explicit StopWriteToken(WriteController* controller)
      : WriteControllerToken(controller) {}
  virtual ~StopWriteToken();
};

class CompactionPressureToken : public WriteControllerToken {
 public:
  explicit CompactionPressureToken(WriteController* controller)
      : WriteControllerToken(controller) {}
  virtual ~CompactionPressureToken();
};

}  // namespace vidardb
