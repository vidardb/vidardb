//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring
//    kTypeDeletion varstring
//    kTypeColumnFamilyValue varint32 varstring varstring
//    kTypeColumnFamilyDeletion varint32 varstring varstring
//    kTypeBeginPrepareXID varstring
//    kTypeEndPrepareXID
//    kTypeCommitXID varstring
//    kTypeRollbackXID varstring
//    kTypeNoop
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "vidardb/write_batch.h"

#include <stack>
#include <stdexcept>
#include <vector>

#include "db/column_family.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/flush_scheduler.h"
#include "memtable/memtable.h"
#include "db/snapshot_impl.h"
#include "db/write_batch_internal.h"
#include "util/coding.h"
#include "util/perf_context_imp.h"
#include "util/statistics.h"

namespace vidardb {

struct SavePoint {
  size_t size;  // size of rep_
  int count;    // count of elements in rep_
};

struct SavePoints {
  std::stack<SavePoint> stack;
};

WriteBatch::WriteBatch(size_t reserved_bytes) : save_points_(nullptr), rep_() {
  rep_.reserve((reserved_bytes > WriteBatchInternal::kHeader) ?
    reserved_bytes : WriteBatchInternal::kHeader);
  rep_.resize(WriteBatchInternal::kHeader);
}

WriteBatch::WriteBatch(const std::string& rep)
    : save_points_(nullptr), rep_(rep) {}

WriteBatch::WriteBatch(const WriteBatch& src)
    : save_points_(src.save_points_), rep_(src.rep_) {}

WriteBatch::WriteBatch(WriteBatch&& src)
    : save_points_(std::move(src.save_points_)), rep_(std::move(src.rep_)) {}

WriteBatch& WriteBatch::operator=(const WriteBatch& src) {
  if (&src != this) {
    this->~WriteBatch();
    new (this) WriteBatch(src);
  }
  return *this;
}

WriteBatch& WriteBatch::operator=(WriteBatch&& src) {
  if (&src != this) {
    this->~WriteBatch();
    new (this) WriteBatch(std::move(src));
  }
  return *this;
}

WriteBatch::~WriteBatch() { delete save_points_; }

WriteBatch::Handler::~Handler() { }

void WriteBatch::Handler::LogData(const Slice& blob) {
  // If the user has not specified something to do with blobs, then we ignore
  // them.
}

bool WriteBatch::Handler::Continue() {
  return true;
}

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(WriteBatchInternal::kHeader);

  if (save_points_ != nullptr) {
    while (!save_points_->stack.empty()) {
      save_points_->stack.pop();
    }
  }
}

int WriteBatch::Count() const {
  return WriteBatchInternal::Count(this);
}

bool ReadKeyFromWriteBatchEntry(Slice* input, Slice* key, bool cf_record) {
  assert(input != nullptr && key != nullptr);
  // Skip tag byte
  input->remove_prefix(1);

  if (cf_record) {
    // Skip column_family bytes
    uint32_t cf;
    if (!GetVarint32(input, &cf)) {
      return false;
    }
  }

  // Extract key
  return GetLengthPrefixedSlice(input, key);
}

Status ReadRecordFromWriteBatch(Slice* input, char* tag,
                                uint32_t* column_family, Slice* key,
                                Slice* value, Slice* blob, Slice* xid) {
  assert(key != nullptr && value != nullptr);
  *tag = (*input)[0];
  input->remove_prefix(1);
  *column_family = 0;  // default
  switch (*tag) {
    case kTypeColumnFamilyValue:
      if (!GetVarint32(input, column_family)) {
        return Status::Corruption("bad WriteBatch Put");
      }
    // intentional fallthrough
    case kTypeValue:
      if (!GetLengthPrefixedSlice(input, key) ||
          !GetLengthPrefixedSlice(input, value)) {
        return Status::Corruption("bad WriteBatch Put");
      }
      break;
    case kTypeColumnFamilyDeletion:
    // intentional fallthrough
    case kTypeDeletion:
      if (!GetLengthPrefixedSlice(input, key)) {
        return Status::Corruption("bad WriteBatch Delete");
      }
      break;
    case kTypeLogData:
      assert(blob != nullptr);
      if (!GetLengthPrefixedSlice(input, blob)) {
        return Status::Corruption("bad WriteBatch Blob");
      }
      break;
    case kTypeNoop:
    case kTypeBeginPrepareXID:
      break;
    case kTypeEndPrepareXID:
      if (!GetLengthPrefixedSlice(input, xid)) {
        return Status::Corruption("bad EndPrepare XID");
      }
      break;
    case kTypeCommitXID:
      if (!GetLengthPrefixedSlice(input, xid)) {
        return Status::Corruption("bad Commit XID");
      }
      break;
    case kTypeRollbackXID:
      if (!GetLengthPrefixedSlice(input, xid)) {
        return Status::Corruption("bad Rollback XID");
      }
      break;
    default:
      return Status::Corruption("unknown WriteBatch tag");
  }
  return Status::OK();
}

Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < WriteBatchInternal::kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(WriteBatchInternal::kHeader);
  Slice key, value, blob, xid;
  int found = 0;
  Status s;
  while (s.ok() && !input.empty() && handler->Continue()) {
    char tag = 0;
    uint32_t column_family = 0;  // default

    s = ReadRecordFromWriteBatch(&input, &tag, &column_family, &key, &value,
                                 &blob, &xid);
    if (!s.ok()) {
      return s;
    }

    switch (tag) {
      case kTypeColumnFamilyValue:
      case kTypeValue:
        s = handler->PutCF(column_family, key, value);
        found++;
        break;
      case kTypeColumnFamilyDeletion:
      case kTypeDeletion:
        s = handler->DeleteCF(column_family, key);
        found++;
        break;
      case kTypeLogData:
        handler->LogData(blob);
        break;
      case kTypeBeginPrepareXID:
        handler->MarkBeginPrepare();
        break;
      case kTypeEndPrepareXID:
        handler->MarkEndPrepare(xid);
        break;
      case kTypeCommitXID:
        handler->MarkCommit(xid);
        break;
      case kTypeRollbackXID:
        handler->MarkRollback(xid);
        break;
      case kTypeNoop:
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (!s.ok()) {
    return s;
  }
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

size_t WriteBatchInternal::GetFirstOffset(WriteBatch* b) {
  return WriteBatchInternal::kHeader;
}

void WriteBatchInternal::Put(WriteBatch* b, uint32_t column_family_id,
                             const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeValue));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyValue));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, key);
  PutLengthPrefixedSlice(&b->rep_, value);
}

void WriteBatch::Put(ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& value) {
  WriteBatchInternal::Put(this, GetColumnFamilyID(column_family), key, value);
}

void WriteBatchInternal::Delete(WriteBatch* b, uint32_t column_family_id,
                                const Slice& key) {
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, key);
}

void WriteBatch::Delete(ColumnFamilyHandle* column_family, const Slice& key) {
  WriteBatchInternal::Delete(this, GetColumnFamilyID(column_family), key);
}

void WriteBatchInternal::InsertNoop(WriteBatch* b) {
  b->rep_.push_back(static_cast<char>(kTypeNoop));
}

void WriteBatchInternal::MarkEndPrepare(WriteBatch* b, const Slice& xid) {
  // a manually constructed batch can only contain one prepare section
  assert(b->rep_[12] == static_cast<char>(kTypeNoop));

  // all savepoints up to this point are cleared
  if (b->save_points_ != nullptr) {
    while (!b->save_points_->stack.empty()) {
      b->save_points_->stack.pop();
    }
  }

  // rewrite noop as begin marker
  b->rep_[12] = static_cast<char>(kTypeBeginPrepareXID);
  b->rep_.push_back(static_cast<char>(kTypeEndPrepareXID));
  PutLengthPrefixedSlice(&b->rep_, xid);
}

void WriteBatchInternal::MarkCommit(WriteBatch* b, const Slice& xid) {
  b->rep_.push_back(static_cast<char>(kTypeCommitXID));
  PutLengthPrefixedSlice(&b->rep_, xid);
}

void WriteBatchInternal::MarkRollback(WriteBatch* b, const Slice& xid) {
  b->rep_.push_back(static_cast<char>(kTypeRollbackXID));
  PutLengthPrefixedSlice(&b->rep_, xid);
}

void WriteBatch::PutLogData(const Slice& blob) {
  rep_.push_back(static_cast<char>(kTypeLogData));
  PutLengthPrefixedSlice(&rep_, blob);
}

void WriteBatch::SetSavePoint() {
  if (save_points_ == nullptr) {
    save_points_ = new SavePoints();
  }
  // Record length and count of current batch of writes.
  save_points_->stack.push(SavePoint{GetDataSize(), Count()});
}

Status WriteBatch::RollbackToSavePoint() {
  if (save_points_ == nullptr || save_points_->stack.size() == 0) {
    return Status::NotFound();
  }

  // Pop the most recent savepoint off the stack
  SavePoint savepoint = save_points_->stack.top();
  save_points_->stack.pop();

  assert(savepoint.size <= rep_.size());
  assert(savepoint.count <= Count());

  if (savepoint.size == rep_.size()) {
    // No changes to rollback
  } else if (savepoint.size == 0) {
    // Rollback everything
    Clear();
  } else {
    rep_.resize(savepoint.size);
    WriteBatchInternal::SetCount(this, savepoint.count);
  }

  return Status::OK();
}

class MemTableInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_;
  ColumnFamilyMemTables* const cf_mems_;
  FlushScheduler* const flush_scheduler_;
  const bool ignore_missing_column_families_;
  const uint64_t recovering_log_number_;
  // log number that all Memtables inserted into should reference
  uint64_t log_number_ref_;
  DBImpl* db_;
  const bool concurrent_memtable_writes_;
  // current recovered transaction we are rebuilding (recovery)
  WriteBatch* rebuilding_trx_;

  // cf_mems should not be shared with concurrent inserters
  MemTableInserter(SequenceNumber sequence, ColumnFamilyMemTables* cf_mems,
                   FlushScheduler* flush_scheduler,
                   bool ignore_missing_column_families,
                   uint64_t recovering_log_number, DB* db,
                   bool concurrent_memtable_writes)
      : sequence_(sequence),
        cf_mems_(cf_mems),
        flush_scheduler_(flush_scheduler),
        ignore_missing_column_families_(ignore_missing_column_families),
        recovering_log_number_(recovering_log_number),
        log_number_ref_(0),
        db_(reinterpret_cast<DBImpl*>(db)),
        concurrent_memtable_writes_(concurrent_memtable_writes),
        rebuilding_trx_(nullptr) {
    assert(cf_mems_);
  }

  void set_log_number_ref(uint64_t log) { log_number_ref_ = log; }

  SequenceNumber get_final_sequence() { return sequence_; }

  bool SeekToColumnFamily(uint32_t column_family_id, Status* s) {
    // If we are in a concurrent mode, it is the caller's responsibility
    // to clone the original ColumnFamilyMemTables so that each thread
    // has its own instance.  Otherwise, it must be guaranteed that there
    // is no concurrent access
    bool found = cf_mems_->Seek(column_family_id);
    if (!found) {
      if (ignore_missing_column_families_) {
        *s = Status::OK();
      } else {
        *s = Status::InvalidArgument(
            "Invalid column family specified in write batch");
      }
      return false;
    }
    if (recovering_log_number_ != 0 &&
        recovering_log_number_ < cf_mems_->GetLogNumber()) {
      // This is true only in recovery environment (recovering_log_number_ is
      // always 0 in
      // non-recovery, regular write code-path)
      // * If recovering_log_number_ < cf_mems_->GetLogNumber(), this means that
      // column
      // family already contains updates from this log. We can't apply updates
      // twice because of update-in-place or merge workloads -- ignore the
      // update
      *s = Status::OK();
      return false;
    }

    if (log_number_ref_ > 0) {
      cf_mems_->GetMemTable()->RefLogContainingPrepSection(log_number_ref_);
    }

    return true;
  }

  virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                       const Slice& value) override {
    if (rebuilding_trx_ != nullptr) {
      WriteBatchInternal::Put(rebuilding_trx_, column_family_id, key, value);
      return Status::OK();
    }

    Status seek_status;
    if (!SeekToColumnFamily(column_family_id, &seek_status)) {
      ++sequence_;
      return seek_status;
    }

    MemTable* mem = cf_mems_->GetMemTable();

    mem->Add(sequence_, kTypeValue, key, value, concurrent_memtable_writes_);

    // Since all Puts are logged in transaction logs (if enabled), always bump
    // sequence number. Even if the update eventually fails and does not result
    // in memtable add/update.
    sequence_++;
    CheckMemtableFull();
    return Status::OK();
  }

  Status DeleteImpl(uint32_t column_family_id, const Slice& key,
                    ValueType delete_type) {
    MemTable* mem = cf_mems_->GetMemTable();
    mem->Add(sequence_, delete_type, key, Slice(), concurrent_memtable_writes_);
    sequence_++;
    CheckMemtableFull();
    return Status::OK();
  }

  virtual Status DeleteCF(uint32_t column_family_id,
                          const Slice& key) override {
    if (rebuilding_trx_ != nullptr) {
      WriteBatchInternal::Delete(rebuilding_trx_, column_family_id, key);
      return Status::OK();
    }

    Status seek_status;
    if (!SeekToColumnFamily(column_family_id, &seek_status)) {
      ++sequence_;
      return seek_status;
    }

    return DeleteImpl(column_family_id, key, kTypeDeletion);
  }

  void CheckMemtableFull() {
    if (flush_scheduler_ != nullptr) {
      auto* cfd = cf_mems_->current();
      assert(cfd != nullptr);
      if (cfd->mem()->ShouldScheduleFlush() &&
          cfd->mem()->MarkFlushScheduled()) {
        // MarkFlushScheduled only returns true if we are the one that
        // should take action, so no need to dedup further
        flush_scheduler_->ScheduleFlush(cfd);
      }
    }
  }

  Status MarkBeginPrepare() override {
    assert(rebuilding_trx_ == nullptr);
    assert(db_);

    if (recovering_log_number_ != 0) {
      // during recovery we rebuild a hollow transaction
      // from all encountered prepare sections of the wal
      if (db_->allow_2pc() == false) {
        return Status::NotSupported(
            "WAL contains prepared transactions. Open with "
            "TransactionDB::Open().");
      }

      // we are now iterating through a prepared section
      rebuilding_trx_ = new WriteBatch();
    } else {
      // in non-recovery we ignore prepare markers
      // and insert the values directly. making sure we have a
      // log for each insertion to reference.
      assert(log_number_ref_ > 0);
    }

    return Status::OK();
  }

  Status MarkEndPrepare(const Slice& name) override {
    assert(db_);
    assert((rebuilding_trx_ != nullptr) == (recovering_log_number_ != 0));

    if (recovering_log_number_ != 0) {
      assert(db_->allow_2pc());
      db_->InsertRecoveredTransaction(recovering_log_number_, name.ToString(),
                                      rebuilding_trx_);
      rebuilding_trx_ = nullptr;
    } else {
      assert(rebuilding_trx_ == nullptr);
      assert(log_number_ref_ > 0);
    }

    return Status::OK();
  }

  Status MarkCommit(const Slice& name) override {
    assert(db_);

    Status s;

    if (recovering_log_number_ != 0) {
      // in recovery when we encounter a commit marker
      // we lookup this transaction in our set of rebuilt transactions
      // and commit.
      auto trx = db_->GetRecoveredTransaction(name.ToString());

      // the log containing the prepared section may have
      // been released in the last incarnation because the
      // data was flushed to L0
      if (trx != nullptr) {
        // at this point individual CF lognumbers will prevent
        // duplicate re-insertion of values.
        assert(log_number_ref_ == 0);
        // all insertes must refernce this trx log number
        log_number_ref_ = trx->log_number_;
        s = trx->batch_->Iterate(this);
        log_number_ref_ = 0;

        if (s.ok()) {
          db_->DeleteRecoveredTransaction(name.ToString());
        }
      }
    } else {
      // in non recovery we simply ignore this tag
    }

    return s;
  }

  Status MarkRollback(const Slice& name) override {
    assert(db_);

    if (recovering_log_number_ != 0) {
      auto trx = db_->GetRecoveredTransaction(name.ToString());

      // the log containing the transactions prep section
      // may have been released in the previous incarnation
      // because we knew it had been rolled back
      if (trx != nullptr) {
        db_->DeleteRecoveredTransaction(name.ToString());
      }
    } else {
      // in non recovery we simply ignore this tag
    }

    return Status::OK();
  }
};

// This function can only be called in these conditions:
// 1) During Recovery()
// 2) During Write(), in a single-threaded write thread
// 3) During Write(), in a concurrent context where memtables has been cloned
// The reason is that it calls memtables->Seek(), which has a stateful cache
Status WriteBatchInternal::InsertInto(
    const std::vector<WriteThread::Writer*>& writers, SequenceNumber sequence,
    ColumnFamilyMemTables* memtables, FlushScheduler* flush_scheduler,
    bool ignore_missing_column_families, uint64_t log_number, DB* db,
    bool concurrent_memtable_writes) {
  MemTableInserter inserter(sequence, memtables, flush_scheduler,
                            ignore_missing_column_families, log_number, db,
                            concurrent_memtable_writes);
  for (size_t i = 0; i < writers.size(); i++) {
    auto w = writers[i];
    if (!w->ShouldWriteToMemtable()) {
      continue;
    }
    inserter.set_log_number_ref(w->log_ref);
    w->status = w->batch->Iterate(&inserter);
    if (!w->status.ok()) {
      return w->status;
    }
  }
  return Status::OK();
}

Status WriteBatchInternal::InsertInto(WriteThread::Writer* writer,
                                      ColumnFamilyMemTables* memtables,
                                      FlushScheduler* flush_scheduler,
                                      bool ignore_missing_column_families,
                                      uint64_t log_number, DB* db,
                                      bool concurrent_memtable_writes) {
  MemTableInserter inserter(WriteBatchInternal::Sequence(writer->batch),
                            memtables, flush_scheduler,
                            ignore_missing_column_families, log_number, db,
                            concurrent_memtable_writes);
  assert(writer->ShouldWriteToMemtable());
  inserter.set_log_number_ref(writer->log_ref);
  return writer->batch->Iterate(&inserter);
}

Status WriteBatchInternal::InsertInto(const WriteBatch* batch,
                                      ColumnFamilyMemTables* memtables,
                                      FlushScheduler* flush_scheduler,
                                      bool ignore_missing_column_families,
                                      uint64_t log_number, DB* db,
                                      bool concurrent_memtable_writes,
                                      SequenceNumber* last_seq_used) {
  MemTableInserter inserter(WriteBatchInternal::Sequence(batch), memtables,
                            flush_scheduler, ignore_missing_column_families,
                            log_number, db, concurrent_memtable_writes);
  Status s = batch->Iterate(&inserter);
  if (last_seq_used != nullptr) {
    *last_seq_used = inserter.get_final_sequence();
  }
  return s;
}

void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= WriteBatchInternal::kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= WriteBatchInternal::kHeader);
  dst->rep_.append(src->rep_.data() + WriteBatchInternal::kHeader,
    src->rep_.size() - WriteBatchInternal::kHeader);
}

size_t WriteBatchInternal::AppendedByteSize(size_t leftByteSize,
                                            size_t rightByteSize) {
  if (leftByteSize == 0 || rightByteSize == 0) {
    return leftByteSize + rightByteSize;
  } else {
    return leftByteSize + rightByteSize - WriteBatchInternal::kHeader;
  }
}

}  // namespace vidardb
