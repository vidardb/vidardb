//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.


#include "db/db_impl_readonly.h"

#include "db/db_impl.h"
#include "db/db_iter.h"
#include "util/perf_context_imp.h"

namespace vidardb {

#ifndef VIDARDB_LITE

DBImplReadOnly::DBImplReadOnly(const DBOptions& db_options,
                               const std::string& dbname)
    : DBImpl(db_options, dbname) {
  Log(INFO_LEVEL, db_options_.info_log, "Opening the db in read only mode");
  LogFlush(db_options_.info_log);
}

DBImplReadOnly::~DBImplReadOnly() {
}

// Implementations of the DB interface
Status DBImplReadOnly::Get(ReadOptions& read_options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           std::string* value) {
  Status s;
  SequenceNumber snapshot = versions_->LastSequence();
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  SuperVersion* super_version = cfd->GetSuperVersion();
  LookupKey lkey(key, snapshot);
  if (super_version->mem->Get(read_options, lkey, value, &s)) {
  } else {
    PERF_TIMER_GUARD(get_from_output_files_time);
    super_version->current->Get(read_options, lkey, value, &s);
  }
  return s;
}

Iterator* DBImplReadOnly::NewIterator(const ReadOptions& read_options,
                                      ColumnFamilyHandle* column_family) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  SuperVersion* super_version = cfd->GetSuperVersion()->Ref();
  SequenceNumber latest_snapshot = versions_->LastSequence();
  auto db_iter = NewArenaWrappedDbIterator(
      env_, *cfd->ioptions(), cfd->user_comparator(),
      (read_options.snapshot != nullptr
           ? reinterpret_cast<const SnapshotImpl*>(read_options.snapshot)
                 ->number_
           : latest_snapshot),
      super_version->mutable_cf_options.max_sequential_skip_in_iterations,
      super_version->version_number);
  auto internal_iter = NewInternalIterator(
      read_options, cfd, super_version, db_iter->GetArena());
  db_iter->SetIterUnderDBIter(internal_iter);
  return db_iter;
}

Status DB::OpenForReadOnly(const Options& options, const std::string& dbname,
                           DB** dbptr, bool error_if_log_file_exist) {
  *dbptr = nullptr;

  Status s;

  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;

  s = DB::OpenForReadOnly(db_options, dbname, column_families, &handles, dbptr);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a
    // reference to default column family
    delete handles[0];
  }
  return s;
}

Status DB::OpenForReadOnly(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, DB** dbptr,
    bool error_if_log_file_exist) {
  *dbptr = nullptr;
  handles->clear();

  DBImplReadOnly* impl = new DBImplReadOnly(db_options, dbname);
  impl->mutex_.Lock();
  Status s = impl->Recover(column_families, true /* read only */,
                           error_if_log_file_exist);
  if (s.ok()) {
    // set column family handles
    for (auto cf : column_families) {
      auto cfd =
          impl->versions_->GetColumnFamilySet()->GetColumnFamily(cf.name);
      if (cfd == nullptr) {
        s = Status::InvalidArgument("Column family not found: ", cf.name);
        break;
      }
      handles->push_back(new ColumnFamilyHandleImpl(cfd, impl, &impl->mutex_));
    }
  }
  if (s.ok()) {
    for (auto cfd : *impl->versions_->GetColumnFamilySet()) {
      delete cfd->InstallSuperVersion(new SuperVersion(), &impl->mutex_);
    }
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    *dbptr = impl;
    for (auto* h : *handles) {
      impl->NewThreadStatusCfInfo(
          reinterpret_cast<ColumnFamilyHandleImpl*>(h)->cfd());
    }
  } else {
    for (auto h : *handles) {
      delete h;
    }
    handles->clear();
    delete impl;
  }
  return s;
}

#else  // !VIDARDB_LITE

Status DB::OpenForReadOnly(const Options& options, const std::string& dbname,
                           DB** dbptr, bool error_if_log_file_exist) {
  return Status::NotSupported("Not supported in VIDARDB_LITE.");
}

Status DB::OpenForReadOnly(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, DB** dbptr,
    bool error_if_log_file_exist) {
  return Status::NotSupported("Not supported in VIDARDB_LITE.");
}
#endif  // !VIDARDB_LITE

}   // namespace vidardb
