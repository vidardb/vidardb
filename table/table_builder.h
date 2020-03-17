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

#include <stdint.h>

#include <string>
#include <utility>
#include <vector>

#include "db/table_properties_collector.h"
#include "util/file_reader_writer.h"
#include "util/mutable_cf_options.h"
#include "vidardb/options.h"
#include "vidardb/table_properties.h"

namespace vidardb {

class Slice;
class Status;

struct TableReaderOptions {
  TableReaderOptions(
      const ImmutableCFOptions& _ioptions, const EnvOptions& _env_options,
      const InternalKeyComparator& _internal_comparator, int _level = -1,
      const std::vector<uint32_t>& _cols = std::vector<uint32_t>())  // Shichao
      : ioptions(_ioptions),
        env_options(_env_options),
        internal_comparator(_internal_comparator),
        level(_level),
        cols(_cols) {}  // Shichao

  const ImmutableCFOptions& ioptions;
  const EnvOptions& env_options;
  const InternalKeyComparator& internal_comparator;
  // what level this table/file is on, -1 for "not set, don't know"
  int level;
  std::vector<uint32_t> cols;  // Shichao
};

struct TableBuilderOptions {
  TableBuilderOptions(
      const ImmutableCFOptions& _ioptions,
      const InternalKeyComparator& _internal_comparator,
      const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
          _int_tbl_prop_collector_factories,
      CompressionType _compression_type,
      const CompressionOptions& _compression_opts,
      const std::string* _compression_dict,
      const std::string& _column_family_name,
      const EnvOptions& _env_options)  // Shichao
      : ioptions(_ioptions),
        internal_comparator(_internal_comparator),
        int_tbl_prop_collector_factories(_int_tbl_prop_collector_factories),
        compression_type(_compression_type),
        compression_opts(_compression_opts),
        compression_dict(_compression_dict),
        column_family_name(_column_family_name),
        env_options(_env_options) {}  // Shichao
  const ImmutableCFOptions& ioptions;
  const InternalKeyComparator& internal_comparator;
  const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
      int_tbl_prop_collector_factories;
  CompressionType compression_type;
  const CompressionOptions& compression_opts;
  // Data for presetting the compression library's dictionary, or nullptr.
  const std::string* compression_dict;
  const std::string& column_family_name;
  const EnvOptions& env_options;  // Shichao, only used by ColumnTableBuilder
};

// TableBuilder provides the interface used to build a Table
// (an immutable and sorted map from keys to values).
//
// Multiple threads can invoke const methods on a TableBuilder without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same TableBuilder must use
// external synchronization.
class TableBuilder {
 public:
  // REQUIRES: Either Finish() or Abandon() has been called.
  virtual ~TableBuilder() {}

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  virtual void Add(const Slice& key, const Slice& value) = 0;

  // Return non-ok iff some error has been detected.
  virtual Status status() const = 0;

  // Finish building the table.
  // REQUIRES: Finish(), Abandon() have not been called
  virtual Status Finish() = 0;

  // Indicate that the contents of this builder should be abandoned.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  virtual void Abandon() = 0;

  // Number of calls to Add() so far.
  virtual uint64_t NumEntries() const = 0;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  virtual uint64_t FileSize() const = 0;

  // For compaction calculation purpose. For row based, should be equal to
  // FileSize(), while for column equals to all column size + meta size.
  virtual uint64_t FileSizeTotal() const { return FileSize(); }  // Shichao

  // If the user defined table properties collector suggest the file to
  // be further compacted.
  virtual bool NeedCompact() const { return false; }

  // Returns table properties
  virtual TableProperties GetTableProperties() const = 0;

  virtual const char* Name() const { return ""; }  // Shichao
};

}  // namespace vidardb
