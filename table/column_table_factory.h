//  Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#include <stdint.h>

#include <memory>
#include <string>

#include "db/dbformat.h"
#include "vidardb/flush_block_policy.h"
#include "vidardb/table.h"

namespace vidardb {

using std::unique_ptr;

class ColumnTableFactory : public TableFactory {
 public:
  explicit ColumnTableFactory(
      const ColumnTableOptions& table_options = ColumnTableOptions());

  ~ColumnTableFactory() {}

  const char* Name() const override { return "ColumnTable"; }

  Status NewTableReader(const TableReaderOptions& table_reader_options,
                        unique_ptr<RandomAccessFileReader>&& file,
                        uint64_t file_size,
                        unique_ptr<TableReader>* table_reader) const override;

  // This is a variant of virtual member function NewTableReader function with
  // added capability to disable pre-fetching of blocks on ColumnTable::Open
  Status NewTableReader(const TableReaderOptions& table_reader_options,
                        unique_ptr<RandomAccessFileReader>&& file,
                        uint64_t file_size,
                        unique_ptr<TableReader>* table_reader,
                        bool prefetch_enabled) const;

  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      uint32_t column_family_id, WritableFileWriter* file) const override;

  // Sanitizes the specified DB Options.
  Status SanitizeOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override;

  std::string GetPrintableTableOptions() const override;

  const ColumnTableOptions& table_options() const;

  void* GetOptions() override { return &table_options_; }

 private:
  ColumnTableOptions table_options_;
};

}  // namespace vidardb
