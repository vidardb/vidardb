//  Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "table/column_table_factory.h"

#include <memory>
#include <string>
#include <stdint.h>

#include "port/port.h"
#include "vidardb/flush_block_policy.h"
#include "vidardb/cache.h"
#include "vidardb/splitter.h"
#include "table/column_table_builder.h"
#include "table/column_table_reader.h"
#include "table/format.h"

namespace vidardb {

ColumnTableFactory::ColumnTableFactory(
    const ColumnTableOptions& table_options)
    : table_options_(table_options) {
  if (table_options_.flush_block_policy_factory == nullptr) {
    table_options_.flush_block_policy_factory.reset(
        new FlushBlockBySizePolicyFactory());
  }
  if (table_options_.no_block_cache) {
    table_options_.block_cache.reset();
  } else if (table_options_.block_cache == nullptr) {
    table_options_.block_cache = NewLRUCache(8 << 20);
  }
  if (table_options_.block_size_deviation < 0 ||
      table_options_.block_size_deviation > 100) {
    table_options_.block_size_deviation = 0;
  }
  if (table_options_.block_restart_interval < 1) {
    table_options_.block_restart_interval = 1;
  }
  if (table_options_.index_block_restart_interval < 1) {
    table_options_.index_block_restart_interval = 1;
  }
  if (table_options_.splitter == nullptr) {
    table_options_.splitter = PipeSplitter();
  }
}

Status ColumnTableFactory::NewTableReader(
    const TableReaderOptions& table_reader_options,
    unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    unique_ptr<TableReader>* table_reader) const {
  return NewTableReader(table_reader_options, std::move(file), file_size,
                        table_reader, true);
}

Status ColumnTableFactory::NewTableReader(
    const TableReaderOptions& table_reader_options,
    unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    unique_ptr<TableReader>* table_reader, bool prefetch_enabled) const {
  return ColumnTable::Open(
      table_reader_options.ioptions, table_reader_options.env_options,
      table_options_, table_reader_options.internal_comparator, std::move(file),
      file_size, table_reader, prefetch_enabled, table_reader_options.level,
      table_reader_options.cols);
}

TableBuilder* ColumnTableFactory::NewTableBuilder(
    const TableBuilderOptions& table_builder_options, uint32_t column_family_id,
    WritableFileWriter* file) const {
  auto table_builder = new ColumnTableBuilder(
      table_builder_options.ioptions, table_options_,
      table_builder_options.internal_comparator,
      table_builder_options.int_tbl_prop_collector_factories, column_family_id,
      file, table_builder_options.compression_type,
      table_builder_options.compression_opts,
      table_builder_options.compression_dict,
      table_builder_options.column_family_name,
      table_builder_options.env_options);

  return table_builder;
}

Status ColumnTableFactory::SanitizeOptions(
    const DBOptions& db_opts, const ColumnFamilyOptions& cf_opts) const {
  return Status::OK();
}

std::string ColumnTableFactory::GetPrintableTableOptions() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];

  snprintf(buffer, kBufferSize, "  flush_block_policy_factory: %s (%p)\n",
           table_options_.flush_block_policy_factory->Name(),
           static_cast<void*>(table_options_.flush_block_policy_factory.get()));
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  no_block_cache: %d\n",
           table_options_.no_block_cache);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  block_cache: %p\n",
           static_cast<void*>(table_options_.block_cache.get()));
  ret.append(buffer);
  if (table_options_.block_cache) {
    snprintf(buffer, kBufferSize, "  block_cache_size: %" VIDARDB_PRIszt "\n",
             table_options_.block_cache->GetCapacity());
    ret.append(buffer);
  }
  snprintf(buffer, kBufferSize, "  block_size: %" VIDARDB_PRIszt "\n",
           table_options_.block_size);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  block_size_deviation: %d\n",
           table_options_.block_size_deviation);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  block_restart_interval: %d\n",
           table_options_.block_restart_interval);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  index_block_restart_interval: %d\n",
           table_options_.index_block_restart_interval);
  ret.append(buffer);
  return ret;
}

const ColumnTableOptions& ColumnTableFactory::table_options() const {
  return table_options_;
}

TableFactory* NewColumnTableFactory(
    const ColumnTableOptions& table_options) {
  return new ColumnTableFactory(table_options);
}

}  // namespace vidardb
