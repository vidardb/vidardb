//  Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// We support two types of tables: block-based table and column table.
//   1. Block-based table: this is the default table type that we inherited from
//      LevelDB, which was designed for storing data in hard disk or flash
//      device.
//   2. column table: it is the column store implementation which suits for
//      analysis workloads.

#pragma once
#include <memory>
#include <string>
#include <unordered_map>

#include "vidardb/env.h"
#include "vidardb/immutable_options.h"
#include "vidardb/iterator.h"
#include "vidardb/options.h"
#include "vidardb/status.h"
#include "vidardb/splitter.h"

namespace vidardb {

// -- Block-based Table
class FlushBlockPolicyFactory;
class RandomAccessFile;
struct TableReaderOptions;
struct TableBuilderOptions;
class TableBuilder;
class TableReader;
class WritableFileWriter;
struct EnvOptions;
struct Options;

using std::unique_ptr;

// For advanced user only
struct BlockBasedTableOptions {
  // @flush_block_policy_factory creates the instances of flush block policy.
  // which provides a configurable way to determine when to flush a block in
  // the block based tables.  If not set, table builder will use the default
  // block flush policy, which cut blocks by block size (please refer to
  // `FlushBlockBySizePolicy`).
  std::shared_ptr<FlushBlockPolicyFactory> flush_block_policy_factory;

  // Disable block cache. If this is set to true,
  // then no block cache should be used, and the block_cache should
  // point to a nullptr object.
  bool no_block_cache = false;

  // If non-NULL use the specified cache for blocks.
  // If NULL, vidardb will automatically create and use an 8MB internal cache.
  std::shared_ptr<Cache> block_cache = nullptr;

  // Approximate size of user data packed per block. Note that the
  // block size specified here corresponds to uncompressed data. The
  // actual size of the unit read from disk may be smaller if
  // compression is enabled. This parameter can be changed dynamically.
  size_t block_size = 4 * 1024;

  // This is used to close a block before it reaches the configured
  // 'block_size'. If the percentage of free space in the current block is less
  // than this specified number and adding a new record to the block will
  // exceed the configured block size, then this block will be closed and the
  // new record will be written to the next block.
  int block_size_deviation = 10;

  // Number of keys between restart points for delta encoding of keys.
  // This parameter can be changed dynamically. Most clients should
  // leave this parameter alone. The minimum value allowed is 1. Any smaller
  // value will be silently overwritten with 1.
  int block_restart_interval = 16;

  // Same as block_restart_interval but used for the index block.
  int index_block_restart_interval = 1;
};

// Create default block based table factory.
extern TableFactory* NewBlockBasedTableFactory(
    const BlockBasedTableOptions& table_options = BlockBasedTableOptions());

/*********************************  Shichao  **********************************/
// For advanced user only
struct ColumnTableOptions {
  // @flush_block_policy_factory creates the instances of flush block policy.
  // which provides a configurable way to determine when to flush a block in
  // the block based tables. If not set, table builder will use the default
  // block flush policy, which cut blocks by block size (please refer to
  // `FlushBlockBySizePolicy`).
  std::shared_ptr<FlushBlockPolicyFactory> flush_block_policy_factory;

  // Disable block cache. If this is set to true,
  // then no block cache should be used, and the block_cache should
  // point to a nullptr object.
  bool no_block_cache = false;

  // If non-NULL use the specified cache for blocks.
  // If NULL, vidardb will automatically create and use an 8MB internal cache.
  std::shared_ptr<Cache> block_cache = nullptr;

  // Approximate size of user data packed per block. Note that the
  // block size specified here corresponds to uncompressed data. The
  // actual size of the unit read from disk may be smaller if
  // compression is enabled. This parameter can be changed dynamically.
  size_t block_size = 4 * 1024;

  // This is used to close a block before it reaches the configured
  // 'block_size'. If the percentage of free space in the current block is less
  // than this specified number and adding a new record to the block will
  // exceed the configured block size, then this block will be closed and the
  // new record will be written to the next block.
  int block_size_deviation = 10;

  // Number of keys between restart points for delta encoding of keys.
  // This parameter can be changed dynamically. Most clients should
  // leave this parameter alone. The minimum value allowed is 1. Any smaller
  // value will be silently overwritten with 1.
  int block_restart_interval = 16;

  // Same as block_restart_interval but used for the index block.
  int index_block_restart_interval = 1;

  // Splitter for attributes excluding key, default is EncodingSplitter
  std::shared_ptr<Splitter> splitter;

  // Total column number excluding key
  uint32_t column_count = 0;
};

// Create default column table factory.
extern TableFactory* NewColumnTableFactory(
    const ColumnTableOptions& table_options = ColumnTableOptions());
/*********************************  Shichao  ************************************/

class RandomAccessFileReader;

// A base class for table factories.
class TableFactory {
 public:
  virtual ~TableFactory() {}

  // The type of the table.
  //
  // The client of this package should switch to a new name whenever
  // the table format implementation changes.
  //
  // Names starting with "vidardb." are reserved and should not be used
  // by any clients of this package.
  virtual const char* Name() const = 0;

  // Returns a Table object table that can fetch data from file specified
  // in parameter file. It's the caller's responsibility to make sure
  // file is in the correct format.
  //
  // NewTableReader() is called in three places:
  // (1) TableCache::FindTable() calls the function when table cache miss
  //     and cache the table object returned.
  // (2) SstFileReader (for SST Dump) opens the table and dump the table
  //     contents using the interator of the table.
  // (3) DBImpl::AddFile() calls this function to read the contents of
  //     the sst file it's attempting to add
  //
  // table_reader_options is a TableReaderOptions which contain all the
  //    needed parameters and configuration to open the table.
  // file is a file handler to handle the file for the table.
  // file_size is the physical file size of the file.
  // table_reader is the output table reader.
  virtual Status NewTableReader(
      const TableReaderOptions& table_reader_options,
      unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      unique_ptr<TableReader>* table_reader) const = 0;

  // Return a table builder to write to a file for this table type.
  //
  // It is called in several places:
  // (1) When flushing memtable to a level-0 output file, it creates a table
  //     builder (In DBImpl::WriteLevel0Table(), by calling BuildTable())
  // (2) During compaction, it gets the builder for writing compaction output
  //     files in DBImpl::OpenCompactionOutputFile().
  // (3) When recovering from transaction logs, it creates a table builder to
  //     write to a level-0 output file (In DBImpl::WriteLevel0TableForRecovery,
  //     by calling BuildTable())
  // (4) When running Repairer, it creates a table builder to convert logs to
  //     SST files (In Repairer::ConvertLogToTable() by calling BuildTable())
  //
  // ImmutableCFOptions is a subset of Options that can not be altered.
  // Multiple configured can be accessed from there, including and not limited
  // to compression options. file is a handle of a writable file.
  // It is the caller's responsibility to keep the file open and close the file
  // after closing the table builder. compression_type is the compression type
  // to use in this table.
  virtual TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      uint32_t column_family_id, WritableFileWriter* file) const = 0;

  // Sanitizes the specified DB Options and ColumnFamilyOptions.
  //
  // If the function cannot find a way to sanitize the input DB Options,
  // a non-ok Status will be returned.
  virtual Status SanitizeOptions(
      const DBOptions& db_opts,
      const ColumnFamilyOptions& cf_opts) const = 0;

  // Return a string that contains printable format of table configurations.
  // VidarDB prints configurations at DB Open().
  virtual std::string GetPrintableTableOptions() const = 0;

  // Returns the raw pointer of the table options that is used by this
  // TableFactory, or nullptr if this function is not supported.
  // Since the return value is a raw pointer, the TableFactory owns the
  // pointer and the caller should not delete the pointer.
  //
  // In certain case, it is desirable to alter the underlying options when the
  // TableFactory is not used by any open DB by casting the returned pointer
  // to the right class. For instance, if BlockBasedTableFactory is used,
  // then the pointer can be casted to BlockBasedTableOptions.
  //
  // Note that changing the underlying TableFactory options while the
  // TableFactory is currently used by any open DB is undefined behavior.
  // Developers should use DB::SetOption() instead to dynamically change
  // options while the DB is open.
  virtual void* GetOptions() { return nullptr; }
};

#ifndef VIDARDB_LITE
// Create a special table factory that can open either of the supported
// table formats, based on setting inside the SST files. It should be used to
// convert a DB from one table format to another.
// @table_factory_to_write: the table factory used when writing to new files.
// @block_based_table_factory:  block based table factory to use. If NULL, use
//                              a default one.
// @column_table_factory: column table factory to use. If NULL, use a default one.
// @knob: starting from which level to use column table factory. Default value 
//        is -1 which means using the default @table_factory_to_write.
extern TableFactory* NewAdaptiveTableFactory(
    std::shared_ptr<TableFactory> table_factory_to_write = nullptr,
    std::shared_ptr<TableFactory> block_based_table_factory = nullptr,
    std::shared_ptr<TableFactory> column_table_factory = nullptr,  // Shichao
    int knob = -1);                                                // Shichao

#endif  // VIDARDB_LITE

}  // namespace vidardb
