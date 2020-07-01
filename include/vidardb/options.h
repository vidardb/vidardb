//  Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_VIDARDB_INCLUDE_OPTIONS_H_
#define STORAGE_VIDARDB_INCLUDE_OPTIONS_H_

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "vidardb/listener.h"
#include "vidardb/splitter.h"
#include "vidardb/universal_compaction.h"
#include "vidardb/version.h"

namespace vidardb {

class Cache;
class CompactionFilter;
class CompactionFilterFactory;
class Comparator;
class Env;
enum InfoLogLevel : unsigned char;
class SstFileManager;
class Logger;
class Snapshot;
class TableFactory;
class MemTableRepFactory;
class TablePropertiesCollectorFactory;
class Slice;
class Statistics;
class InternalKeyComparator;
class WalFilter;
class Splitter;

// DB contents are stored in a set of blocks, each of which holds a
// sequence of key,value pairs.  Each block may be compressed before
// being stored in a file.  The following enum describes which
// compression method (if any) is used to compress a block.
enum CompressionType : char {
  // NOTE: do not change the values of existing entries, as these are
  // part of the persistent format on disk.
  kNoCompression = 0x0,
  kSnappyCompression = 0x1,
  kZlibCompression = 0x2,
  kBZip2Compression = 0x3,

  // kDisableCompressionOption is used to disable some compression options.
  kDisableCompressionOption = -1,
};

enum CompactionStyle : char {
  // level based compaction style
  kCompactionStyleLevel = 0x0,
  // Universal compaction style
  // Not supported in VIDARDB_LITE.
  kCompactionStyleUniversal = 0x1,
  // FIFO compaction style
  // Not supported in VIDARDB_LITE
  kCompactionStyleFIFO = 0x2,
  // Disable background compaction. Compaction jobs are submitted
  // via CompactFiles().
  // Not supported in VIDARDB_LITE
  kCompactionStyleNone = 0x3,
};

// In Level-based comapction, it Determines which file from a level to be
// picked to merge to the next level. We suggest people try
// kMinOverlappingRatio first when you tune your database.
enum CompactionPri : char {
  // Slightly Priotize larger files by size compensated by #deletes
  kByCompensatedSize = 0x0,
  // First compact files whose data's latest update time is oldest.
  // Try this if you only update some hot keys in small ranges.
  kOldestLargestSeqFirst = 0x1,
  // First compact files whose range hasn't been compacted to the next level
  // for the longest. If your updates are random across the key space,
  // write amplification is slightly better with this option.
  kOldestSmallestSeqFirst = 0x2,
  // First compact files whose ratio between overlapping size in next level
  // and its size is the smallest. It in many cases can optimize write
  // amplification.
  kMinOverlappingRatio = 0x3,
};

enum class WALRecoveryMode : char {
  // Original levelDB recovery
  // We tolerate incomplete record in trailing data on all logs
  // Use case : This is legacy behavior (default)
  kTolerateCorruptedTailRecords = 0x00,
  // Recover from clean shutdown
  // We don't expect to find any corruption in the WAL
  // Use case : This is ideal for unit tests and rare applications that
  // can require high consistency guarantee
  kAbsoluteConsistency = 0x01,
  // Recover to point-in-time consistency
  // We stop the WAL playback on discovering WAL inconsistency
  // Use case : Ideal for systems that have disk controller cache like
  // hard disk, SSD without super capacitor that store related data
  kPointInTimeRecovery = 0x02,
  // Recovery after a disaster
  // We ignore any corruption in the WAL and try to salvage as much data as
  // possible
  // Use case : Ideal for last ditch effort to recover data or systems that
  // operate with low grade unrelated data
  kSkipAnyCorruptedRecords = 0x03,
};

struct CompactionOptionsFIFO {
  // once the total sum of table files reaches this, we will delete the oldest
  // table file
  // Default: 1GB
  uint64_t max_table_files_size;

  CompactionOptionsFIFO() : max_table_files_size(1 * 1024 * 1024 * 1024) {}
};

// Compression options for different compression algorithms like Zlib
struct CompressionOptions {
  int window_bits;
  int level;
  int strategy;
  // Maximum size of dictionary used to prime the compression library. Currently
  // this dictionary will be constructed by sampling the first output file in a
  // subcompaction when the target level is bottommost. This dictionary will be
  // loaded into the compression library before compressing/uncompressing each
  // data block of subsequent files in the subcompaction. Effectively, this
  // improves compression ratios when there are repetitions across data blocks.
  // A value of 0 indicates the feature is disabled.
  // Default: 0.
  uint32_t max_dict_bytes;

  CompressionOptions()
      : window_bits(-14), level(-1), strategy(0), max_dict_bytes(0) {}
  CompressionOptions(int wbits, int _lev, int _strategy, int _max_dict_bytes)
      : window_bits(wbits),
        level(_lev),
        strategy(_strategy),
        max_dict_bytes(_max_dict_bytes) {}
};

struct DbPath {
  std::string path;
  uint64_t target_size;  // Target size of total files under the path, in byte.

  DbPath() : target_size(0) {}
  DbPath(const std::string& p, uint64_t t) : path(p), target_size(t) {}
};

struct Options;

struct ColumnFamilyOptions {
  // Some functions that make it easier to optimize VidarDB
  // Use this if your DB is very small (like under 1GB) and you don't want to
  // spend lots of memory for memtables.
  ColumnFamilyOptions* OptimizeForSmallDb();

  // Adaptive table factory should not be enabled in this case.
  //
  // Not supported in VIDARDB_LITE
  ColumnFamilyOptions* OptimizeForPointLookup(
      uint64_t block_cache_size_mb);

  // Default values for some parameters in ColumnFamilyOptions are not
  // optimized for heavy workloads and big datasets, which means you might
  // observe write stalls under some conditions. As a starting point for tuning
  // VidarDB options, use the following three functions:
  // * OptimizeLevelStyleCompaction -- optimizes level style compaction
  // * OptimizeUniversalStyleCompaction -- optimizes universal style compaction
  // * OptimizeAdaptiveLevelStyleCompaction -- optimizes level style compaction
  //                                           for adaptive table storage
  // Universal style compaction is focused on reducing Write Amplification
  // Factor for big data sets, but increases Space Amplification.
  // Make sure to also call IncreaseParallelism(), which will provide the
  // biggest performance gains.
  // Note: we might use more memory than memtable_memory_budget during high
  // write rate period
  //
  // OptimizeUniversalStyleCompaction is not supported in VIDARDB_LITE
  ColumnFamilyOptions* OptimizeLevelStyleCompaction(
      uint64_t memtable_memory_budget = 512 * 1024 * 1024);
  ColumnFamilyOptions* OptimizeUniversalStyleCompaction(
      uint64_t memtable_memory_budget = 512 * 1024 * 1024);
  /********************* Shichao ***************************/
  ColumnFamilyOptions* OptimizeAdaptiveLevelStyleCompaction(
        uint64_t memtable_memory_budget = 512 * 1024 * 1024);
  /********************* Shichao ***************************/

  // -------------------
  // Parameters that affect behavior

  // Comparator used to define the order of keys in the table.
  // Default: a comparator that uses lexicographic byte-wise ordering
  //
  // REQUIRES: The client must ensure that the comparator supplied
  // here has the same name and orders keys *exactly* the same as the
  // comparator provided to previous open calls on the same DB.
  const Comparator* comparator;

  // Splitter used to reformat the values in the table.
  //
  // REQUIRES: The client must ensure that the splitter is initialized
  // for columnar storage.
  std::shared_ptr<Splitter> splitter;

  // -------------------
  // Parameters that affect performance

  // Amount of data to build up in memory (backed by an unsorted log
  // on disk) before converting to a sorted on-disk file.
  //
  // Larger values increase performance, especially during bulk loads.
  // Up to max_write_buffer_number write buffers may be held in memory
  // at the same time,
  // so you may wish to adjust this parameter to control memory usage.
  // Also, a larger write buffer will result in a longer recovery time
  // the next time the database is opened.
  //
  // Note that write_buffer_size is enforced per column family.
  // See db_write_buffer_size for sharing memory across column families.
  //
  // Default: 64MB
  //
  // Dynamically changeable through SetOptions() API
  size_t write_buffer_size;

  // The maximum number of write buffers that are built up in memory.
  // The default and the minimum number is 2, so that when 1 write buffer
  // is being flushed to storage, new writes can continue to the other
  // write buffer.
  // If max_write_buffer_number > 3, writing will be slowed down to
  // options.delayed_write_rate if we are writing to the last write buffer
  // allowed.
  //
  // Default: 2
  //
  // Dynamically changeable through SetOptions() API
  int max_write_buffer_number;

  // The minimum number of write buffers that will be merged together
  // before writing to storage.  If set to 1, then
  // all write buffers are flushed to L0 as individual files and this increases
  // read amplification because a get request has to check in all of these
  // files. Also, an in-memory merge may result in writing lesser
  // data to storage if there are duplicate records in each of these
  // individual write buffers.  Default: 1
  int min_write_buffer_number_to_merge;

  // The total maximum number of write buffers to maintain in memory including
  // copies of buffers that have already been flushed.  Unlike
  // max_write_buffer_number, this parameter does not affect flushing.
  // This controls the minimum amount of write history that will be available
  // in memory for conflict checking when Transactions are used.
  //
  // When using a TransactionDB:
  // If Transaction::SetSnapshot is used, TransactionDB will read either
  // in-memory write buffers or SST files to do write-conflict checking.
  // Increasing this value can reduce the number of reads to SST files
  // done for conflict detection.
  //
  // Setting this value to 0 will cause write buffers to be freed immediately
  // after they are flushed.
  // If this value is set to -1, 'max_write_buffer_number' will be used.
  //
  // Default:
  // If using a TransactionDB, the default value will
  // be set to the value of 'max_write_buffer_number' if it is not explicitly
  // set by the user.  Otherwise, the default is 0.
  int max_write_buffer_number_to_maintain;

  // Compress blocks using the specified compression algorithm.  This
  // parameter can be changed dynamically.
  //
  // Default: kSnappyCompression, if it's supported. If snappy is not linked
  // with the library, the default is kNoCompression.
  //
  // Typical speeds of kSnappyCompression on an Intel(R) Core(TM)2 2.4GHz:
  //    ~200-500MB/s compression
  //    ~400-800MB/s decompression
  // Note that these speeds are significantly faster than most
  // persistent storage speeds, and therefore it is typically never
  // worth switching to kNoCompression.  Even if the input data is
  // incompressible, the kSnappyCompression implementation will
  // efficiently detect that and will switch to uncompressed mode.
  CompressionType compression;

  // Different levels can have different compression policies. There
  // are cases where most lower levels would like to use quick compression
  // algorithms while the higher levels (which have more data) use
  // compression algorithms that have better compression but could
  // be slower. This array, if non-empty, should have an entry for
  // each level of the database; these override the value specified in
  // the previous field 'compression'.
  //
  // NOTICE if level_compaction_dynamic_level_bytes=true,
  // compression_per_level[0] still determines L0, but other elements
  // of the array are based on base level (the level L0 files are merged
  // to), and may not match the level users see from info log for metadata.
  // If L0 files are merged to level-n, then, for i>0, compression_per_level[i]
  // determines compaction type for level n+i-1.
  // For example, if we have three 5 levels, and we determine to merge L0
  // data to L4 (which means L1..L3 will be empty), then the new files go to
  // L4 uses compression type compression_per_level[1].
  // If now L0 is merged to L2. Data goes to L2 will be compressed
  // according to compression_per_level[1], L3 using compression_per_level[2]
  // and L4 using compression_per_level[3]. Compaction for each level can
  // change when data grows.
  std::vector<CompressionType> compression_per_level;

  // Compression algorithm that will be used for the bottommost level that
  // contain files. If level-compaction is used, this option will only affect
  // levels after base level.
  //
  // Default: kDisableCompressionOption (Disabled)
  CompressionType bottommost_compression;

  // different options for compression algorithms
  CompressionOptions compression_opts;

  // Number of levels for this database
  int num_levels;

  // Number of files to trigger level-0 compaction. A value <0 means that
  // level-0 compaction will not be triggered by number of files at all.
  //
  // Default: 4
  //
  // Dynamically changeable through SetOptions() API
  int level0_file_num_compaction_trigger;

  // Target file size for compaction.
  // target_file_size_base is per-file size for level-1.
  // Target file size for level L can be calculated by
  // target_file_size_base * (target_file_size_multiplier ^ (L-1))
  // For example, if target_file_size_base is 2MB and
  // target_file_size_multiplier is 10, then each file on level-1 will
  // be 2MB, and each file on level 2 will be 20MB,
  // and each file on level-3 will be 200MB.
  //
  // Default: 64MB.
  //
  // Dynamically changeable through SetOptions() API
  uint64_t target_file_size_base;

  // By default target_file_size_multiplier is 1, which means
  // by default files in different levels will have similar size.
  //
  // Dynamically changeable through SetOptions() API
  int target_file_size_multiplier;

  // Control maximum total data size for a level.
  // max_bytes_for_level_base is the max total for level-1.
  // Maximum number of bytes for level L can be calculated as
  // (max_bytes_for_level_base) * (max_bytes_for_level_multiplier ^ (L-1))
  // For example, if max_bytes_for_level_base is 200MB, and if
  // max_bytes_for_level_multiplier is 10, total data size for level-1
  // will be 2GB, total file size for level-2 will be 20GB,
  // and total file size for level-3 will be 200GB.
  //
  // Default: 256MB.
  //
  // Dynamically changeable through SetOptions() API
  uint64_t max_bytes_for_level_base;

  // Default: 10.
  //
  // Dynamically changeable through SetOptions() API
  int max_bytes_for_level_multiplier;

  // Different max-size multipliers for different levels.
  // These are multiplied by max_bytes_for_level_multiplier to arrive
  // at the max-size of each level.
  //
  // Default: 1
  //
  // Dynamically changeable through SetOptions() API
  std::vector<int> max_bytes_for_level_multiplier_additional;

  // Maximum number of bytes in all compacted files.  We avoid expanding
  // the lower level file set of a compaction if it would make the
  // total compaction cover more than
  // (expanded_compaction_factor * targetFileSizeLevel()) many bytes.
  //
  // Dynamically changeable through SetOptions() API
  int expanded_compaction_factor;

  // Maximum number of bytes in all source files to be compacted in a
  // single compaction run. We avoid picking too many files in the
  // source level so that we do not exceed the total source bytes
  // for compaction to exceed
  // (source_compaction_factor * targetFileSizeLevel()) many bytes.
  // Default:1, i.e. pick maxfilesize amount of data as the source of
  // a compaction.
  //
  // Dynamically changeable through SetOptions() API
  int source_compaction_factor;

  // Control maximum bytes of overlaps in grandparent (i.e., level+2) before we
  // stop building a single file in a level->level+1 compaction.
  //
  // Dynamically changeable through SetOptions() API
  int max_grandparent_overlap_factor;

  // size of one block in arena memory allocation.
  // If <= 0, a proper value is automatically calculated (usually 1/8 of
  // writer_buffer_size, rounded up to a multiple of 4KB).
  //
  // There are two additional restriction of the The specified size:
  // (1) size should be in the range of [4096, 2 << 30] and
  // (2) be the multiple of the CPU word (which helps with the memory
  // alignment).
  //
  // We'll automatically check and adjust the size number to make sure it
  // conforms to the restrictions.
  //
  // Default: 0
  //
  // Dynamically changeable through SetOptions() API
  size_t arena_block_size;

  // Disable automatic compactions. Manual compactions can still
  // be issued on this column family
  //
  // Dynamically changeable through SetOptions() API
  bool disable_auto_compactions;

  // The compaction style. Default: kCompactionStyleLevel
  CompactionStyle compaction_style;

  // If level compaction_style = kCompactionStyleLevel, for each level,
  // which files are prioritized to be picked to compact.
  // Default: kByCompensatedSize
  CompactionPri compaction_pri;

  // If true, compaction will verify checksum on every read that happens
  // as part of compaction
  //
  // Default: true
  //
  // Dynamically changeable through SetOptions() API
  bool verify_checksums_in_compaction;

  // The options needed to support Universal Style compactions
  CompactionOptionsUniversal compaction_options_universal;

  // The options for FIFO compaction style
  CompactionOptionsFIFO compaction_options_fifo;

  // This is a factory that provides MemTableRep objects.
  // Default: a factory that provides a skip-list-based implementation of
  // MemTableRep.
  std::shared_ptr<MemTableRepFactory> memtable_factory;

  // This is a factory that provides TableFactory objects.
  // Default: a block-based table factory that provides a default
  // implementation of TableBuilder and TableReader with default
  // BlockBasedTableOptions.
  std::shared_ptr<TableFactory> table_factory;

  // Block-based table related options are moved to BlockBasedTableOptions.
  // Related options that were originally here but now moved include:
  //   no_block_cache
  //   block_cache
  //   block_cache_compressed
  //   block_size
  //   block_size_deviation
  //   block_restart_interval
  //   filter_policy
  //   whole_key_filtering
  // If you'd like to customize some of these options, you will need to
  // use NewBlockBasedTableFactory() to construct a new table factory.

  // This option allows user to collect their own interested statistics of
  // the tables.
  // Default: empty vector -- no user-defined statistics collection will be
  // performed.
  typedef std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
      TablePropertiesCollectorFactories;
  TablePropertiesCollectorFactories table_properties_collector_factories;

  // After writing every SST file, reopen it and read all the keys.
  // Default: false
  bool paranoid_file_checks;

  // Measure IO stats in compactions and flushes, if true.
  // Default: false
  bool report_bg_io_stats;

  // Create ColumnFamilyOptions with default values for all fields
  ColumnFamilyOptions();
  // Create ColumnFamilyOptions from Options
  explicit ColumnFamilyOptions(const Options& options);

  void Dump(Logger* log) const;
};

struct DBOptions {
  // Some functions that make it easier to optimize VidarDB

  // Use this if your DB is very small (like under 1GB) and you don't want to
  // spend lots of memory for memtables.
  DBOptions* OptimizeForSmallDb();

#ifndef VIDARDB_LITE
  // By default, VidarDB uses only one background thread for flush and
  // compaction. Calling this function will set it up such that total of
  // `total_threads` is used. Good value for `total_threads` is the number of
  // cores. You almost definitely want to call this function if your system is
  // bottlenecked by VidarDB.
  DBOptions* IncreaseParallelism(int total_threads = 16);
#endif  // VIDARDB_LITE

  // If true, the database will be created if it is missing.
  // Default: false
  bool create_if_missing;

  // If true, missing column families will be automatically created.
  // Default: false
  bool create_missing_column_families;

  // If true, an error is raised if the database already exists.
  // Default: false
  bool error_if_exists;

  // If true, VidarDB will aggressively check consistency of the data.
  // Also, if any of the writes to the database fails (Put, Delete,
  // Write), the database will switch to read-only mode and fail all other
  // Write operations.
  // In most cases you want this to be set to true.
  // Default: true
  bool paranoid_checks;

  // Use the specified object to interact with the environment,
  // e.g. to read/write files, schedule background work, etc.
  // Default: Env::Default()
  Env* env;

  // Use to track SST files and control their file deletion rate.
  //
  // Features:
  //  - Throttle the deletion rate of the SST files.
  //  - Keep track the total size of all SST files.
  //  - Set a maximum allowed space limit for SST files that when reached
  //    the DB wont do any further flushes or compactions and will set the
  //    background error.
  //  - Can be shared between multiple dbs.
  // Limitations:
  //  - Only track and throttle deletes of SST files in
  //    first db_path (db_name if db_paths is empty).
  //
  // Default: nullptr
  std::shared_ptr<SstFileManager> sst_file_manager;

  // Any internal progress/error information generated by the db will
  // be written to info_log if it is non-nullptr, or to a file stored
  // in the same directory as the DB contents if info_log is nullptr.
  // Default: nullptr
  std::shared_ptr<Logger> info_log;

  InfoLogLevel info_log_level;

  // Number of open files that can be used by the DB.  You may need to
  // increase this if your database has a large working set. Value -1 means
  // files opened are always kept open. You can estimate number of files based
  // on target_file_size_base and target_file_size_multiplier for level-based
  // compaction. For universal-style compaction, you can usually set it to -1.
  // Default: -1
  int max_open_files;

  // If max_open_files is -1, DB will open all files on DB::Open(). You can
  // use this option to increase the number of threads used to open the files.
  // Default: 16
  int max_file_opening_threads;

  // Once write-ahead logs exceed this size, we will start forcing the flush of
  // column families whose memtables are backed by the oldest live WAL file
  // (i.e. the ones that are causing all the space amplification). If set to 0
  // (default), we will dynamically choose the WAL size limit to be
  // [sum of all write_buffer_size * max_write_buffer_number] * 4
  // Default: 0
  uint64_t max_total_wal_size;

  // If non-null, then we should collect metrics about database operations
  // Statistics objects should not be shared between DB instances as
  // it does not use any locks to prevent concurrent updates.
  std::shared_ptr<Statistics> statistics;

  // If true, then the contents of manifest and data files are not synced
  // to stable storage. Their contents remain in the OS buffers till the
  // OS decides to flush them. This option is good for bulk-loading
  // of data. Once the bulk-loading is complete, please issue a
  // sync to the OS to flush all dirty buffesrs to stable storage.
  // Default: false
  bool disableDataSync;

  // If true, then every store to stable storage will issue a fsync.
  // If false, then every store to stable storage will issue a fdatasync.
  // This parameter should be set to true while storing data to
  // filesystem like ext3 that can lose files after a reboot.
  // Default: false
  bool use_fsync;

  // A list of paths where SST files can be put into, with its target size.
  // Newer data is placed into paths specified earlier in the vector while
  // older data gradually moves to paths specified later in the vector.
  //
  // For example, you have a flash device with 10GB allocated for the DB,
  // as well as a hard drive of 2TB, you should config it to be:
  //   [{"/flash_path", 10GB}, {"/hard_drive", 2TB}]
  //
  // The system will try to guarantee data under each path is close to but
  // not larger than the target size. But current and future file sizes used
  // by determining where to place a file are based on best-effort estimation,
  // which means there is a chance that the actual size under the directory
  // is slightly more than target size under some workloads. User should give
  // some buffer room for those cases.
  //
  // If none of the paths has sufficient room to place a file, the file will
  // be placed to the last path anyway, despite to the target size.
  //
  // Placing newer data to earlier paths is also best-efforts. User should
  // expect user files to be placed in higher levels in some extreme cases.
  //
  // If left empty, only one path will be used, which is db_name passed when
  // opening the DB.
  // Default: empty
  std::vector<DbPath> db_paths;

  // This specifies the info LOG dir.
  // If it is empty, the log files will be in the same dir as data.
  // If it is non empty, the log files will be in the specified dir,
  // and the db data dir's absolute path will be used as the log file
  // name's prefix.
  std::string db_log_dir;

  // This specifies the absolute dir path for write-ahead logs (WAL).
  // If it is empty, the log files will be in the same dir as data,
  //   dbname is used as the data dir by default
  // If it is non empty, the log files will be in kept the specified dir.
  // When destroying the db,
  //   all log files in wal_dir and the dir itself is deleted
  std::string wal_dir;

  // The periodicity when obsolete files get deleted. The default
  // value is 6 hours. The files that get out of scope by compaction
  // process will still get automatically delete on every compaction,
  // regardless of this setting
  uint64_t delete_obsolete_files_period_micros;

  // Suggested number of concurrent background compaction jobs, submitted to
  // the default LOW priority thread pool.
  //
  // Default: 1
  int base_background_compactions;

  // Maximum number of concurrent background compaction jobs, submitted to
  // the default LOW priority thread pool.
  // We first try to schedule compactions based on
  // `base_background_compactions`. If the compaction cannot catch up , we
  // will increase number of compaction threads up to
  // `max_background_compactions`.
  //
  // If you're increasing this, also consider increasing number of threads in
  // LOW priority thread pool. For more information, see
  // Env::SetBackgroundThreads
  // Default: 1
  int max_background_compactions;

  // This value represents the maximum number of threads that will
  // concurrently perform a compaction job by breaking it into multiple,
  // smaller ones that are run simultaneously.
  // Default: 1 (i.e. no subcompactions)
  uint32_t max_subcompactions;

  // Maximum number of concurrent background memtable flush jobs, submitted to
  // the HIGH priority thread pool.
  //
  // By default, all background jobs (major compaction and memtable flush) go
  // to the LOW priority pool. If this option is set to a positive number,
  // memtable flush jobs will be submitted to the HIGH priority pool.
  // It is important when the same Env is shared by multiple db instances.
  // Without a separate pool, long running major compaction jobs could
  // potentially block memtable flush jobs of other db instances, leading to
  // unnecessary Put stalls.
  //
  // If you're increasing this, also consider increasing number of threads in
  // HIGH priority thread pool. For more information, see
  // Env::SetBackgroundThreads
  // Default: 1
  int max_background_flushes;

  // Specify the maximal size of the info log file. If the log file
  // is larger than `max_log_file_size`, a new info log file will
  // be created.
  // If max_log_file_size == 0, all logs will be written to one
  // log file.
  size_t max_log_file_size;

  // Time for the info log file to roll (in seconds).
  // If specified with non-zero value, log file will be rolled
  // if it has been active longer than `log_file_time_to_roll`.
  // Default: 0 (disabled)
  size_t log_file_time_to_roll;

  // Maximal info log files to be kept.
  // Default: 1000
  size_t keep_log_file_num;

  // Recycle log files.
  // If non-zero, we will reuse previously written log files for new
  // logs, overwriting the old data.  The value indicates how many
  // such files we will keep around at any point in time for later
  // use.  This is more efficient because the blocks are already
  // allocated and fdatasync does not need to update the inode after
  // each write.
  // Default: 0
  size_t recycle_log_file_num;

  // manifest file is rolled over on reaching this limit.
  // The older manifest file be deleted.
  // The default value is MAX_INT so that roll-over does not take place.
  uint64_t max_manifest_file_size;

  // Number of shards used for table cache.
  int table_cache_numshardbits;

  // The following two fields affect how archived logs will be deleted.
  // 1. If both set to 0, logs will be deleted asap and will not get into
  //    the archive.
  // 2. If WAL_ttl_seconds is 0 and WAL_size_limit_MB is not 0,
  //    WAL files will be checked every 10 min and if total size is greater
  //    then WAL_size_limit_MB, they will be deleted starting with the
  //    earliest until size_limit is met. All empty files will be deleted.
  // 3. If WAL_ttl_seconds is not 0 and WAL_size_limit_MB is 0, then
  //    WAL files will be checked every WAL_ttl_secondsi / 2 and those that
  //    are older than WAL_ttl_seconds will be deleted.
  // 4. If both are not 0, WAL files will be checked every 10 min and both
  //    checks will be performed with ttl being first.
  uint64_t WAL_ttl_seconds;
  uint64_t WAL_size_limit_MB;

  // Number of bytes to preallocate (via fallocate) the manifest
  // files.  Default is 4mb, which is reasonable to reduce random IO
  // as well as prevent overallocation for mounts that preallocate
  // large amounts of data (such as xfs's allocsize option).
  size_t manifest_preallocation_size;

  // Hint the OS that it should not buffer disk I/O. Enabling this
  // parameter may improve performance but increases pressure on the
  // system cache.
  //
  // The exact behavior of this parameter is platform dependent.
  //
  // On POSIX systems, after VidarDB reads data from disk it will
  // mark the pages as "unneeded". The operating system may - or may not
  // - evict these pages from memory, reducing pressure on the system
  // cache. If the disk block is requested again this can result in
  // additional disk I/O.
  //
  // On WINDOWS system, files will be opened in "unbuffered I/O" mode
  // which means that data read from the disk will not be cached or
  // bufferized. The hardware buffer of the devices may however still
  // be used. Memory mapped files are not impacted by this parameter.
  //
  // Default: true
  bool allow_os_buffer;

  // Allow the OS to mmap file for reading sst tables. Default: false
  bool allow_mmap_reads;

  // Allow the OS to mmap file for writing.
  // DB::SyncWAL() only works if this is set to false.
  // Default: false
  bool allow_mmap_writes;

  // If false, fallocate() calls are bypassed
  bool allow_fallocate;

  // Disable child process inherit open files. Default: true
  bool is_fd_close_on_exec;

  // if not zero, dump vidardb.stats to LOG every stats_dump_period_sec
  // Default: 600 (10 min)
  unsigned int stats_dump_period_sec;

  // If set true, will hint the underlying file system that the file
  // access pattern is random, when a sst file is opened.
  // Default: true
  bool advise_random_on_open;

  // Amount of data to build up in memtables across all column
  // families before writing to disk.
  //
  // This is distinct from write_buffer_size, which enforces a limit
  // for a single memtable.
  //
  // This feature is disabled by default. Specify a non-zero value
  // to enable it.
  //
  // Default: 0 (disabled)
  size_t db_write_buffer_size;

  // Specify the file access pattern once a compaction is started.
  // It will be applied to all input files of a compaction.
  // Default: NORMAL
  enum AccessHint {
      NONE,
      NORMAL,
      SEQUENTIAL,
      WILLNEED
  };
  AccessHint access_hint_on_compaction_start;

  // If true, always create a new file descriptor and new table reader
  // for compaction inputs. Turn this parameter on may introduce extra
  // memory usage in the table reader, if it allocates extra memory
  // for indexes. This will allow file descriptor prefetch options
  // to be set for compaction input files and not to impact file
  // descriptors for the same file used by user queries.
  // Suggest to enable BlockBasedTableOptions.cache_index_and_filter_blocks
  // for this mode if using block-based table.
  //
  // Default: false
  bool new_table_reader_for_compaction_inputs;

  // If non-zero, we perform bigger reads when doing compaction. If you're
  // running VidarDB on spinning disks, you should set this to at least 2MB.
  // That way VidarDB's compaction is doing sequential instead of random reads.
  //
  // When non-zero, we also force new_table_reader_for_compaction_inputs to
  // true.
  //
  // Default: 0
  size_t compaction_readahead_size;

  // This is a maximum buffer size that is used by WinMmapReadableFile in
  // unbuffered disk I/O mode. We need to maintain an aligned buffer for
  // reads. We allow the buffer to grow until the specified value and then
  // for bigger requests allocate one shot buffers. In unbuffered mode we
  // always bypass read-ahead buffer at ReadaheadRandomAccessFile
  // When read-ahead is required we then make use of compaction_readahead_size
  // value and always try to read ahead. With read-ahead we always
  // pre-allocate buffer to the size instead of growing it up to a limit.
  //
  // This option is currently honored only on Windows
  //
  // Default: 1 Mb
  //
  // Special value: 0 - means do not maintain per instance buffer. Allocate
  //                per request buffer and avoid locking.
  size_t random_access_max_buffer_size;

  // This is the maximum buffer size that is used by WritableFileWriter.
  // On Windows, we need to maintain an aligned buffer for writes.
  // We allow the buffer to grow until it's size hits the limit.
  //
  // Default: 1024 * 1024 (1 MB)
  size_t writable_file_max_buffer_size;

  // Use adaptive mutex, which spins in the user space before resorting
  // to kernel. This could reduce context switch when the mutex is not
  // heavily contended. However, if the mutex is hot, we could end up
  // wasting spin time.
  // Default: false
  bool use_adaptive_mutex;

  // Create DBOptions with default values for all fields
  DBOptions();
  // Create DBOptions from Options
  explicit DBOptions(const Options& options);

  void Dump(Logger* log) const;

  // Allows OS to incrementally sync files to disk while they are being
  // written, asynchronously, in the background. This operation can be used
  // to smooth out write I/Os over time. Users shouldn't reply on it for
  // persistency guarantee.
  // Issue one request for every bytes_per_sync written. 0 turns it off.
  // Default: 0
  //
  // You may consider using rate_limiter to regulate write rate to device.
  // When rate limiter is enabled, it automatically enables bytes_per_sync
  // to 1MB.
  //
  // This option applies to table files
  uint64_t bytes_per_sync;

  // Same as bytes_per_sync, but applies to WAL files
  // Default: 0, turned off
  uint64_t wal_bytes_per_sync;

  // A vector of EventListeners which call-back functions will be called
  // when specific VidarDB event happens.
  std::vector<std::shared_ptr<EventListener>> listeners;

  // If true, then the status of the threads involved in this DB will
  // be tracked and available via GetThreadList() API.
  //
  // Default: false
  bool enable_thread_tracking;

  // The limited write rate to DB if soft_pending_compaction_bytes_limit or
  // level0_slowdown_writes_trigger is triggered, or we are writing to the
  // last mem table allowed and we allow more than 3 mem tables. It is
  // calculated using size of user write requests before compression.
  // VidarDB may decide to slow down more if the compaction still
  // gets behind further.
  // Unit: byte per second.
  //
  // Default: 2MB/s
  uint64_t delayed_write_rate;

  // If true, allow multi-writers to update mem tables in parallel.
  // Concurrent memtable writes are not compatible with inplace_update_support
  // or filter_deletes.
  // It is strongly recommended to set enable_write_thread_adaptive_yield
  // if you are going to use this feature.
  //
  // THIS FEATURE IS NOT STABLE YET.
  //
  // Default: false
  bool allow_concurrent_memtable_write;

  // The latency in microseconds after which a std::this_thread::yield
  // call (sched_yield on Linux) is considered to be a signal that
  // other processes or threads would like to use the current core.
  // Increasing this makes writer threads more likely to take CPU
  // by spinning, which will show up as an increase in the number of
  // involuntary context switches.
  //
  // Default: 3
  uint64_t write_thread_slow_yield_usec;

  // If true, then DB::Open() will not update the statistics used to optimize
  // compaction decision by loading table properties from many files.
  // Turning off this feature will improve DBOpen time especially in
  // disk environment.
  //
  // Default: false
  bool skip_stats_update_on_db_open;

  // Recovery mode to control the consistency while replaying WAL
  // Default: kPointInTimeRecovery
  WALRecoveryMode wal_recovery_mode;

  // if set to false then recovery will fail when a prepared
  // transaction is encountered in the WAL
  bool allow_2pc = false;

  // A global cache for table-level rows.
  // Default: nullptr (disabled)
  // Not supported in VIDARDB_LITE mode!
  std::shared_ptr<Cache> row_cache;

#ifndef VIDARDB_LITE
  // A filter object supplied to be invoked while processing write-ahead-logs
  // (WALs) during recovery. The filter provides a way to inspect log
  // records, ignoring a particular record or skipping replay.
  // The filter is invoked at startup and is invoked from a single-thread
  // currently.
  WalFilter* wal_filter;
#endif  // VIDARDB_LITE

  // If true, then DB::Open / CreateColumnFamily / DropColumnFamily
  // / SetOptions will fail if options file is not detected or properly
  // persisted.
  //
  // DEFAULT: false
  bool fail_if_options_file_error;

  // If true, then print malloc stats together with vidardb.stats
  // when printing to LOG.
  // DEFAULT: false
  bool dump_malloc_stats;

  /*************************** Shichao ***********************/
  using showKeyFun = void(*)(const std::string&, std::string&);
  showKeyFun show_key_fun;

  std::string export_path;
  /*************************** Shichao ***********************/
};

// Options to control the behavior of a database (passed to DB::Open)
struct Options : public DBOptions, public ColumnFamilyOptions {
  // Create an Options object with default values for all fields.
  Options() : DBOptions(), ColumnFamilyOptions() {}

  Options(const DBOptions& db_options,
          const ColumnFamilyOptions& column_family_options)
      : DBOptions(db_options), ColumnFamilyOptions(column_family_options) {}

  void Dump(Logger* log) const;

  void DumpCFOptions(Logger* log) const;

  // Some functions that make it easier to optimize VidarDB

  // Set appropriate parameters for bulk loading.
  // The reason that this is a function that returns "this" instead of a
  // constructor is to enable chaining of multiple similar calls in the future.
  //

  // All data will be in level 0 without any automatic compaction.
  // It's recommended to manually call CompactRange(NULL, NULL) before reading
  // from the database, because otherwise the read can be very slow.
  Options* PrepareForBulkLoad();

  // Use this if your DB is very small (like under 1GB) and you don't want to
  // spend lots of memory for memtables.
  Options* OptimizeForSmallDb();
};

//
// An application can issue a read request (via Get/Iterators) and specify
// if that read should process data that ALREADY resides on a specified cache
// level. For example, if an application specifies kBlockCacheTier then the
// Get call will process data that is already processed in the memtable or
// the block cache. It will not page in data from the OS cache or data that
// resides in storage.
enum ReadTier {
  kReadAllTier = 0x0,     // data in memtable, block cache, OS cache or storage
  kBlockCacheTier = 0x1,  // data in memtable or block cache
  kPersistedTier = 0x2    // persisted data.  When WAL is disabled, this option
                          // will skip data in memtable.
                          // Note that this ReadTier currently only supports
                          // Get and MultiGet and does not support iterators.
};

// Options that control read operations
struct ReadOptions {
  // If true, all data read from underlying storage will be
  // verified against corresponding checksums.
  // Default: true
  bool verify_checksums;

  // Should the "data block"/"index block"/"filter block" read for this
  // iteration be cached in memory?
  // Callers may wish to set this field to false for bulk scans.
  // Default: true
  bool fill_cache;

  // If this option is set and memtable implementation allows, Seek
  // might only return keys with the same prefix as the seek-key
  //

  // If "snapshot" is non-nullptr, read as of the supplied snapshot
  // (which must belong to the DB that is being read and which must
  // not have been released).  If "snapshot" is nullptr, use an impliicit
  // snapshot of the state at the beginning of this read operation.
  // Default: nullptr
  const Snapshot* snapshot;

  // "iterate_upper_bound" defines the extent upto which the forward iterator
  // can returns entries. Once the bound is reached, Valid() will be false.
  // "iterate_upper_bound" is exclusive ie the bound value is
  // not a valid entry.  If iterator_extractor is not null, the Seek target
  // and iterator_upper_bound need to have the same prefix.
  // This is because ordering is not guaranteed outside of prefix domain.
  // There is no lower bound on the iterator. If needed, that can be easily
  // implemented
  //
  // Default: nullptr
  const Slice* iterate_upper_bound;

  // Specify if this read request should process data that ALREADY
  // resides on a particular cache. If the required data is not
  // found at the specified cache, then Status::Incomplete is returned.
  // Default: kReadAllTier
  ReadTier read_tier;

  // Specify to create a tailing iterator -- a special iterator that has a
  // view of the complete database (i.e. it can also be used to read newly
  // added data) and is optimized for sequential reads. It will return records
  // that were inserted into the database after the creation of the iterator.
  // Default: false
  // Not supported in VIDARDB_LITE mode!
  bool tailing;

  // Enable a total order seek regardless of index format (e.g. hash index)
  // used in the table. Some table format (e.g. plain table) may not support
  // this option.
  // If true when calling Get(), we also skip prefix bloom when reading from
  // block based table. It provides a way to read existing data after
  // changing implementation of prefix extractor.
  bool total_order_seek;

  // Keep the blocks loaded by the iterator pinned in memory as long as the
  // iterator is not deleted, If used when reading from tables created with
  // BlockBasedTableOptions::use_delta_encoding = false,
  // Iterator's property "vidardb.iterator.is-key-pinned" is guaranteed to
  // return 1.
  // Default: false
  bool pin_data;

  // If non-zero, NewIterator will create a new table reader which
  // performs reads of the given size. Using a large size (> 2MB) can
  // improve the performance of forward iteration on spinning disks.
  // Default: 0
  size_t readahead_size;

  /***************************** Quanzhao *********************************/
  // If empty, RangeQuery will return all columns, else return the specified
  // index column.
  // Note: Column index must be from 0 to MAX_COLUMN_INDEX.
  //       Index 0 means only querying the user keys, and
  //       the value column index is from 1 to MAX_COLUMN_INDEX.
  std::vector<uint32_t> columns;

  // If non-zero, RangeQuery will return the expected result pairs of the given
  // maximum size in every batch. Otherwise, it will return the all result pairs
  // in one batch. Note: the size unit is Byte.
  // Default: 0
  size_t batch_capacity;

  // Store the temporary states for RangeQuery.
  // Note: Caller should not set the value.
  void* range_query_meta;

  // Store the result key and value size in one batch for RangeQuery.
  // Note: Caller should not set the value and the size unit is Byte.
  size_t result_key_size;
  size_t result_val_size;
  /***************************** Quanzhao *********************************/

  ReadOptions();
  ReadOptions(bool cksum, bool cache);
};

// Options that control write operations
struct WriteOptions {
  // If true, the write will be flushed from the operating system
  // buffer cache (by calling WritableFile::Sync()) before the write
  // is considered complete.  If this flag is true, writes will be
  // slower.
  //
  // If this flag is false, and the machine crashes, some recent
  // writes may be lost.  Note that if it is just the process that
  // crashes (i.e., the machine does not reboot), no writes will be
  // lost even if sync==false.
  //
  // In other words, a DB write with sync==false has similar
  // crash semantics as the "write()" system call.  A DB write
  // with sync==true has similar crash semantics to a "write()"
  // system call followed by "fdatasync()".
  //
  // Default: false
  bool sync;

  // If true, writes will not first go to the write ahead log,
  // and the write may got lost after a crash.
  bool disableWAL;

  // If true and if user is trying to write to column families that don't exist
  // (they were dropped), ignore the write (don't return an error). If there
  // are multiple writes in a WriteBatch, other writes will succeed.
  // Default: false
  bool ignore_missing_column_families;

  WriteOptions()
      : sync(false),
        disableWAL(false),
        ignore_missing_column_families(false) {}
};

// Options that control flush operations
struct FlushOptions {
  // If true, the flush will wait until the flush is done.
  // Default: true
  bool wait;

  FlushOptions() : wait(true) {}
};

// Create a Logger from provided DBOptions
extern Status CreateLoggerFromOptions(const std::string& dbname,
                                      const DBOptions& options,
                                      std::shared_ptr<Logger>* logger);

// CompactionOptions are used in CompactFiles() call.
struct CompactionOptions {
  // Compaction output compression type
  // Default: snappy
  CompressionType compression;
  // Compaction will create files of size `output_file_size_limit`.
  // Default: MAX, which means that compaction will create a single file
  uint64_t output_file_size_limit;

  CompactionOptions()
      : compression(kSnappyCompression),
        output_file_size_limit(std::numeric_limits<uint64_t>::max()) {}
};

// For level based compaction, we can configure if we want to skip/force
// bottommost level compaction.
enum class BottommostLevelCompaction {
  // Skip bottommost level compaction
  kSkip,
  // Only compact bottommost level if there is a compaction filter
  // This is the default option
  kIfHaveCompactionFilter,
  // Always compact bottommost level
  kForce,
};

// CompactRangeOptions is used by CompactRange() call.
struct CompactRangeOptions {
  // If true, no other compaction will run at the same time as this
  // manual compaction
  bool exclusive_manual_compaction = true;
  // If true, compacted files will be moved to the minimum level capable
  // of holding the data or given level (specified non-negative target_level).
  bool change_level = false;
  // If change_level is true and target_level have non-negative value, compacted
  // files will be moved to target_level.
  int target_level = -1;
  // Compaction outputs will be placed in options.db_paths[target_path_id].
  // Behavior is undefined if target_path_id is out of range.
  uint32_t target_path_id = 0;
  // By default level based compaction will only compact the bottommost level
  // if there is a compaction filter
  BottommostLevelCompaction bottommost_level_compaction =
      BottommostLevelCompaction::kIfHaveCompactionFilter;
};

}  // namespace vidardb

#endif  // STORAGE_VIDARDB_INCLUDE_OPTIONS_H_
