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

#include "db/table_cache.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/version_edit.h"

#include "vidardb/statistics.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "table/get_context.h"
#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/perf_context_imp.h"
#include "util/stop_watch.h"
#include "util/sync_point.h"

namespace vidardb {

namespace {

template <class T>
static void DeleteEntry(const Slice& key, void* value) {
  T* typed_value = reinterpret_cast<T*>(value);
  delete typed_value;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

static void DeleteTableReader(void* arg1, void* arg2) {
  TableReader* table_reader = reinterpret_cast<TableReader*>(arg1);
  delete table_reader;
}

static Slice GetSliceForFileNumber(const uint64_t* file_number) {
  return Slice(reinterpret_cast<const char*>(file_number),
               sizeof(*file_number));
}

#ifndef VIDARDB_LITE

void AppendVarint64(IterKey* key, uint64_t v) {
  char buf[10];
  auto ptr = EncodeVarint64(buf, v);
  key->TrimAppend(key->Size(), buf, ptr - buf);
}

#endif  // VIDARDB_LITE

}  // namespace

TableCache::TableCache(const ImmutableCFOptions& ioptions,
                       const EnvOptions& env_options, Cache* const cache)
    : ioptions_(ioptions), env_options_(env_options), cache_(cache) {
  if (ioptions_.row_cache) {
    // If the same cache is shared by multiple instances, we need to
    // disambiguate its entries.
    PutVarint64(&row_cache_id_, ioptions_.row_cache->NewId());
  }
}

TableCache::~TableCache() {
}

TableReader* TableCache::GetTableReaderFromHandle(Cache::Handle* handle) {
  return reinterpret_cast<TableReader*>(cache_->Value(handle));
}

void TableCache::ReleaseHandle(Cache::Handle* handle) {
  cache_->Release(handle);
}

Status TableCache::GetTableReader(
    const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
    bool sequential_mode, size_t readahead, bool record_read_stats,
    HistogramImpl* file_read_hist, unique_ptr<TableReader>* table_reader,
    int level, const std::vector<uint32_t>& cols) {  // Shichao
  std::string fname =
      TableFileName(ioptions_.db_paths, fd.GetNumber(), fd.GetPathId());
  unique_ptr<RandomAccessFile> file;
  Status s = ioptions_.env->NewRandomAccessFile(fname, &file, env_options);

  if (readahead > 0) {
    file = NewReadaheadRandomAccessFile(std::move(file), readahead);
  }

  RecordTick(ioptions_.statistics, NO_FILE_OPENS);
  if (s.ok()) {
    if (!sequential_mode && ioptions_.advise_random_on_open) {
      file->Hint(RandomAccessFile::RANDOM);
    }
    StopWatch sw(ioptions_.env, ioptions_.statistics, TABLE_OPEN_IO_MICROS);
    std::unique_ptr<RandomAccessFileReader> file_reader(
        new RandomAccessFileReader(std::move(file), ioptions_.env,
                                   ioptions_.statistics, record_read_stats,
                                   file_read_hist));

    s = ioptions_.table_factory->NewTableReader(
        TableReaderOptions(ioptions_, env_options, internal_comparator, level,
                           cols),  // Shichao
        std::move(file_reader), fd.GetFileSize(), table_reader);
    TEST_SYNC_POINT("TableCache::GetTableReader:0");
  }
  return s;
}

void TableCache::EraseHandle(const FileDescriptor& fd, Cache::Handle* handle) {
  ReleaseHandle(handle);
  uint64_t number = fd.GetNumber();
  Slice key = GetSliceForFileNumber(&number);
  cache_->Erase(key);
}

Status TableCache::FindTable(const EnvOptions& env_options,
                             const InternalKeyComparator& internal_comparator,
                             const FileDescriptor& fd, Cache::Handle** handle,
                             const bool no_io, bool record_read_stats,
                             HistogramImpl* file_read_hist, int level) {
  PERF_TIMER_GUARD(find_table_nanos);
  Status s;
  uint64_t number = fd.GetNumber();
  Slice key = GetSliceForFileNumber(&number);
  *handle = cache_->Lookup(key);
  TEST_SYNC_POINT_CALLBACK("TableCache::FindTable:0",
                           const_cast<bool*>(&no_io));

  if (*handle == nullptr) {
    if (no_io) {  // Don't do IO and return a not-found status
      return Status::Incomplete("Table not found in table_cache, no_io is set");
    }
    unique_ptr<TableReader> table_reader;
    s = GetTableReader(env_options, internal_comparator, fd,
                       false /* sequential mode */, 0 /* readahead */,
                       record_read_stats, file_read_hist, &table_reader, level);
    if (!s.ok()) {
      assert(table_reader == nullptr);
      RecordTick(ioptions_.statistics, NO_FILE_ERRORS);
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      s = cache_->Insert(key, table_reader.get(), 1, &DeleteEntry<TableReader>,
                         handle);
      if (s.ok()) {
        // Release ownership of table reader.
        table_reader.release();
      }
    }
  }

  return s;
}

/**************************** Shichao *****************************/
Status TableCache::FindTableForRangeQuery(
    const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
    Cache::Handle** handle, const bool no_io, bool record_read_stats,
    HistogramImpl* file_read_hist, int level, size_t readahead) {
  PERF_TIMER_GUARD(find_table_nanos);
  Status s;
  uint64_t number = fd.GetNumber();
  // For range query, we set another half of number as the cache key to avoid
  // the conflict of the original cache key, since their open file options are
  // different, direct v.s. buffered read.
  assert(number < (kFileNumberMask >> 1));
  number = kFileNumberMask - number;
  Slice key = GetSliceForFileNumber(&number);
  *handle = cache_->Lookup(key);
  TEST_SYNC_POINT_CALLBACK("TableCache::FindTable:0",
                           const_cast<bool*>(&no_io));

  if (*handle == nullptr) {
    if (no_io) {  // Don't do IO and return a not-found status
      return Status::Incomplete("Table not found in table_cache, no_io is set");
    }
    unique_ptr<TableReader> table_reader;
    s = GetTableReader(env_options, internal_comparator, fd,
                       true /* sequential mode */, readahead, record_read_stats,
                       file_read_hist, &table_reader, level);
    if (!s.ok()) {
      assert(table_reader == nullptr);
      RecordTick(ioptions_.statistics, NO_FILE_ERRORS);
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      s = cache_->Insert(key, table_reader.get(), 1, &DeleteEntry<TableReader>,
                         handle);
      if (s.ok()) {
        // Release ownership of table reader.
        table_reader.release();
      }
    }
  }

  return s;
}
/**************************** Shichao *****************************/

InternalIterator* TableCache::NewIterator(
    const ReadOptions& options, const EnvOptions& env_options,
    const InternalKeyComparator& icomparator, const FileDescriptor& fd,
    TableReader** table_reader_ptr, HistogramImpl* file_read_hist,
    bool for_compaction, Arena* arena, int level, bool for_range_query,
    const Slice& smallest_user_key) {  // Shichao
  PERF_TIMER_GUARD(new_table_iterator_nanos);

  if (table_reader_ptr != nullptr) {
    *table_reader_ptr = nullptr;
  }

  TableReader* table_reader = nullptr;
  Cache::Handle* handle = nullptr;

  size_t readahead = 0;
  bool create_new_table_reader = false;
  if (for_compaction) {
    if (ioptions_.new_table_reader_for_compaction_inputs) {
      readahead = ioptions_.compaction_readahead_size;
      create_new_table_reader = true;
    }
  } else {
    readahead = options.readahead_size;
    create_new_table_reader = readahead > 0;
  }

  if (create_new_table_reader) {
    unique_ptr<TableReader> table_reader_unique_ptr;
    Status s = GetTableReader(
        env_options, icomparator, fd, true /* sequential_mode */, readahead,
        !for_compaction /* record stats */, nullptr, &table_reader_unique_ptr,
        level, options.columns /*Shichao*/);
    if (!s.ok()) {
      return NewErrorInternalIterator(s, arena);
    }
    table_reader = table_reader_unique_ptr.release();
  } else {
    if (for_range_query) {
      // For range query, we create another reader first time and cache it to
      // make tail index in memory.
      Status s = FindTableForRangeQuery(
          env_options, icomparator, fd, &handle,
          options.read_tier == kBlockCacheTier /* no_io */,
          !for_compaction /* record read_stats */, file_read_hist, level,
          options.readahead_size);
      if (!s.ok()) {
        return NewErrorInternalIterator(s, arena);
      }
      table_reader = GetTableReaderFromHandle(handle);
    } else {
      table_reader = fd.table_reader;
      if (table_reader == nullptr) {
        Status s = FindTable(env_options, icomparator, fd, &handle,
                             options.read_tier == kBlockCacheTier /* no_io */,
                             !for_compaction /* record read_stats */,
                             file_read_hist, level);
        if (!s.ok()) {
          return NewErrorInternalIterator(s, arena);
        }
        table_reader = GetTableReaderFromHandle(handle);
      }
    }
  }

  InternalIterator* result = table_reader->NewIterator(
      options, arena, for_range_query, smallest_user_key);

  if (create_new_table_reader) {
    assert(handle == nullptr);
    result->RegisterCleanup(&DeleteTableReader, table_reader, nullptr);
  } else if (handle != nullptr) {
    result->RegisterCleanup(&UnrefEntry, cache_, handle);
  }

  if (for_compaction || for_range_query) {
    table_reader->SetupForCompaction();
  }
  if (table_reader_ptr != nullptr) {
    *table_reader_ptr = table_reader;
  }

  return result;
}

Status TableCache::Get(const ReadOptions& options,
                       const InternalKeyComparator& internal_comparator,
                       const FileDescriptor& fd, const Slice& k,
                       GetContext* get_context, HistogramImpl* file_read_hist,
                       int level) {
  TableReader* t = fd.table_reader;
  Status s;
  Cache::Handle* handle = nullptr;
  std::string* row_cache_entry = nullptr;

#ifndef VIDARDB_LITE
  IterKey row_cache_key;
  std::string row_cache_entry_buffer;

  // Check row cache if enabled. Since row cache does not currently store
  // sequence numbers, we cannot use it if we need to fetch the sequence.
  if (ioptions_.row_cache && !get_context->NeedToReadSequence()) {
    uint64_t fd_number = fd.GetNumber();
    auto user_key = ExtractUserKey(k);
    // We use the user key as cache key instead of the internal key,
    // otherwise the whole cache would be invalidated every time the
    // sequence key increases. However, to support caching snapshot
    // reads, we append the sequence number (incremented by 1 to
    // distinguish from 0) only in this case.
    uint64_t seq_no =
        options.snapshot == nullptr ? 0 : 1 + GetInternalKeySeqno(k);

    // Compute row cache key.
    row_cache_key.TrimAppend(row_cache_key.Size(), row_cache_id_.data(),
                             row_cache_id_.size());
    AppendVarint64(&row_cache_key, fd_number);
    AppendVarint64(&row_cache_key, seq_no);
    row_cache_key.TrimAppend(row_cache_key.Size(), user_key.data(),
                             user_key.size());

    if (auto row_handle = ioptions_.row_cache->Lookup(row_cache_key.GetKey())) {
      auto found_row_cache_entry = static_cast<const std::string*>(
          ioptions_.row_cache->Value(row_handle));
      replayGetContextLog(*found_row_cache_entry, user_key, get_context);
      ioptions_.row_cache->Release(row_handle);
      RecordTick(ioptions_.statistics, ROW_CACHE_HIT);
      return Status::OK();
    }

    // Not found, setting up the replay log.
    RecordTick(ioptions_.statistics, ROW_CACHE_MISS);
    row_cache_entry = &row_cache_entry_buffer;
  }
#endif  // VIDARDB_LITE

  if (!t) {
    s = FindTable(env_options_, internal_comparator, fd, &handle,
                  options.read_tier == kBlockCacheTier /* no_io */,
                  true /* record_read_stats */, file_read_hist, level);
    if (s.ok()) {
      t = GetTableReaderFromHandle(handle);
    }
  }
  if (s.ok()) {
    get_context->SetReplayLog(row_cache_entry);  // nullptr if no cache.
    s = t->Get(options, k, get_context);
    get_context->SetReplayLog(nullptr);
    if (handle != nullptr) {
      ReleaseHandle(handle);
    }
  } else if (options.read_tier == kBlockCacheTier && s.IsIncomplete()) {
    // Couldn't find Table in cache but treat as kFound if no_io set
    get_context->MarkKeyMayExist();
    return Status::OK();
  }

#ifndef VIDARDB_LITE
  // Put the replay log in row cache only if something was found.
  if (s.ok() && row_cache_entry && !row_cache_entry->empty()) {
    size_t charge =
        row_cache_key.Size() + row_cache_entry->size() + sizeof(std::string);
    void* row_ptr = new std::string(std::move(*row_cache_entry));
    ioptions_.row_cache->Insert(row_cache_key.GetKey(), row_ptr, charge,
                                &DeleteEntry<std::string>);
  }
#endif  // VIDARDB_LITE

  return s;
}

Status TableCache::GetTableProperties(
    const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
    std::shared_ptr<const TableProperties>* properties, bool no_io) {
  Status s;
  auto table_reader = fd.table_reader;
  // table already been pre-loaded?
  if (table_reader) {
    *properties = table_reader->GetTableProperties();

    return s;
  }

  Cache::Handle* table_handle = nullptr;
  s = FindTable(env_options, internal_comparator, fd, &table_handle, no_io);
  if (!s.ok()) {
    return s;
  }
  assert(table_handle);
  auto table = GetTableReaderFromHandle(table_handle);
  *properties = table->GetTableProperties();
  ReleaseHandle(table_handle);
  return s;
}

size_t TableCache::GetMemoryUsageByTableReader(
    const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator,
    const FileDescriptor& fd) {
  auto table_reader = fd.table_reader;
  // table already been pre-loaded?
  if (table_reader) {
    return table_reader->ApproximateMemoryUsage();
  }

  Cache::Handle* table_handle = nullptr;
  Status s = FindTable(env_options, internal_comparator, fd, &table_handle, true);
  if (!s.ok()) {
    return 0;
  }
  assert(table_handle);
  auto table = GetTableReaderFromHandle(table_handle);
  auto ret = table->ApproximateMemoryUsage();
  ReleaseHandle(table_handle);
  return ret;
}

void TableCache::Evict(Cache* cache, uint64_t file_number) {
  cache->Erase(GetSliceForFileNumber(&file_number));
}

}  // namespace vidardb
