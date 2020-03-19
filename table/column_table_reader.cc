//  Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "table/column_table_reader.h"

#include <string>
#include <utility>

#include "db/dbformat.h"
#include "db/filename.h"

#include "vidardb/cache.h"
#include "vidardb/comparator.h"
#include "vidardb/env.h"
#include "vidardb/iterator.h"
#include "vidardb/options.h"
#include "vidardb/statistics.h"
#include "vidardb/table.h"
#include "vidardb/table_properties.h"
#include "vidardb/splitter.h"

#include "table/block.h"
#include "table/column_table_factory.h"
#include "table/format.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/meta_blocks.h"
#include "table/two_level_iterator.h"

#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/perf_context_imp.h"
#include "util/stop_watch.h"
#include "util/string_util.h"

namespace vidardb {

extern const uint64_t kColumnTableMagicNumber;
using std::unique_ptr;

typedef ColumnTable::IndexReader IndexReader;

namespace {

// Read the block identified by "handle" from "file".
// The only relevant option is options.verify_checksums for now.
// On failure return non-OK.
// On success fill *result and return OK - caller owns *result
// @param compression_dict Data for presetting the compression library's
//    dictionary.
Status ReadBlockFromFile(RandomAccessFileReader* file, const Footer& footer,
                         const ReadOptions& options, const BlockHandle& handle,
                         std::unique_ptr<Block>* result, Env* env,
                         bool do_uncompress, const Slice& compression_dict,
                         Logger* info_log) {
  BlockContents contents;
  Status s = ReadBlockContents(file, footer, options, handle, &contents, env,
                               do_uncompress, compression_dict, info_log);
  if (s.ok()) {
    result->reset(new Block(std::move(contents)));
  }

  return s;
}

// Delete the resource that is held by the iterator.
template <class ResourceType>
void DeleteHeldResource(void* arg, void* ignored) {
  delete reinterpret_cast<ResourceType*>(arg);
}

// Delete the entry resided in the cache.
template <class Entry>
void DeleteCachedEntry(const Slice& key, void* value) {
  auto entry = reinterpret_cast<Entry*>(value);
  delete entry;
}

void DeleteCachedIndexEntry(const Slice& key, void* value);

// Release the cached entry and decrement its ref count.
void ReleaseCachedEntry(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

Slice GetCacheKeyFromOffset(const char* cache_key_prefix,
                            size_t cache_key_prefix_size, uint64_t offset,
                            char* cache_key) {
  assert(cache_key != nullptr);
  assert(cache_key_prefix_size != 0);
  assert(cache_key_prefix_size <= ColumnTable::kMaxCacheKeyPrefixSize);
  memcpy(cache_key, cache_key_prefix, cache_key_prefix_size);
  char* end = EncodeVarint64(cache_key + cache_key_prefix_size, offset);
  return Slice(cache_key, static_cast<size_t>(end - cache_key));
}

Cache::Handle* GetEntryFromCache(Cache* block_cache, const Slice& key,
                                 Tickers block_cache_miss_ticker,
                                 Tickers block_cache_hit_ticker,
                                 Statistics* statistics) {
  auto cache_handle = block_cache->Lookup(key);
  if (cache_handle != nullptr) {
    PERF_COUNTER_ADD(block_cache_hit_count, 1);
    // overall cache hit
    RecordTick(statistics, BLOCK_CACHE_HIT);
    // total bytes read from cache
    RecordTick(statistics, BLOCK_CACHE_BYTES_READ,
               block_cache->GetUsage(cache_handle));
    // block-type specific cache hit
    RecordTick(statistics, block_cache_hit_ticker);
  } else {
    // overall cache miss
    RecordTick(statistics, BLOCK_CACHE_MISS);
    // block-type specific cache miss
    RecordTick(statistics, block_cache_miss_ticker);
  }

  return cache_handle;
}

}  // namespace

// -- IndexReader and its subclasses
// IndexReader is the interface that provide the functionality for index access.
class ColumnTable::IndexReader {
 public:
  explicit IndexReader(const Comparator* comparator, Statistics* stats)
      : comparator_(comparator), statistics_(stats) {}

  virtual ~IndexReader() {}

  // Create an iterator for index access.
  // An iter is passed in, if it is not null, update this one and return it
  // If it is null, create a new Iterator
  virtual InternalIterator* NewIterator(BlockIter* iter = nullptr) = 0;

  // The size of the index.
  virtual size_t size() const = 0;
  // Memory usage of the index block
  virtual size_t usable_size() const = 0;
  // return the statistics pointer
  virtual Statistics* statistics() const { return statistics_; }
  // Report an approximation of how much memory has been used other than memory
  // that was allocated in block cache.
  virtual size_t ApproximateMemoryUsage() const = 0;

 protected:
  const Comparator* comparator_;

 private:
  Statistics* statistics_;
};

// Index that allows binary search lookup for the first key of each block.
// This class can be viewed as a thin wrapper for `Block` class which already
// supports binary search.
class BinarySearchIndexReader : public IndexReader {
 public:
  // Read index from the file and create an instance for
  // `BinarySearchIndexReader`.
  // On success, index_reader will be populated; otherwise it will remain
  // unmodified.
  static Status Create(RandomAccessFileReader* file, const Footer& footer,
                       const BlockHandle& index_handle, Env* env,
                       const Comparator* comparator, IndexReader** index_reader,
                       Statistics* statistics) {
    std::unique_ptr<Block> index_block;
    auto s = ReadBlockFromFile(file, footer, ReadOptions(), index_handle,
                               &index_block, env, true /* decompress */,
                               Slice() /*compression dict*/,
                               /*info_log*/ nullptr);

    if (s.ok()) {
      *index_reader = new BinarySearchIndexReader(
          comparator, std::move(index_block), statistics);
    }

    return s;
  }

  virtual InternalIterator* NewIterator(BlockIter* iter = nullptr) override {
    return index_block_->NewIterator(comparator_, iter);
  }

  virtual size_t size() const override { return index_block_->size(); }
  virtual size_t usable_size() const override {
    return index_block_->usable_size();
  }

  virtual size_t ApproximateMemoryUsage() const override {
    assert(index_block_);
    return index_block_->ApproximateMemoryUsage();
  }

 private:
  BinarySearchIndexReader(const Comparator* comparator,
                          std::unique_ptr<Block>&& index_block,
                          Statistics* stats)
      : IndexReader(comparator, stats), index_block_(std::move(index_block)) {
    assert(index_block_ != nullptr);
  }
  std::unique_ptr<Block> index_block_;
};

namespace {

void DeleteCachedIndexEntry(const Slice& key, void* value) {
  IndexReader* index_reader = reinterpret_cast<IndexReader*>(value);
  if (index_reader->statistics() != nullptr) {
    RecordTick(index_reader->statistics(), BLOCK_CACHE_INDEX_BYTES_EVICT,
               index_reader->usable_size());
  }
  delete index_reader;
}

}  // anonymous namespace

// CachableEntry represents the entries that *may* be fetched from block cache.
//  field `value` is the item we want to get.
//  field `cache_handle` is the cache handle to the block cache. If the value
//    was not read from cache, `cache_handle` will be nullptr.
template <class TValue>
struct ColumnTable::CachableEntry {
  CachableEntry(TValue* _value, Cache::Handle* _cache_handle)
      : value(_value), cache_handle(_cache_handle) {}
  CachableEntry() : CachableEntry(nullptr, nullptr) {}
  void Release(Cache* cache) {
    if (cache_handle) {
      cache->Release(cache_handle);
      value = nullptr;
      cache_handle = nullptr;
    }
  }
  bool IsSet() const { return cache_handle != nullptr; }

  TValue* value = nullptr;
  // if the entry is from the cache, cache_handle will be populated.
  Cache::Handle* cache_handle = nullptr;
};

struct ColumnTable::Rep {
  Rep(const ImmutableCFOptions& _ioptions, const EnvOptions& _env_options,
      const ColumnTableOptions& _table_opt,
      const InternalKeyComparator& _internal_comparator)
      : ioptions(_ioptions),
        env_options(_env_options),
        table_options(_table_opt),
        internal_comparator(_internal_comparator) {}

  const ImmutableCFOptions& ioptions;
  const EnvOptions& env_options;
  const ColumnTableOptions& table_options;
  const InternalKeyComparator& internal_comparator;
  std::unique_ptr<ColumnKeyComparator> column_comparator;
  Status status;
  unique_ptr<RandomAccessFileReader> file;
  char cache_key_prefix[kMaxCacheKeyPrefixSize];
  size_t cache_key_prefix_size = 0;
  uint64_t dummy_index_reader_offset = 0;  // ID unique for the block cache.

  // Footer contains the fixed table information
  Footer footer;
  // index_reader will be populated and used only when options.block_cache is
  // nullptr; otherwise we will get the index block via the block cache.
  unique_ptr<IndexReader> index_reader;

  std::shared_ptr<const TableProperties> table_properties;
  // Block containing the data for the compression dictionary. We take ownership
  // for the entire block struct, even though we only use its Slice member. This
  // is easier because the Slice member depends on the continued existence of
  // another member ("allocation").
  std::unique_ptr<const BlockContents> compression_dict_block;

  bool main_column;
  std::vector<unique_ptr<ColumnTable>> tables;  // sub colum tables
};

// Load the meta-block from the file. On success, return the loaded meta block
// and its iterator.
Status ColumnTable::ReadMetaBlock(Rep* rep,
                                  std::unique_ptr<Block>* meta_block,
                                  std::unique_ptr<InternalIterator>* iter) {
  std::unique_ptr<Block> meta;
  Status s = ReadBlockFromFile(
      rep->file.get(), rep->footer, ReadOptions(),
      rep->footer.metaindex_handle(), &meta, rep->ioptions.env,
      true /* decompress */, Slice() /*compression dict*/,
      rep->ioptions.info_log);

  if (!s.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, rep->ioptions.info_log,
        "Encountered error while reading data from properties"
        " block %s", s.ToString().c_str());
    return s;
  }

  *meta_block = std::move(meta);
  // meta block uses bytewise comparator.
  iter->reset(meta_block->get()->NewIterator(BytewiseComparator()));
  return Status::OK();
}

void ColumnTable::GenerateCachePrefix(Cache* cc, RandomAccessFile* file,
                                      char* buffer, size_t* size) {
  // generate an id from the file
  *size = file->GetUniqueId(buffer, kMaxCacheKeyPrefixSize);

  // If the prefix wasn't generated or was too long, create one from the cache.
  if (cc && *size == 0) {
    char* end = EncodeVarint64(buffer, cc->NewId());
    *size = static_cast<size_t>(end - buffer);
  }
}

// Helper function to setup the cache key's prefix for the Table.
void ColumnTable::SetupCacheKeyPrefix(Rep* rep, uint64_t file_size) {
  assert(kMaxCacheKeyPrefixSize >= 10);
  rep->cache_key_prefix_size = 0;
  if (rep->table_options.block_cache != nullptr) {
    GenerateCachePrefix(rep->table_options.block_cache.get(), rep->file->file(),
                        &rep->cache_key_prefix[0], &rep->cache_key_prefix_size);
    // Create dummy offset of index reader which is beyond the file size.
    rep->dummy_index_reader_offset =
        file_size + rep->table_options.block_cache->NewId();
  }
}

Slice ColumnTable::GetCacheKey(const char* cache_key_prefix,
                               size_t cache_key_prefix_size,
                               const BlockHandle& handle, char* cache_key) {
  assert(cache_key != nullptr);
  assert(cache_key_prefix_size != 0);
  assert(cache_key_prefix_size <= kMaxCacheKeyPrefixSize);
  memcpy(cache_key, cache_key_prefix, cache_key_prefix_size);
  char* end =
      EncodeVarint64(cache_key + cache_key_prefix_size, handle.offset());
  return Slice(cache_key, static_cast<size_t>(end - cache_key));
}

Status ColumnTable::PutDataBlockToCache(
    const Slice& block_cache_key, Cache* block_cache, Statistics* statistics,
    CachableEntry<Block>* block, Block* raw_block) {
  assert(raw_block->compression_type() == kNoCompression);
  Status s;
  block->value = raw_block;
  raw_block = nullptr;

  // insert into uncompressed block cache
  assert((block->value->compression_type() == kNoCompression));
  if (block_cache != nullptr && block->value->cachable()) {
    s = block_cache->Insert(block_cache_key, block->value,
                            block->value->usable_size(),
                            &DeleteCachedEntry<Block>, &(block->cache_handle));
    if (s.ok()) {
      assert(block->cache_handle != nullptr);
      RecordTick(statistics, BLOCK_CACHE_ADD);
      RecordTick(statistics, BLOCK_CACHE_BYTES_WRITE,
                 block->value->usable_size());
      assert(reinterpret_cast<Block*>(
                 block_cache->Value(block->cache_handle)) == block->value);
    } else {
      RecordTick(statistics, BLOCK_CACHE_ADD_FAILURES);
      delete block->value;
      block->value = nullptr;
    }
  }

  return s;
}

Status ColumnTable::GetDataBlockFromCache(
    const Slice& block_cache_key, Cache* block_cache, Statistics* statistics,
    ColumnTable::CachableEntry<Block>* block) {
  Status s;

  // Lookup uncompressed cache first
  if (block_cache != nullptr) {
    block->cache_handle =
        GetEntryFromCache(block_cache, block_cache_key, BLOCK_CACHE_DATA_MISS,
                          BLOCK_CACHE_DATA_HIT, statistics);
    if (block->cache_handle != nullptr) {
      block->value =
          reinterpret_cast<Block*>(block_cache->Value(block->cache_handle));
      return s;
    }
  }

  assert(block->cache_handle == nullptr && block->value == nullptr);

  return s;
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
// If input_iter is null, new a iterator
// If input_iter is not null, update this iter and return it
InternalIterator* ColumnTable::NewDataBlockIterator(
    Rep* rep, const ReadOptions& read_options, const Slice& index_value,
    BlockIter* input_iter) {
  PERF_TIMER_GUARD(new_table_block_iter_nanos);

  BlockHandle handle;
  Slice input = index_value;
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.
  Status s = handle.DecodeFrom(&input);

  if (!s.ok()) {
    if (input_iter != nullptr) {
      input_iter->SetStatus(s);
      return input_iter;
    } else {
      return NewErrorInternalIterator(s);
    }
  }

  Slice compression_dict;
  if (rep->compression_dict_block) {
    compression_dict = rep->compression_dict_block->data;
  }

  const bool no_io = (read_options.read_tier == kBlockCacheTier);
  Cache* block_cache = rep->table_options.block_cache.get();
  CachableEntry<Block> block;
  // If block cache is enabled, we'll try to read from it.
  if (block_cache != nullptr) {
    Statistics* statistics = rep->ioptions.statistics;
    char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
    // create key for block cache
    Slice key = GetCacheKey(rep->cache_key_prefix, rep->cache_key_prefix_size,
                            handle, cache_key);

    s = GetDataBlockFromCache(key, block_cache, statistics, &block);

    if (block.value == nullptr && !no_io && read_options.fill_cache) {
      std::unique_ptr<Block> raw_block;
      {
        StopWatch sw(rep->ioptions.env, statistics, READ_BLOCK_GET_MICROS);
        s = ReadBlockFromFile(rep->file.get(), rep->footer, read_options,
                              handle, &raw_block, rep->ioptions.env, true,
                              compression_dict, rep->ioptions.info_log);
      }

      if (s.ok()) {
        s = PutDataBlockToCache(key, block_cache, statistics, &block,
                                raw_block.release());
      }
    }
  }

  // Didn't get any data from block caches.
  if (s.ok() && block.value == nullptr) {
    if (no_io) {
      // Could not read from block_cache and can't do IO
      if (input_iter != nullptr) {
        input_iter->SetStatus(Status::Incomplete("no blocking io"));
        return input_iter;
      } else {
        return NewErrorInternalIterator(Status::Incomplete("no blocking io"));
      }
    }
    std::unique_ptr<Block> block_value;
    s = ReadBlockFromFile(rep->file.get(), rep->footer, read_options, handle,
                          &block_value, rep->ioptions.env, true,
                          compression_dict, rep->ioptions.info_log);
    if (s.ok()) {
      block.value = block_value.release();
    }
  }

  InternalIterator* iter;
  if (s.ok() && block.value != nullptr) {
    iter = block.value->NewIterator(&rep->internal_comparator, input_iter,
                                    !rep->main_column);
    if (block.cache_handle != nullptr) {
      iter->RegisterCleanup(&ReleaseCachedEntry, block_cache,
                            block.cache_handle);
    } else {
      iter->RegisterCleanup(&DeleteHeldResource<Block>, block.value, nullptr);
    }
  } else {
    if (input_iter != nullptr) {
      input_iter->SetStatus(s);
      iter = input_iter;
    } else {
      iter = NewErrorInternalIterator(s);
    }
  }
  return iter;
}

Status ColumnTable::CreateIndexReader(IndexReader** index_reader) {
  auto file = rep_->file.get();
  auto env = rep_->ioptions.env;
  auto comparator = &rep_->internal_comparator;
  const Footer& footer = rep_->footer;
  Statistics* stats = rep_->ioptions.statistics;

  return BinarySearchIndexReader::Create(file, footer, footer.index_handle(),
                                         env, comparator, index_reader, stats);
}

InternalIterator* ColumnTable::NewIndexIterator(
    const ReadOptions& read_options, BlockIter* input_iter,
    CachableEntry<IndexReader>* index_entry) {
  // index reader has already been pre-populated.
  if (rep_->index_reader) {
    return rep_->index_reader->NewIterator(input_iter);
  }

  PERF_TIMER_GUARD(read_index_block_nanos);

  bool no_io = read_options.read_tier == kBlockCacheTier;
  Cache* block_cache = rep_->table_options.block_cache.get();
  char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  auto key = GetCacheKeyFromOffset(rep_->cache_key_prefix,
                                   rep_->cache_key_prefix_size,
                                   rep_->dummy_index_reader_offset, cache_key);
  Statistics* statistics = rep_->ioptions.statistics;
  auto cache_handle = GetEntryFromCache(block_cache, key,
                                        BLOCK_CACHE_INDEX_MISS,
                                        BLOCK_CACHE_INDEX_HIT, statistics);

  if (cache_handle == nullptr && no_io) {
    if (input_iter != nullptr) {
      input_iter->SetStatus(Status::Incomplete("no blocking io"));
      return input_iter;
    } else {
      return NewErrorInternalIterator(Status::Incomplete("no blocking io"));
    }
  }

  IndexReader* index_reader = nullptr;
  if (cache_handle != nullptr) {
    index_reader =
        reinterpret_cast<IndexReader*>(block_cache->Value(cache_handle));
  } else {
    // Create index reader and put it in the cache.
    Status s = CreateIndexReader(&index_reader);
    if (s.ok()) {
      s = block_cache->Insert(key, index_reader, index_reader->usable_size(),
                              &DeleteCachedIndexEntry, &cache_handle);
    }

    if (s.ok()) {
      size_t usable_size = index_reader->usable_size();
      RecordTick(statistics, BLOCK_CACHE_ADD);
      RecordTick(statistics, BLOCK_CACHE_BYTES_WRITE, usable_size);
      RecordTick(statistics, BLOCK_CACHE_INDEX_BYTES_INSERT, usable_size);
    } else {
      RecordTick(statistics, BLOCK_CACHE_ADD_FAILURES);
      // make sure if something goes wrong, index_reader shall remain intact.
      if (input_iter != nullptr) {
        input_iter->SetStatus(s);
        return input_iter;
      } else {
        return NewErrorInternalIterator(s);
      }
    }
  }

  assert(cache_handle);
  auto* iter = index_reader->NewIterator(input_iter);

  // the caller would like to take ownership of the index block
  // don't call RegisterCleanup() in this case, the caller will take care of it
  if (index_entry != nullptr) {
    *index_entry = {index_reader, cache_handle};
  } else {
    iter->RegisterCleanup(&ReleaseCachedEntry, block_cache, cache_handle);
  }

  return iter;
}

Status ColumnTable::DumpIndexBlock(WritableFile* out_file) {
  out_file->Append(
      "Index Details:\n"
      "--------------------------------------\n");

  std::unique_ptr<InternalIterator> blockhandles_iter(
      NewIndexIterator(ReadOptions()));
  Status s = blockhandles_iter->status();
  if (!s.ok()) {
    out_file->Append("Can not read Index Block \n\n");
    return s;
  }

  out_file->Append("  Block key hex dump: Data block handle\n");
  out_file->Append("  Block key ascii\n\n");
  for (blockhandles_iter->SeekToFirst(); blockhandles_iter->Valid();
       blockhandles_iter->Next()) {
    s = blockhandles_iter->status();
    if (!s.ok()) {
      break;
    }
    Slice key = blockhandles_iter->key();
    InternalKey ikey;
    ikey.DecodeFrom(key);

    out_file->Append("  HEX    ");
    out_file->Append(ikey.user_key().ToString(true).c_str());
    out_file->Append(": ");
    out_file->Append(blockhandles_iter->value().ToString(true).c_str());
    out_file->Append("\n");

    std::string str_key = ikey.user_key().ToString();
    std::string res_key("");
    char cspace = ' ';
    for (size_t i = 0; i < str_key.size(); i++) {
      res_key.append(&str_key[i], 1);
      res_key.append(1, cspace);
    }
    out_file->Append("  ASCII  ");
    out_file->Append(res_key.c_str());
    out_file->Append("\n  ------\n");
  }
  out_file->Append("\n");
  return Status::OK();
}

Status ColumnTable::DumpDataBlocks(WritableFile* out_file) {
  std::unique_ptr<InternalIterator> blockhandles_iter(
      NewIndexIterator(ReadOptions()));
  Status s = blockhandles_iter->status();
  if (!s.ok()) {
    out_file->Append("Can not read Index Block \n\n");
    return s;
  }

  size_t block_id = 1;
  for (blockhandles_iter->SeekToFirst(); blockhandles_iter->Valid();
       block_id++, blockhandles_iter->Next()) {
    s = blockhandles_iter->status();
    if (!s.ok()) {
      break;
    }

    out_file->Append("Data Block # ");
    out_file->Append(vidardb::ToString(block_id));
    out_file->Append(" @ ");
    out_file->Append(blockhandles_iter->value().ToString(true).c_str());
    out_file->Append("\n");
    out_file->Append("--------------------------------------\n");

    std::unique_ptr<InternalIterator> datablock_iter;
    datablock_iter.reset(
        NewDataBlockIterator(rep_, ReadOptions(), blockhandles_iter->value()));
    s = datablock_iter->status();

    if (!s.ok()) {
      out_file->Append("Error reading the block - Skipped \n\n");
      continue;
    }

    for (datablock_iter->SeekToFirst(); datablock_iter->Valid();
         datablock_iter->Next()) {
      s = datablock_iter->status();
      if (!s.ok()) {
        out_file->Append("Error reading the block - Skipped \n");
        break;
      }
      Slice key = datablock_iter->key();
      Slice value = datablock_iter->value();
      InternalKey ikey, iValue;
      ikey.DecodeFrom(key);
      iValue.DecodeFrom(value);

      out_file->Append("  HEX    ");
      out_file->Append(ikey.user_key().ToString(true).c_str());
      out_file->Append(": ");
      out_file->Append(iValue.user_key().ToString(true).c_str());
      out_file->Append("\n");

      std::string str_key = ikey.user_key().ToString();
      std::string str_value = iValue.user_key().ToString();
      std::string res_key(""), res_value("");
      char cspace = ' ';
      for (size_t i = 0; i < str_key.size(); i++) {
        res_key.append(&str_key[i], 1);
        res_key.append(1, cspace);
      }
      for (size_t i = 0; i < str_value.size(); i++) {
        res_value.append(&str_value[i], 1);
        res_value.append(1, cspace);
      }

      out_file->Append("  ASCII  ");
      out_file->Append(res_key.c_str());
      out_file->Append(": ");
      out_file->Append(res_value.c_str());
      out_file->Append("\n  ------\n");
    }
    out_file->Append("\n");
  }
  return Status::OK();
}

Status ColumnTable::Open(const ImmutableCFOptions& ioptions,
                         const EnvOptions& env_options,
                         const ColumnTableOptions& table_options,
                         const InternalKeyComparator& internal_comparator,
                         unique_ptr<RandomAccessFileReader>&& file,
                         uint64_t file_size,
                         unique_ptr<TableReader>* table_reader,
                         const bool prefetch_index, const int level,
                         const std::vector<uint32_t>& cols) {
  table_reader->reset();

  Footer footer;
  auto s = ReadFooterFromFile(file.get(), file_size, &footer,
                              kColumnTableMagicNumber);
  if (!s.ok()) {
    return s;
  }

  // We've successfully read the footer and the index block: we're
  // ready to serve requests.
  Rep* rep = new ColumnTable::Rep(ioptions, env_options, table_options,
                                  internal_comparator);
  rep->file = std::move(file);
  rep->footer = footer;
  SetupCacheKeyPrefix(rep, file_size);

  // Read meta index
  std::unique_ptr<Block> meta;
  std::unique_ptr<InternalIterator> meta_iter;
  s = ReadMetaBlock(rep, &meta, &meta_iter);
  if (!s.ok()) {
    return s;
  }

  // Read column block
  bool found_column_block = true;
  s = SeekToColumnBlock(meta_iter.get(), &found_column_block);
  if (!s.ok()) {
    Log(InfoLogLevel::WARN_LEVEL, rep->ioptions.info_log,
        "Cannot seek to column block from file: %s", s.ToString().c_str());
  } else if (found_column_block) {
    s = meta_iter->status();
    if (!s.ok()) {
      return s;
    }

    uint32_t column_count;
    std::vector<uint64_t> file_sizes;
    s = ReadMetaColumnBlock(meta_iter->value(), rep->file.get(), rep->footer,
                            ioptions.env, ioptions.info_log, &column_count,
                            file_sizes);
    if (!s.ok()) {
      return s;
    }

    rep->tables.resize(column_count);
    rep->main_column = rep->tables.empty()? false: true;
    if (rep->main_column && column_count != rep->table_options.column_count) {
      return Status::InvalidArgument("table_options.column_count");
    }
    if (rep->main_column) {
      rep->column_comparator.reset(new ColumnKeyComparator());
    }

    size_t readahead = rep->file->file()->ReadaheadSize();
    std::string fname = rep->file->file()->GetFileName();
    for (auto i = 0u; i < column_count; i++) {
      // filter unnecessary columns, cols starts from 0 to MAX_COLUMN_INDEX and
      // index 0 means only querying the user keys, and the value column index
      // is from 1 to MAX_COLUMN_INDEX
      if (!cols.empty() &&
          std::find(cols.begin(), cols.end(), i+1) == cols.end()) {
        continue;
      }
      std::string col_fname = TableSubFileName(fname, i+1);
      unique_ptr<RandomAccessFile> col_file;
      s = rep->ioptions.env->NewRandomAccessFile(col_fname, &col_file,
                                                 env_options);
      if (!s.ok()) {
        return s;
      }
      if (readahead > 0) {
        col_file = NewReadaheadRandomAccessFile(std::move(col_file), readahead);
      }
      unique_ptr<RandomAccessFileReader> file_reader(
          new RandomAccessFileReader(std::move(col_file), ioptions.env));
      unique_ptr<TableReader> table;
      s = Open(ioptions, env_options, table_options, *(rep->column_comparator),
               std::move(file_reader), file_sizes[i], &table, prefetch_index,
               level);
      if (!s.ok()) {
        return s;
      }
      rep->tables[i].reset(dynamic_cast<ColumnTable*>(table.release()));
    }
  }

  // Read the properties
  bool found_properties_block = true;
  s = SeekToPropertiesBlock(meta_iter.get(), &found_properties_block);
  if (!s.ok()) {
    Log(InfoLogLevel::WARN_LEVEL, rep->ioptions.info_log,
        "Cannot seek to properties block from file: %s", s.ToString().c_str());
  } else if (found_properties_block) {
    s = meta_iter->status();
    TableProperties* table_properties = nullptr;
    if (s.ok()) {
      s = ReadProperties(meta_iter->value(), rep->file.get(), rep->footer,
                         rep->ioptions.env, rep->ioptions.info_log,
                         &table_properties);
    }

    if (!s.ok()) {
      Log(InfoLogLevel::WARN_LEVEL, rep->ioptions.info_log,
        "Encountered error while reading data from properties "
        "block %s", s.ToString().c_str());
    } else {
      rep->table_properties.reset(table_properties);
    }
  } else {
    Log(InfoLogLevel::ERROR_LEVEL, rep->ioptions.info_log,
        "Cannot find Properties block from file.");
  }

  // Read the compression dictionary meta block
  bool found_compression_dict;
  s = SeekToCompressionDictBlock(meta_iter.get(), &found_compression_dict);
  if (!s.ok()) {
    Log(InfoLogLevel::WARN_LEVEL, rep->ioptions.info_log,
        "Cannot seek to compression dictionary block from file: %s",
        s.ToString().c_str());
  } else if (found_compression_dict) {
    unique_ptr<BlockContents> compression_dict_block(new BlockContents());
    s = vidardb::ReadMetaBlock(rep->file.get(), file_size,
                               kColumnTableMagicNumber, rep->ioptions.env,
                               vidardb::kCompressionDictBlock,
                               compression_dict_block.get());
    if (!s.ok()) {
      Log(InfoLogLevel::WARN_LEVEL, rep->ioptions.info_log,
          "Encountered error while reading data from compression dictionary "
          "block %s",
          s.ToString().c_str());
    } else {
      rep->compression_dict_block = std::move(compression_dict_block);
    }
  }

  unique_ptr<ColumnTable> new_table(new ColumnTable(rep));
  if (prefetch_index) {
    // pre-fetching of blocks is turned on
    // If we don't use block cache for index blocks access, we'll
    // pre-load these blocks, which will kept in member variables in Rep
    // and with a same life-time as this table object.
    IndexReader* index_reader = nullptr;
    s = new_table->CreateIndexReader(&index_reader);

    if (s.ok()) {
      rep->index_reader.reset(index_reader);
    } else {
      delete index_reader;
    }
  }

  if (s.ok()) {
    *table_reader = std::move(new_table);
  }

  return s;
}

class ColumnTable::BlockEntryIteratorState : public TwoLevelIteratorState {
 public:
  BlockEntryIteratorState(ColumnTable* table,
                          const ReadOptions& read_options)
      : TwoLevelIteratorState(),
        table_(table),
        read_options_(read_options) {}

  InternalIterator* NewSecondaryIterator(const Slice& index_value) override {
    return NewDataBlockIterator(table_->rep_, read_options_, index_value);
  }

 private:
  // Don't own table_
  ColumnTable* table_;
  const ReadOptions read_options_;
};

class ColumnTable::ColumnIterator : public InternalIterator {
 public:
  ColumnIterator(const std::vector<InternalIterator*>& columns,
                 std::shared_ptr<Splitter> splitter,
                 bool has_main_column,
                 const InternalKeyComparator& internal_comparator,
                 uint64_t num_entries = 0)
  : columns_(columns), splitter_(splitter), has_main_column_(has_main_column),
    internal_comparator_(internal_comparator), num_entries_(num_entries) {}

  virtual ~ColumnIterator() {
    for (const auto& it : columns_) {
      it->~InternalIterator();
    }
  }

  virtual bool Valid() const {
    for (const auto& it : columns_) {
      if (!it->Valid()) {
        return false;
      }
    }
    return true;
  }

  virtual void SeekToFirst() {
    for (const auto& it : columns_) {
      it->SeekToFirst();
    }
    ParseCurrentValue();
  }

  virtual void SeekToLast() {
    for (const auto& it : columns_) {
      it->SeekToLast();
    }
    ParseCurrentValue();
  }

  virtual void Seek(const Slice& target) {
    Slice sub_column_target = target;
    for (auto i = 0u; i < columns_.size(); i++) {
      const auto& it = columns_[i];
      if (has_main_column_ && i==0u) {
        it->Seek(target);
        if (!it->Valid()) {
          return;
        }
        // set sub column target key
        sub_column_target = it->value();
      } else {
        it->Seek(sub_column_target);
        if (!it->Valid()) {
          return;
        }
      }
    }
    ParseCurrentValue();
  }

  virtual void Next() {
    assert(Valid());
    for (const auto& it : columns_) {
      it->Next();
    }
    ParseCurrentValue();
  }

  virtual void Prev() {
    assert(Valid());
    for (const auto& it : columns_) {
      it->Prev();
    }
    ParseCurrentValue();
  }

  virtual Slice key() const {
    assert(Valid());
    return columns_[0]->key();
  }

  virtual Slice value() const {
    assert(Valid());
    return value_;
  }

  virtual Status status() const {
    if (!status_.ok()) {
      return status_;
    }
    Status s;
    for (const auto& it : columns_) {
      s = it->status();
      if (!s.ok()) {
        break;
      }
    }
    return s;
  }

  virtual Status RangeQuery(ReadOptions& read_options, const LookupRange& range,
                            std::list<RangeQueryKeyVal>& res) {
    std::vector<std::map<std::string, SeqTypeVal>::iterator> user_vals;
    std::vector<bool> sub_key_bs; // track the valid sub_keys
    if (num_entries_ > 0) {
      user_vals.reserve(num_entries_);
      sub_key_bs.reserve(num_entries_);
    }

    std::string start_sub_key;  // track start sub key
    SequenceNumber sequence_num = range.SequenceNum();
    RangeQueryMeta* meta =
        static_cast<RangeQueryMeta*>(read_options.range_query_meta);

    // Range query one by one to improve performance
    for (size_t i = 0u; i < columns_.size(); i++) {
      InternalIterator* iter = columns_[i];

      if (i == 0) {  // query sub keys from main column
        if (range.start_->user_key().compare(kRangeQueryMin) == 0) {
          iter->SeekToFirst(); // Full search
        } else {
          iter->Seek(range.start_->internal_key());
        }

        for (; iter->Valid(); iter->Next()) {  // main iterator
          if (CompareRangeLimit(internal_comparator_, iter->key(),
                                meta->current_limit_key) > 0) {
            break;
          }

          ParsedInternalKey parsed_key;
          if (!ParseInternalKey(iter->key(), &parsed_key)) {
            return Status::Corruption("corrupted internal key in Table::Iter");
          }

          if (parsed_key.sequence <= sequence_num) {
            if (start_sub_key.empty()) {
              start_sub_key.assign(iter->value().data_, iter->value().size_);
            }

            std::string user_key(iter->key().data(), iter->key().size() - 8);
            SeqTypeVal stv(parsed_key.sequence, parsed_key.type, res.end());

            // give accurate hint
            auto it = meta->map_res.end();
            if (!user_vals.empty()) {
              it = user_vals.back();
              it++;
            }
            it = meta->map_res.emplace_hint(it, user_key, std::move(stv));
            if (it->second.seq_ > parsed_key.sequence) {
              // already exists the same user key, which shadows the current
              sub_key_bs.push_back(false);
              continue;
            }

            // two cases:
            // 1. already exists the same user key, invalidate the old one
            // 2. same seq, the current one
            sub_key_bs.push_back(true);
            if (it->second.seq_ < parsed_key.sequence) {
              // replaced
              if (it->second.type_ == kTypeDeletion) {
                meta->del_keys.erase(it->second.seq_);
              }
              assert(read_options.result_val_size >=
                  it->second.iter_->user_val.size());
              read_options.result_val_size -= it->second.iter_->user_val.size();
              it->second.seq_ = parsed_key.sequence;
              it->second.type_ = parsed_key.type;
              it->second.iter_->user_val = "";
              user_vals.push_back(std::move(it));
              if (parsed_key.type == kTypeDeletion) {
                meta->del_keys.insert({parsed_key.sequence, it->second.iter_});
              }
            } else {
              // inserted
              res.emplace_back(user_key, "");
              read_options.result_key_size += user_key.size();
              it->second.iter_ = --res.end();
              user_vals.push_back(std::move(it));
              if (parsed_key.type == kTypeDeletion) {
                meta->del_keys.insert({parsed_key.sequence, it->second.iter_});
              }

              if (CompressResultList(&res, read_options)
                  && meta->map_res.rbegin()->first <= user_key) {
                if (meta->map_res.rbegin()->first < user_key) {
                  // element added but popped out in map, remove in bits as well
                  sub_key_bs.pop_back();
                  user_vals.pop_back();
                }
                break;  // Reach the batch capacity
              }
            }

          } else {
            sub_key_bs.push_back(false);
          }
        }
      } else {  // loop query all sub column values
        if (!start_sub_key.empty()) {
          iter->Seek(start_sub_key);
        } else {  // not found valid keys
          break;
        }

        size_t sub_key_idx = 0, user_val_idx = 0;
        for (; iter->Valid() && sub_key_idx < sub_key_bs.size(); iter->Next()) {
          if (!sub_key_bs[sub_key_idx++]) {  // follow sub_key's order
            continue;
          }

          auto& it = user_vals[user_val_idx++]->second.iter_;
          size_t prev_val_size = it->user_val.size();
          splitter_->Append(it->user_val, iter->value(),
                            i + 1 >= columns_.size());
          size_t delta_val_size = it->user_val.size() - prev_val_size;
          read_options.result_val_size += delta_val_size;
        }
      }
    }

    return Status();
  }

  virtual void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) {
    for (const auto& it : columns_) {
      it->SetPinnedItersMgr(pinned_iters_mgr);
    }
  }

  virtual bool IsKeyPinned() const {
    for (const auto& it : columns_) {
      if (!it->IsKeyPinned()) {
        return false;
      }
    }
    return true;
  }

 private:
  inline bool ParseCurrentValue() {
    value_.clear();
    for (auto i = 0u; i < columns_.size(); i++) {
      if (!columns_[i]->Valid()) {
        return false;
      }
      if (has_main_column_ && i==0u) {
        // skip main column (only key)
        continue;
      }
      splitter_->Append(value_, columns_[i]->value(), i + 1 >= columns_.size());
    }
    return true;
  }

  std::vector<InternalIterator*> columns_;
  std::string value_;
  std::shared_ptr<Splitter> splitter_;
  Status status_;
  bool has_main_column_;  // true in NewIterator, false in Get & Prefetch
  const InternalKeyComparator& internal_comparator_; // used in rangrquery
  uint64_t num_entries_;  // used in rangrquery
};

// Note: Column index must be from 0 to MAX_COLUMN_INDEX.
//       Index 0 means only querying the user keys, and 
//       the value column index is from 1 to MAX_COLUMN_INDEX.
inline static ReadOptions SanitizeColumnReadOptions(
    uint32_t column_count, const ReadOptions& read_options) {
  ReadOptions ro = read_options;
  if (ro.columns.empty()) {  // all value columns
    // Let's add index=0 to columns, since it gets filtered by current callers.
    // Keep 0 here to be consistent within semantics in case of future callers.
    ro.columns.resize(column_count + 1);
    for (uint32_t i = 0; i <= column_count; i++) {
      ro.columns[i] = i;  // from 0 to MAX_COLUMN_INDEX
    }
    return ro;
  }

  return ro;
}

InternalIterator* ColumnTable::NewIterator(const ReadOptions& read_options,
                                           Arena* arena) {
  ReadOptions ro = SanitizeColumnReadOptions(
      rep_->table_options.column_count, read_options);

  std::vector<InternalIterator*> iters;  // main column
  iters.push_back(NewTwoLevelIterator(new BlockEntryIteratorState(this, ro),
                                      NewIndexIterator(ro), arena));
  for (const auto& column_index : ro.columns) {  // sub column
    if (column_index < 1) {  // only process the value columns
      continue;
    }
    iters.push_back(NewTwoLevelIterator(
        new BlockEntryIteratorState(rep_->tables[column_index-1].get(), ro),
        rep_->tables[column_index-1]->NewIndexIterator(ro), arena));
  }
  return new ColumnIterator(iters, rep_->table_options.splitter,
                            true, rep_->internal_comparator,
                            rep_->table_properties->num_entries);
}

Status ColumnTable::Get(const ReadOptions& read_options, const Slice& key,
                        GetContext* get_context) {
  ReadOptions ro = SanitizeColumnReadOptions(
      rep_->table_options.column_count, read_options);
  BlockIter iiter;
  NewIndexIterator(ro, &iiter);

  Status s;
  bool done = false;
  for (iiter.Seek(key); iiter.Valid() && !done; iiter.Next()) {
    std::unique_ptr<InternalIterator> biter;
    biter.reset(NewDataBlockIterator(rep_, ro, iiter.value()));
    if (ro.read_tier == kBlockCacheTier && biter->status().IsIncomplete()) {
      // couldn't get block from block_cache
      // Update Saver.state to Found because we are only looking for whether
      // we can guarantee the key is not there when "no_io" is set
      get_context->MarkKeyMayExist();
      break;
    }
    if (!biter->status().ok()) {
      s = biter->status();
      break;
    }

    bool isIncomplete = false;
    // Call the *saver function on each entry/block until it returns false
    for (biter->Seek(key); biter->Valid(); biter->Next()) {
      ParsedInternalKey parsed_key;
      if (!ParseInternalKey(biter->key(), &parsed_key)) {
        s = Status::Corruption(Slice());
        break;
      }

      // early filter, defer citers as much as possible
      if (!get_context->IsEqualToUserKey(parsed_key)) {
        done = true;
        break;
      }

      std::vector<InternalIterator*> iters;
      for (const auto& it : ro.columns) {
        if (it < 1) {  // only process the value columns
          continue;
        }
        iters.push_back(NewTwoLevelIterator(
            new BlockEntryIteratorState(rep_->tables[it-1].get(), ro),
            rep_->tables[it-1]->NewIndexIterator(ro)));
      }

      ColumnIterator citers(iters, rep_->table_options.splitter,
                            false, rep_->internal_comparator, /* might change*/
                            rep_->table_properties->num_entries);
      if (!citers.status().ok()) {
        s = citers.status();
        break;
      }

      citers.Seek(biter->value());
      if (ro.read_tier == kBlockCacheTier && citers.status().IsIncomplete()) {
        isIncomplete = true;
        get_context->MarkKeyMayExist();
        break;
      }
      if (!citers.status().ok()) {
        s = citers.status();
        break;
      }

      if (!get_context->SaveValue(parsed_key, citers.value())) {
        done = true;
        break;
      }
    }

    if (isIncomplete || !s.ok()) {
      break;
    } else {
      s = biter->status();
    }
  }
  if (s.ok()) {
    s = iiter.status();
  }

  return s;
}

Status ColumnTable::Prefetch(const Slice* const begin, const Slice* const end) {
  return Prefetch(begin, end, ReadOptions());
}

Status ColumnTable::Prefetch(const Slice* const begin, const Slice* const end,
                             const ReadOptions& read_options) {
  ReadOptions ro = SanitizeColumnReadOptions(
      rep_->table_options.column_count, read_options);
  auto& comparator = rep_->internal_comparator;
  // pre-condition
  if (begin && end && comparator.Compare(*begin, *end) > 0) {
    return Status::InvalidArgument(*begin, *end);
  }

  BlockIter iiter;
  NewIndexIterator(ro, &iiter);

  if (!iiter.status().ok()) {
    // error opening index iterator
    return iiter.status();
  }

  // indicates if we are on the last page that need to be pre-fetched
  bool prefetching_boundary_page = false;

  for (begin ? iiter.Seek(*begin) : iiter.SeekToFirst(); iiter.Valid();
       iiter.Next()) {

    if (end && comparator.Compare(iiter.key(), *end) >= 0) {
      if (prefetching_boundary_page) {
        break;
      }

      // The index entry represents the last key in the data block.
      // We should load this page into memory as well, but no more
      prefetching_boundary_page = true;
    }

    // Load the block specified by the block_handle into the block cache
    std::unique_ptr<InternalIterator> biter;
    biter.reset(NewDataBlockIterator(rep_, ro, iiter.value()));
    if (!biter->status().ok()) {
      // there was an unexpected error while pre-fetching
      return biter->status();
    }

    biter->SeekToFirst();
    if (!biter->status().ok()) {
      return biter->status();
    }

    std::vector<InternalIterator*> iters;
    for (const auto& it : ro.columns) {
      if (it < 1) {  // only process the value columns
        continue;
      }
      iters.push_back(NewTwoLevelIterator(
          new BlockEntryIteratorState(rep_->tables[it-1].get(), ro),
          rep_->tables[it-1]->NewIndexIterator(ro)));
    }

    ColumnIterator citers(iters, rep_->table_options.splitter,
                          false, rep_->internal_comparator,
                          rep_->table_properties->num_entries);
    if (!citers.status().ok()) {
      return citers.status();
    }

    citers.Seek(biter->value());
    if (!citers.status().ok()) {
      return citers.status();
    }
  }

  return Status::OK();
}

uint64_t ColumnTable::ApproximateOffsetOf(const Slice& key) {
  unique_ptr<InternalIterator> index_iter(NewIndexIterator(ReadOptions()));

  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->footer.metaindex_handle().offset();
    }
  } else {
    // key is past the last key in the file. If table_properties is not
    // available, approximate the offset by returning the offset of the
    // metaindex block (which is right near the end of the file).
    result = 0;
    if (rep_->table_properties) {
      result = rep_->table_properties->data_size;
    }
    // table_properties is not present in the table.
    if (result == 0) {
      result = rep_->footer.metaindex_handle().offset();
    }
  }

  if (!rep_->main_column) {
    return result;
  }

  if (index_iter->Valid()) {
    std::unique_ptr<InternalIterator> datablock_iter;
    datablock_iter.reset(NewDataBlockIterator(rep_, ReadOptions(),
                                              index_iter->value()));
    datablock_iter->SeekToFirst();
    Slice sub_column_key = datablock_iter->value();

    for (const auto& it : rep_->tables) {
      if (it) {
        result += it->ApproximateOffsetOf(sub_column_key);
      }
    }
  } else {
    for (const auto& it : rep_->tables) {
      if (it) {
        uint64_t sub_result = 0;
        if (it->rep_->table_properties) {
          sub_result = it->rep_->table_properties->data_size;
        }
        // table_properties is not present in the table.
        if (sub_result == 0) {
          sub_result = it->rep_->footer.metaindex_handle().offset();
        }
        result += sub_result;
      }
    }
  }

  return result;
}

void ColumnTable::SetupForCompaction() {
  switch (rep_->ioptions.access_hint_on_compaction_start) {
    case Options::NONE:
    case Options::NORMAL:
    case Options::WILLNEED:
    case Options::SEQUENTIAL:
      rep_->file->file()->Hint(RandomAccessFile::SEQUENTIAL);
      break;
    default:
      assert(false);
  }
  compaction_optimized_ = true;
  for (const auto& it : rep_->tables) {
    if (it) {
      it->SetupForCompaction();
    }
  }
}

std::shared_ptr<const TableProperties> ColumnTable::GetTableProperties() const {
  return rep_->table_properties;
}

size_t ColumnTable::ApproximateMemoryUsage() const {
  size_t usage = 0;
  if (rep_->index_reader) {
    usage += rep_->index_reader->ApproximateMemoryUsage();
  }
  for (const auto& it : rep_->tables) {
    if (it) {
      usage += it->ApproximateMemoryUsage();
    }
  }
  return usage;
}

Status ColumnTable::DumpTable(WritableFile* out_file) {
  // Output Footer
  out_file->Append(
      "Footer Details:\n"
      "--------------------------------------\n"
      "  ");
  out_file->Append(rep_->footer.ToString().c_str());
  out_file->Append("\n");

  // Output MetaIndex
  out_file->Append(
      "Metaindex Details:\n"
      "--------------------------------------\n");
  std::unique_ptr<Block> meta;
  std::unique_ptr<InternalIterator> meta_iter;
  Status s = ReadMetaBlock(rep_, &meta, &meta_iter);
  if (s.ok()) {
    for (meta_iter->SeekToFirst(); meta_iter->Valid(); meta_iter->Next()) {
      s = meta_iter->status();
      if (!s.ok()) {
        return s;
      }
      if (meta_iter->key() == vidardb::kPropertiesBlock) {
        out_file->Append("  Properties block handle: ");
        out_file->Append(meta_iter->value().ToString(true).c_str());
        out_file->Append("\n");
      } else if (meta_iter->key() == vidardb::kCompressionDictBlock) {
        out_file->Append("  Compression dictionary block handle: ");
        out_file->Append(meta_iter->value().ToString(true).c_str());
        out_file->Append("\n");
      }
    }
    out_file->Append("\n");
  } else {
    return s;
  }

  // Output TableProperties
  const vidardb::TableProperties* table_properties;
  table_properties = rep_->table_properties.get();

  if (table_properties != nullptr) {
    out_file->Append(
        "Table Properties:\n"
        "--------------------------------------\n"
        "  ");
    out_file->Append(table_properties->ToString("\n  ", ": ").c_str());
    out_file->Append("\n");
  }

  // Output Index block
  s = DumpIndexBlock(out_file);
  if (!s.ok()) {
    return s;
  }
  // Output Data blocks
  s = DumpDataBlocks(out_file);

  return s;
}

void ColumnTable::Close() {
  // cleanup index blocks to avoid accessing dangling pointer
  if (!rep_->table_options.no_block_cache) {
    char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
    // Get the index block key
    auto key = GetCacheKeyFromOffset(rep_->cache_key_prefix,
                                rep_->cache_key_prefix_size,
                                rep_->dummy_index_reader_offset, cache_key);
    rep_->table_options.block_cache.get()->Erase(key);
  }
  for (const auto& it : rep_->tables) {
    if (it) {
      it->Close();
    }
  }
}

ColumnTable::~ColumnTable() {
  if (rep_->main_column) {
    Close();
  }
  delete rep_;
}

}  // namespace vidardb
