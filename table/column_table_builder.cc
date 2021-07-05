//  Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "table/column_table_builder.h"

#include <assert.h>
#include <inttypes.h>
#include <stdio.h>

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "db/dbformat.h"
#include "db/filename.h"
#include "table/block.h"
#include "table/column_table_factory.h"
#include "table/column_table_reader.h"
#include "table/format.h"
#include "table/index_builder.h"
#include "table/main_column_block_builder.h"
#include "table/meta_blocks.h"
#include "table/min_max_block_builder.h"
#include "table/sub_column_block_builder.h"
#include "table/table_builder.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "vidardb/cache.h"
#include "vidardb/comparator.h"
#include "vidardb/env.h"
#include "vidardb/flush_block_policy.h"
#include "vidardb/splitter.h"
#include "vidardb/table.h"

namespace vidardb {

// Without anonymous namespace here, we fail the warning -Wmissing-prototypes
namespace {

// Create a index builder based on its type.
IndexBuilder* CreateIndexBuilder(const Comparator* comparator,
                                 int index_block_restart_interval,
                                 const Comparator* value_comparator) {
  if (value_comparator == nullptr) {
    return new ShortenedIndexBuilder(comparator, index_block_restart_interval);
  } else {
    return new MinMaxShortenedIndexBuilder(
        comparator, index_block_restart_interval, value_comparator);
  }
}

bool GoodCompressionRatio(size_t compressed_size, size_t raw_size) {
  // Check to see if compressed less than 12.5%
  return compressed_size < raw_size - (raw_size / 8u);
}

Slice CompressBlock(const Slice& raw,
                    const CompressionOptions& compression_options,
                    CompressionType* type, const Slice& compression_dict,
                    std::string* compressed_output) {
  if (*type == kNoCompression) {
    return raw;
  }

  // Will return compressed block contents if (1) the compression method is
  // supported in this platform and (2) the compression rate is "good enough".
  switch (*type) {
    case kSnappyCompression:
      if (Snappy_Compress(compression_options, raw.data(), raw.size(),
                          compressed_output) &&
          GoodCompressionRatio(compressed_output->size(), raw.size())) {
        return *compressed_output;
      }
      break;  // fall back to no compression.
    case kZlibCompression:
      if (Zlib_Compress(
              compression_options,
              GetCompressFormatForVersion(kZlibCompression),
              raw.data(), raw.size(), compressed_output, compression_dict) &&
          GoodCompressionRatio(compressed_output->size(), raw.size())) {
        return *compressed_output;
      }
      break;  // fall back to no compression.
    case kBZip2Compression:
      if (BZip2_Compress(
              compression_options,
              GetCompressFormatForVersion(kBZip2Compression),
              raw.data(), raw.size(), compressed_output) &&
          GoodCompressionRatio(compressed_output->size(), raw.size())) {
        return *compressed_output;
      }
      break;  // fall back to no compression.
    default: {}  // Do not recognize this compression type
  }

  // Compression method is not supported, or not good compression ratio, so just
  // fall back to uncompressed form.
  *type = kNoCompression;
  return raw;
}

}  // namespace

// Slight change from kBlockBasedTableMagicNumber.
// Please note that kColumnTableMagicNumber may also be accessed by other
// .cc files
// for that reason we declare it extern in the header but to get the space
// allocated
// it must be not extern in one place.
const uint64_t kColumnTableMagicNumber = 0x88e241b785f4cfffull;

struct ColumnTableBuilder::Rep {
  uint32_t column_num;
  const ImmutableCFOptions ioptions;
  const ColumnTableOptions table_options;
  const InternalKeyComparator& internal_comparator;
  std::unique_ptr<ColumnKeyComparator> column_comparator;
  WritableFileWriter* file;
  uint64_t offset = 0;
  Status status;
  std::unique_ptr<BlockBuilder> data_block;

  std::unique_ptr<IndexBuilder> index_builder;

  std::string last_key;
  const CompressionType compression_type;
  const CompressionOptions compression_opts;
  // Data for presetting the compression library's dictionary, or nullptr.
  const std::string* compression_dict;
  TableProperties props;

  bool closed = false;  // Either Finish() or Abandon() has been called.

  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
  std::unique_ptr<FlushBlockPolicy> flush_block_policy;
  uint32_t column_family_id;
  const std::string& column_family_name;

  std::vector<std::unique_ptr<IntTblPropCollector>> table_properties_collectors;

  const EnvOptions& env_options;
  std::vector<std::unique_ptr<ColumnTableBuilder>> builders;

  Rep(uint32_t _column_num, const ImmutableCFOptions& _ioptions,
      const ColumnTableOptions& table_opt,
      const InternalKeyComparator& icomparator,
      const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
          int_tbl_prop_collector_factories,
      uint32_t _column_family_id, WritableFileWriter* f,
      const CompressionType _compression_type,
      const CompressionOptions& _compression_opts,
      const std::string* _compression_dict,
      const std::string& _column_family_name, const EnvOptions& _env_options)
      : column_num(_column_num),
        ioptions(_ioptions),
        table_options(table_opt),
        internal_comparator(icomparator),
        column_comparator(column_num == 0 ? new ColumnKeyComparator()
                                          : nullptr),
        file(f),
        index_builder(CreateIndexBuilder(
            &internal_comparator, table_options.index_block_restart_interval,
            (column_num == 0)
                ? nullptr
                : table_options.value_comparators[column_num - 1])),
        compression_type(_compression_type),
        compression_opts(_compression_opts),
        compression_dict(_compression_dict),
        column_family_id(_column_family_id),
        column_family_name(_column_family_name),
        env_options(_env_options) {
    if (column_num == 0) {
      data_block.reset(
          new MainColumnBlockBuilder(table_options.block_restart_interval));
      flush_block_policy.reset(
          table_options.flush_block_policy_factory->NewFlushBlockPolicy(
              table_options, *data_block));
    } else {
      data_block.reset(
          new SubColumnBlockBuilder(table_options.block_restart_interval));
      flush_block_policy.reset(nullptr);
    }

    if (column_num == 0 && int_tbl_prop_collector_factories) {
      for (auto& collector_factories : *int_tbl_prop_collector_factories) {
        table_properties_collectors.emplace_back(
            collector_factories->CreateIntTblPropCollector(column_family_id));
      }
    }
  }
};

ColumnTableBuilder::ColumnTableBuilder(
    const ImmutableCFOptions& ioptions, const ColumnTableOptions& table_options,
    const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id, WritableFileWriter* file,
    const CompressionType compression_type,
    const CompressionOptions& compression_opts,
    const std::string* compression_dict, const std::string& column_family_name,
    const EnvOptions& env_options, uint32_t column_num) {
  rep_ = new Rep(column_num, ioptions, table_options, internal_comparator,
                 int_tbl_prop_collector_factories, column_family_id, file,
                 compression_type, compression_opts, compression_dict,
                 column_family_name, env_options);
}

ColumnTableBuilder::~ColumnTableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_;
}

void ColumnTableBuilder::CreateSubcolumnBuilders(Rep* r) {
  r->builders.resize(r->table_options.column_count);
  std::string fname = r->file->writable_file()->GetFileName();
  Env::IOPriority pri = r->file->writable_file()->GetIOPriority();
  for (auto i = 0u; i < r->table_options.column_count; i++) {
    unique_ptr<WritableFile> file;
    std::string col_fname(TableSubFileName(fname, i+1));
    r->status = NewWritableFile(r->ioptions.env, col_fname, &file,
                                r->env_options);
    assert(r->status.ok());
    file->SetIOPriority(pri);
    r->builders[i].reset(new ColumnTableBuilder(
        r->ioptions, r->table_options, *(r->column_comparator), nullptr,
        r->column_family_id,
        new WritableFileWriter(std::move(file), r->env_options),
        r->compression_type, r->compression_opts, r->compression_dict,
        r->column_family_name, r->env_options, i + 1));
  }
}

void ColumnTableBuilder::AddInSubcolumnBuilders(Rep* r, const Slice& key,
                                                const Slice& value,
                                                bool should_flush) {
  std::vector<Slice> vals(r->ioptions.splitter->Split(value));
  if (!vals.empty() && vals.size() != r->table_options.column_count) {
    r->status = Status::InvalidArgument("table_options.column_count");
    return;
  }

  for (auto i = 0u; i < r->table_options.column_count; i++) {
    const auto& rep = r->builders[i]->rep_;
    assert(!rep->closed);
    if (!r->builders[i]->ok()) return;
    if (rep->props.num_entries > 0) {
      assert(rep->internal_comparator.Compare(key, Slice(rep->last_key)) > 0);
    }

    // instead of fixed block size, every block has the same amount of attribute
    // values horizontally
    if (should_flush) {
      assert(!rep->data_block->empty());
      r->builders[i]->Flush();
      if (r->builders[i]->ok()) {
        rep->index_builder->AddIndexEntry(&rep->last_key, &key,
                                          rep->pending_handle);
      }
    }

    rep->last_key.assign(key.data(), key.size());
    // sub column format (, vals[i]): (, vals[i+0]), (, vals[i+1])
    // however, key is stored in the first elem of every restart
    rep->data_block->Add(key, vals.empty()? Slice(): vals[i]);
    rep->props.num_entries++;
    rep->props.raw_key_size += rep->data_block->IsKeyStored() ? key.size() : 0;
    rep->props.raw_value_size += vals.empty()? 0: vals[i].size();
    rep->index_builder->OnKeyAdded(vals.empty()? Slice(): vals[i]);
  }
}

void ColumnTableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->props.num_entries > 0) {
    assert(r->internal_comparator.Compare(key, Slice(r->last_key)) > 0);
  }

  // We create subcolumn builders here
  if (r->builders.empty()) {
    CreateSubcolumnBuilders(r);
  }

  // Be careful about big endian and small endian issue
  // when comparing number with binary format
  std::string pos;
  PutFixed32BigEndian(&pos, r->props.num_entries);

  auto should_flush = r->flush_block_policy->Update(key, pos);
  if (should_flush) {
    assert(!r->data_block->empty());
    Flush();

    // Add item to index block.
    // We do not emit the index entry for a block until we have seen the
    // first key for the next data block. This allows us to use shorter
    // keys in the index block. For example, consider a block boundary
    // between the keys "the quick brown fox" and "the who". We can use
    // "the r" as the key for the index block entry since it is >= all
    // entries in the first block and < all entries in subsequent blocks.
    if (ok()) {
      r->index_builder->AddIndexEntry(&r->last_key, &key, r->pending_handle);
    }
  }

  r->last_key.assign(key.data(), key.size());
  // main column format (keyN, pos): (key0, 0), (key1, ) ...
  // pos is only stored at every restart
  r->data_block->Add(key, pos);
  r->props.num_entries++;
  r->props.raw_key_size += key.size();
  r->props.raw_value_size += pos.size();
  NotifyCollectTableCollectorsOnAdd(key, pos, r->offset,
                                    r->table_properties_collectors,
                                    r->ioptions.info_log);

  AddInSubcolumnBuilders(r, pos, value, should_flush);
}

void ColumnTableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block->empty()) return;
  WriteBlock(r->data_block.get(), &r->pending_handle, true /* is_data_block */);
  if (ok()) {
    r->status = r->file->Flush();
  }
  r->props.data_size = r->offset;
  ++r->props.num_data_blocks;
}

void ColumnTableBuilder::WriteBlock(BlockBuilder* block,
                                    BlockHandle* handle,
                                    bool is_data_block) {
  Slice raw_block_contents = block->Finish();
  WriteBlock(raw_block_contents, handle, is_data_block);
  if (is_data_block) {
    rep_->props.raw_data_size += raw_block_contents.size();
  }
  block->Reset();
}

void ColumnTableBuilder::WriteBlock(const Slice& raw_block_contents,
                                    BlockHandle* handle, bool is_data_block) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;

  auto type = r->compression_type;
  Slice block_contents;
  if (raw_block_contents.size() < kCompressionSizeLimit) {
    Slice compression_dict;
    if (is_data_block && r->compression_dict && r->compression_dict->size()) {
      compression_dict = *r->compression_dict;
    }
    block_contents = CompressBlock(raw_block_contents, r->compression_opts,
                                   &type, compression_dict,
                                   &r->compressed_output);
  } else {
    RecordTick(r->ioptions.statistics, NUMBER_BLOCK_NOT_COMPRESSED);
    type = kNoCompression;
    block_contents = raw_block_contents;
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
}

void ColumnTableBuilder::WriteRawBlock(const Slice& block_contents,
                                       CompressionType type,
                                       BlockHandle* handle) {
  Rep* r = rep_;
  StopWatch sw(r->ioptions.env, r->ioptions.statistics, WRITE_RAW_BLOCK_MICROS);
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    char* trailer_without_type = trailer + 1;

    auto crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend to cover block type
    EncodeFixed32(trailer_without_type, crc32c::Mask(crc));

    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status ColumnTableBuilder::status() const {
  for (const auto& it : rep_->builders) {
    if (it && !it->rep_->status.ok()) {
      return it->rep_->status;
    }
  }
  return rep_->status;
}

Status ColumnTableBuilder::Finish() {
  Rep* r = rep_;
  if (r->column_num == 0) {
    for (const auto& it : r->builders) {
      if (it) {
        it->rep_->status = it->Finish();
        if (!it->rep_->status.ok()) {
          return it->rep_->status;
        }
      }
    }
  }

  bool empty_data_block = r->data_block->empty();
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle metaindex_block_handle, index_block_handle,
      compression_dict_block_handle;

  // To make sure properties block is able to keep the accurate size of index
  // block, we will finish writing all index entries here and flush them to
  // storage after metaindex block is written.
  if (ok() && !empty_data_block) {
    r->index_builder->AddIndexEntry(
        &r->last_key, nullptr /* no next data block */, r->pending_handle);
  }

  IndexBuilder::IndexBlocks index_blocks;
  auto s = r->index_builder->Finish(&index_blocks);
  if (!s.ok()) {
    return s;
  }

  // Write meta blocks and metaindex block with the following order.
  //    1. [format, col_num; col_file_size...]
  //    2. [properties]
  //    3. [compression_dict]
  //    4. [meta_index_builder]
  //    5. [index_blocks]
  MetaIndexBuilder meta_index_builder;

  if (ok()) {
    // Write column block.
    {
      MetaColumnBlockBuilder meta_column_block_builder;
      uint32_t column_count = (uint32_t)r->builders.size();
      meta_column_block_builder.Add(r->column_num, column_count);
      for (auto i = 0u; i < column_count; i++) {
        meta_column_block_builder.Add(i + 1, r->builders[i]->rep_->offset);
      }

      BlockHandle column_block_handle;
      WriteRawBlock(meta_column_block_builder.Finish(), kNoCompression,
                    &column_block_handle);
      meta_index_builder.Add(kColumnBlock, column_block_handle);
    }

    // Write properties and compression dictionary blocks.
    {
      PropertyBlockBuilder property_block_builder;
      r->props.column_family_id = r->column_family_id;
      r->props.column_family_name = r->column_family_name;
      r->props.index_size =
          r->index_builder->EstimatedSize() + kBlockTrailerSize;
      r->props.comparator_name = r->ioptions.comparator != nullptr
                                     ? r->ioptions.comparator->Name()
                                     : "nullptr";
      r->props.compression_name = CompressionTypeToString(r->compression_type);

      if (r->column_num == 0) {
        std::string property_collectors_names = "[";
        for (size_t i = 0;
             i < r->ioptions.table_properties_collector_factories.size(); ++i) {
          if (i != 0) {
            property_collectors_names += ",";
          }
          property_collectors_names +=
              r->ioptions.table_properties_collector_factories[i]->Name();
        }
        property_collectors_names += "]";
        r->props.property_collectors_names = property_collectors_names;
      }

      // Add basic properties
      property_block_builder.AddTableProperty(r->props);

      // Add user collected properties
      if (r->column_num == 0) {
        NotifyCollectTableCollectorsOnFinish(r->table_properties_collectors,
                                             r->ioptions.info_log,
                                             &property_block_builder);
      }

      BlockHandle properties_block_handle;
      WriteRawBlock(property_block_builder.Finish(), kNoCompression,
                    &properties_block_handle);
      meta_index_builder.Add(kPropertiesBlock, properties_block_handle);

      // Write compression dictionary block
      if (r->compression_dict && r->compression_dict->size()) {
        WriteRawBlock(*r->compression_dict, kNoCompression,
                      &compression_dict_block_handle);
        meta_index_builder.Add(kCompressionDictBlock,
                               compression_dict_block_handle);
      }
    }  // end of properties/compression dictionary block writing
  }    // meta blocks

  // Write index block
  if (ok()) {
    // flush the meta index block
    WriteRawBlock(meta_index_builder.Finish(), kNoCompression,
                  &metaindex_block_handle);
    WriteBlock(index_blocks.index_block_contents, &index_block_handle, false);
  }

  // Write footer
  if (ok()) {
    // No need to write out new footer if we're using default checksum.
    Footer footer(kColumnTableMagicNumber);
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }

  // Different from blockbasedtable, we take care of subcolumn file sync and
  // close inside the builder
  if (r->column_num == 0) {
    for (const auto& it : r->builders) {
      if (it && it->rep_->status.ok()) {
        it->rep_->file->Sync(r->ioptions.use_fsync);
        it->rep_->file->Close();
      }
    }
  }

  return r->status;
}

void ColumnTableBuilder::Abandon() {
  Rep* r = rep_;
  for (const auto& it : r->builders) {
    if (it) {
      assert(!it->rep_->closed);
      it->rep_->closed = true;
    }
  }
  assert(!r->closed);
  r->closed = true;
}

uint64_t ColumnTableBuilder::NumEntries() const {
  return rep_->props.num_entries;
}

uint64_t ColumnTableBuilder::FileSize() const {
  return rep_->offset;
}

uint64_t ColumnTableBuilder::FileSizeTotal() const {
  uint64_t res = rep_->offset;
  for (const auto& it : rep_->builders) {
    if (it) {
      res += it->rep_->offset;
    }
  }
  return res;
}

bool ColumnTableBuilder::NeedCompact() const {
  for (const auto& collector : rep_->table_properties_collectors) {
    if (collector->NeedCompact()) {
      return true;
    }
  }
  return false;
}

TableProperties ColumnTableBuilder::GetTableProperties() const {
  TableProperties ret = rep_->props;
  for (const auto& collector : rep_->table_properties_collectors) {
    for (const auto& prop : collector->GetReadableProperties()) {
      ret.readable_properties.insert(prop);
    }
    collector->Finish(&ret.user_collected_properties);
  }
  return ret;
}

}  // namespace vidardb
