//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include <string>
#include <inttypes.h>

#include "vidardb/env.h"
#include "table/block.h"
#include "table/block_based_table_reader.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"
#include "util/perf_context_imp.h"
#include "util/string_util.h"

namespace vidardb {

extern const uint64_t kBlockBasedTableMagicNumber;

const uint32_t DefaultStackBufferSize = 5000;

void BlockHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) &&
      GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

// Return a string that contains the copy of handle.
std::string BlockHandle::ToString(bool hex) const {
  std::string handle_str;
  EncodeTo(&handle_str);
  if (hex) {
    return Slice(handle_str).ToString(true);
  } else {
    return handle_str;
  }
}

const BlockHandle BlockHandle::kNullBlockHandle(0, 0);

// new footer format:
//    checksum (char, 1 byte)
//    metaindex handle (varint64 offset, varint64 size)
//    index handle     (varint64 offset, varint64 size)
//    <padding> to make the total size 2 * BlockHandle::kMaxEncodedLength + 1
//    footer version (4 bytes)
//    table_magic_number (8 bytes)
void Footer::EncodeTo(std::string* dst) const {
  assert(HasInitializedTableMagicNumber());
  const size_t original_size = dst->size();
  metaindex_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);
  dst->resize(original_size + kEncodedLength - 8);  // Padding
  PutFixed32(dst, static_cast<uint32_t>(table_magic_number() & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(table_magic_number() >> 32));
  assert(dst->size() == original_size + kEncodedLength);
}

Footer::Footer(uint64_t _table_magic_number)
  : table_magic_number_(_table_magic_number) {}

Status Footer::DecodeFrom(Slice* input) {
  assert(!HasInitializedTableMagicNumber());
  assert(input != nullptr);
  assert(input->size() >= kEncodedLength);

  const char *magic_ptr =
    input->data() + input->size() - kMagicNumberLengthByte;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                    (static_cast<uint64_t>(magic_lo)));

  set_table_magic_number(magic);

  // Footer version 1 and higher will always occupy exactly this many bytes.
  // It consists of the checksum type, two block handles, padding,
  // a version number, and a magic number
  if (input->size() < kEncodedLength) {
    return Status::Corruption("input is too short to be an sstable");
  } else {
    input->remove_prefix(input->size() - kEncodedLength);
  }

  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + kMagicNumberLengthByte;
    *input = Slice(end, input->data() + input->size() - end);
  }
  return result;
}

std::string Footer::ToString() const {
  std::string result, handle_;
  result.reserve(1024);

  result.append("metaindex handle: " + metaindex_handle_.ToString() + "\n  ");
  result.append("index handle: " + index_handle_.ToString() + "\n  ");
  result.append("table_magic_number: " +
                vidardb::ToString(table_magic_number_) + "\n  ");
  return result;
}

Status ReadFooterFromFile(RandomAccessFileReader* file, uint64_t file_size,
                          Footer* footer, uint64_t enforce_table_magic_number) {
  if (file_size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  size_t read_offset =
    (file_size > Footer::kEncodedLength)
    ? static_cast<size_t>(file_size - Footer::kEncodedLength)
    : 0;
  Status s = file->Read(read_offset, Footer::kEncodedLength, &footer_input,
                        footer_space);
  if (!s.ok()) return s;

  // Check that we actually read the whole footer from the file. It may be
  // that size isn't correct.
  if (footer_input.size() < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  s = footer->DecodeFrom(&footer_input);
  if (!s.ok()) {
    return s;
  }
  if (enforce_table_magic_number != 0 &&
      enforce_table_magic_number != footer->table_magic_number()) {
    return Status::Corruption("Bad table magic number");
  }
  return Status::OK();
}

// Without anonymous namespace here, we fail the warning -Wmissing-prototypes
namespace {

// Read a block and check its CRC
// contents is the result of reading.
// According to the implementation of file->Read, contents may not point to buf
Status ReadBlock(RandomAccessFileReader* file, const Footer& footer,
                 const ReadOptions& options, const BlockHandle& handle,
                 Slice* contents, /* result of reading */ char* buf) {
  size_t n = static_cast<size_t>(handle.size());
  Status s;

  {
    PERF_TIMER_GUARD(block_read_time);
    s = file->Read(handle.offset(), n + kBlockTrailerSize, contents, buf);
  }

  PERF_COUNTER_ADD(block_read_count, 1);
  PERF_COUNTER_ADD(block_read_byte, n + kBlockTrailerSize);

  if (!s.ok()) {
    return s;
  }
  if (contents->size() != n + kBlockTrailerSize) {
    return Status::Corruption("truncated block read");
  }

  // Check the crc of the type and the block contents
  const char* data = contents->data();  // Pointer to where Read put the data
  if (options.verify_checksums) {
    PERF_TIMER_GUARD(block_checksum_time);
    uint32_t value = DecodeFixed32(data + n + 1);
    uint32_t actual = 0;

    value = crc32c::Unmask(value);
    actual = crc32c::Value(data, n + 1);

    if (s.ok() && actual != value) {
      s = Status::Corruption("block checksum mismatch");
    }
    if (!s.ok()) {
      return s;
    }
  }
  return s;
}

}  // namespace

Status ReadBlockContents(RandomAccessFileReader* file, const Footer& footer,
                         const ReadOptions& read_options,
                         const BlockHandle& handle, BlockContents* contents,
                         Env* env, bool decompression_requested,
                         const Slice& compression_dict,
                         Logger* info_log) {
  Status status;
  Slice slice;
  size_t n = static_cast<size_t>(handle.size());
  std::unique_ptr<char[]> heap_buf;
  char stack_buf[DefaultStackBufferSize];
  char* used_buf = nullptr;
  vidardb::CompressionType compression_type;

  status = Status::NotFound();

  // cache miss read from device
  if (decompression_requested &&
      n + kBlockTrailerSize < DefaultStackBufferSize) {
    // If we've got a small enough hunk of data, read it in to the
    // trivially allocated stack buffer instead of needing a full malloc()
    used_buf = &stack_buf[0];
  } else {
    heap_buf = std::unique_ptr<char[]>(new char[n + kBlockTrailerSize]);
    used_buf = heap_buf.get();
  }

  status = ReadBlock(file, footer, read_options, handle, &slice, used_buf);

  if (!status.ok()) {
    return status;
  }

  PERF_TIMER_GUARD(block_decompress_time);

  compression_type = static_cast<vidardb::CompressionType>(slice.data()[n]);

  if (decompression_requested && compression_type != kNoCompression) {
    // compressed page, uncompress, update cache
    status = UncompressBlockContents(slice.data(), n, contents,
                                     compression_dict);
  } else if (slice.data() != used_buf) {
    // the slice content is not the buffer provided
    *contents = BlockContents(Slice(slice.data(), n), false, compression_type);
  } else {
    // page is uncompressed, the buffer either stack or heap provided
    if (used_buf == &stack_buf[0]) {
      heap_buf = std::unique_ptr<char[]>(new char[n]);
      memcpy(heap_buf.get(), stack_buf, n);
    }
    *contents = BlockContents(std::move(heap_buf), n, true, compression_type);
  }

  return status;
}

//
// The 'data' points to the raw block contents that was read in from file.
// This method allocates a new heap buffer and the raw block
// contents are uncompresed into this buffer. This
// buffer is returned via 'result' and it is upto the caller to
// free this buffer.
// format_version is the block format as defined in include/vidardb/table.h
Status UncompressBlockContents(const char* data, size_t n,
                               BlockContents* contents,
                               const Slice& compression_dict) {
  std::unique_ptr<char[]> ubuf;
  int decompress_size = 0;
  assert(data[n] != kNoCompression);
  switch (data[n]) {
  case kSnappyCompression: {
    size_t ulength = 0;
    static char snappy_corrupt_msg[] =
      "Snappy not supported or corrupted Snappy compressed block contents";
    if (!Snappy_GetUncompressedLength(data, n, &ulength)) {
      return Status::Corruption(snappy_corrupt_msg);
    }
    ubuf.reset(new char[ulength]);
    if (!Snappy_Uncompress(data, n, ubuf.get())) {
      return Status::Corruption(snappy_corrupt_msg);
    }
    *contents = BlockContents(std::move(ubuf), ulength, true, kNoCompression);
    break;
  }
  case kZlibCompression:
    ubuf.reset(Zlib_Uncompress(
                 data, n, &decompress_size,
                 GetCompressFormatForVersion(kZlibCompression),
                 compression_dict));
    if (!ubuf) {
      static char zlib_corrupt_msg[] =
        "Zlib not supported or corrupted Zlib compressed block contents";
      return Status::Corruption(zlib_corrupt_msg);
    }
    *contents =
      BlockContents(std::move(ubuf), decompress_size, true, kNoCompression);
    break;
  case kBZip2Compression:
    ubuf.reset(BZip2_Uncompress(
                 data, n, &decompress_size,
                 GetCompressFormatForVersion(kBZip2Compression)));
    if (!ubuf) {
      static char bzip2_corrupt_msg[] =
        "Bzip2 not supported or corrupted Bzip2 compressed block contents";
      return Status::Corruption(bzip2_corrupt_msg);
    }
    *contents =
      BlockContents(std::move(ubuf), decompress_size, true, kNoCompression);
    break;
  case kLZ4Compression:
    ubuf.reset(LZ4_Uncompress(
                 data, n, &decompress_size,
                 GetCompressFormatForVersion(kLZ4Compression),
                 compression_dict));
    if (!ubuf) {
      static char lz4_corrupt_msg[] =
        "LZ4 not supported or corrupted LZ4 compressed block contents";
      return Status::Corruption(lz4_corrupt_msg);
    }
    *contents =
      BlockContents(std::move(ubuf), decompress_size, true, kNoCompression);
    break;
  case kLZ4HCCompression:
    ubuf.reset(LZ4_Uncompress(
                 data, n, &decompress_size,
                 GetCompressFormatForVersion(kLZ4HCCompression),
                 compression_dict));
    if (!ubuf) {
      static char lz4hc_corrupt_msg[] =
        "LZ4HC not supported or corrupted LZ4HC compressed block contents";
      return Status::Corruption(lz4hc_corrupt_msg);
    }
    *contents =
      BlockContents(std::move(ubuf), decompress_size, true, kNoCompression);
    break;
  case kXpressCompression:
    ubuf.reset(XPRESS_Uncompress(data, n, &decompress_size));
    if (!ubuf) {
      static char xpress_corrupt_msg[] =
        "XPRESS not supported or corrupted XPRESS compressed block contents";
      return Status::Corruption(xpress_corrupt_msg);
    }
    *contents =
      BlockContents(std::move(ubuf), decompress_size, true, kNoCompression);
    break;
  case kZSTDNotFinalCompression:
    ubuf.reset(ZSTD_Uncompress(data, n, &decompress_size, compression_dict));
    if (!ubuf) {
      static char zstd_corrupt_msg[] =
        "ZSTD not supported or corrupted ZSTD compressed block contents";
      return Status::Corruption(zstd_corrupt_msg);
    }
    *contents =
      BlockContents(std::move(ubuf), decompress_size, true, kNoCompression);
    break;
  default:
    return Status::Corruption("bad block type");
  }

  return Status::OK();
}

}  // namespace vidardb
