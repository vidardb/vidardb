//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef VIDARDB_LITE

#include "utilities/write_batch_with_index/write_batch_with_index_internal.h"

#include "db/column_family.h"
#include "util/coding.h"
#include "util/string_util.h"
#include "vidardb/comparator.h"
#include "vidardb/db.h"
#include "vidardb/utilities/write_batch_with_index.h"

namespace vidardb {

class Env;
class Logger;
class Statistics;

Status ReadableWriteBatch::GetEntryFromDataOffset(size_t data_offset,
                                                  WriteType* type, Slice* Key,
                                                  Slice* value, Slice* blob,
                                                  Slice* xid) const {
  if (type == nullptr || Key == nullptr || value == nullptr ||
      blob == nullptr || xid == nullptr) {
    return Status::InvalidArgument("Output parameters cannot be null");
  }

  if (data_offset == GetDataSize()) {
    // reached end of batch.
    return Status::NotFound();
  }

  if (data_offset > GetDataSize()) {
    return Status::InvalidArgument("data offset exceed write batch size");
  }
  Slice input = Slice(rep_.data() + data_offset, rep_.size() - data_offset);
  char tag;
  uint32_t column_family;
  Status s = ReadRecordFromWriteBatch(&input, &tag, &column_family, Key, value,
                                      blob, xid);

  switch (tag) {
    case kTypeColumnFamilyValue:
    case kTypeValue:
      *type = kPutRecord;
      break;
    case kTypeColumnFamilyDeletion:
    case kTypeDeletion:
      *type = kDeleteRecord;
      break;
    case kTypeLogData:
      *type = kLogDataRecord;
      break;
    case kTypeBeginPrepareXID:
    case kTypeEndPrepareXID:
    case kTypeCommitXID:
    case kTypeRollbackXID:
      *type = kXIDRecord;
      break;
    default:
      return Status::Corruption("unknown WriteBatch tag");
  }
  return Status::OK();
}

int WriteBatchEntryComparator::operator()(
    const WriteBatchIndexEntry* entry1,
    const WriteBatchIndexEntry* entry2) const {
  if (entry1->column_family > entry2->column_family) {
    return 1;
  } else if (entry1->column_family < entry2->column_family) {
    return -1;
  }

  if (entry1->offset == WriteBatchIndexEntry::kFlagMin) {
    return -1;
  } else if (entry2->offset == WriteBatchIndexEntry::kFlagMin) {
    return 1;
  }

  Slice key1, key2;
  if (entry1->search_key == nullptr) {
    key1 = Slice(write_batch_->Data().data() + entry1->key_offset,
                 entry1->key_size);
  } else {
    key1 = *(entry1->search_key);
  }
  if (entry2->search_key == nullptr) {
    key2 = Slice(write_batch_->Data().data() + entry2->key_offset,
                 entry2->key_size);
  } else {
    key2 = *(entry2->search_key);
  }

  int cmp = CompareKey(entry1->column_family, key1, key2);
  if (cmp != 0) {
    return cmp;
  } else if (entry1->offset > entry2->offset) {
    return 1;
  } else if (entry1->offset < entry2->offset) {
    return -1;
  }
  return 0;
}

int WriteBatchEntryComparator::CompareKey(uint32_t column_family,
                                          const Slice& key1,
                                          const Slice& key2) const {
  if (column_family < cf_comparators_.size() &&
      cf_comparators_[column_family] != nullptr) {
    return cf_comparators_[column_family]->Compare(key1, key2);
  } else {
    return default_comparator_->Compare(key1, key2);
  }
}

WriteBatchWithIndexInternal::Result WriteBatchWithIndexInternal::GetFromBatch(
    const DBOptions& options, WriteBatchWithIndex* batch,
    ColumnFamilyHandle* column_family, const Slice& key,
    WriteBatchEntryComparator* cmp, std::string* value, bool overwrite_key,
    Status* s) {
  uint32_t cf_id = GetColumnFamilyID(column_family);
  *s = Status::OK();
  WriteBatchWithIndexInternal::Result result =
      WriteBatchWithIndexInternal::Result::kNotFound;

  std::unique_ptr<WBWIIterator> iter(batch->NewIterator(column_family));

  // We want to iterate in the reverse order that the writes were added to the
  // batch. Since we don't have a reverse iterator, we must seek past the end.
  iter->Seek(key);
  while (iter->Valid()) {
    const WriteEntry entry = iter->Entry();
    if (cmp->CompareKey(cf_id, entry.key, key) != 0) {
      break;
    }

    iter->Next();
  }

  if (!(*s).ok()) {
    return WriteBatchWithIndexInternal::Result::kError;
  }

  if (!iter->Valid()) {
    // Read past end of results.  Reposition on last result.
    iter->SeekToLast();
  } else {
    iter->Prev();
  }

  Slice entry_value;
  while (iter->Valid()) {
    const WriteEntry entry = iter->Entry();
    if (cmp->CompareKey(cf_id, entry.key, key) != 0) {
      // Unexpected error or we've reached a different next key
      break;
    }

    switch (entry.type) {
      case kPutRecord: {
        result = WriteBatchWithIndexInternal::Result::kFound;
        entry_value = entry.value;
        break;
      }
      case kDeleteRecord: {
        result = WriteBatchWithIndexInternal::Result::kDeleted;
        break;
      }
      case kLogDataRecord:
      case kXIDRecord: {
        // ignore
        break;
      }
      default: {
        result = WriteBatchWithIndexInternal::Result::kError;
        (*s) = Status::Corruption("Unexpected entry in WriteBatchWithIndex:",
                                  ToString(entry.type));
        break;
      }
    }
    if (result == WriteBatchWithIndexInternal::Result::kFound ||
        result == WriteBatchWithIndexInternal::Result::kDeleted ||
        result == WriteBatchWithIndexInternal::Result::kError) {
      // We can stop iterating once we find a PUT or DELETE
      break;
    }

    iter->Prev();
  }

  if ((*s).ok() && result == WriteBatchWithIndexInternal::Result::kFound) {
    value->assign(entry_value.data(), entry_value.size());  // PUT
  }

  return result;
}

}  // namespace vidardb

#endif  // !VIDARDB_LITE
