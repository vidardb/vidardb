//  Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/adaptive_table_factory.h"

#include "table/table_builder.h"
#include "table/format.h"
#include "port/port.h"
#include "util/instrumented_mutex.h"  // Shichao

namespace vidardb {

AdaptiveTableFactory::~AdaptiveTableFactory() {}  // Shichao

AdaptiveTableFactory::AdaptiveTableFactory(
    std::shared_ptr<TableFactory> table_factory_to_write,
    std::shared_ptr<TableFactory> block_based_table_factory,
    std::shared_ptr<TableFactory> column_table_factory,  // Shichao
    int knob)  // Shichao
    : table_factory_to_write_(table_factory_to_write),
      block_based_table_factory_(block_based_table_factory),
      column_table_factory_(column_table_factory) {  // Shichao
  if (!table_factory_to_write_) {
    table_factory_to_write_ = block_based_table_factory_;
  }
  if (!block_based_table_factory_) {
    block_based_table_factory_.reset(NewBlockBasedTableFactory());
  }
  /************************ Shichao ***************************/
  if (!column_table_factory_) {
    column_table_factory_.reset(NewColumnTableFactory());
  }

  knob_ = knob;
  mutex_.reset(new InstrumentedMutex());
  /************************ Shichao ***************************/
}

extern const uint64_t kBlockBasedTableMagicNumber;
extern const uint64_t kColumnTableMagicNumber;  // Shichao

Status AdaptiveTableFactory::NewTableReader(
    const TableReaderOptions& table_reader_options,
    unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    unique_ptr<TableReader>* table) const {
  Footer footer;
  auto s = ReadFooterFromFile(file.get(), file_size, &footer);
  if (!s.ok()) {
    return s;
  }
  if (footer.table_magic_number() == kBlockBasedTableMagicNumber) {
    return block_based_table_factory_->NewTableReader(
        table_reader_options, std::move(file), file_size, table);
  /***************************** Shichao *****************************/
  } else if (footer.table_magic_number() == kColumnTableMagicNumber) {
    return column_table_factory_->NewTableReader(
        table_reader_options, std::move(file), file_size, table);
  /***************************** Shichao *****************************/
  } else {
    return Status::NotSupported("Unidentified table format");
  }
}

TableBuilder* AdaptiveTableFactory::NewTableBuilder(
    const TableBuilderOptions& table_builder_options, uint32_t column_family_id,
    WritableFileWriter* file) const {
  if (knob_ == -1) {
    return table_factory_to_write_->NewTableBuilder(table_builder_options,
                                                    column_family_id, file);
  }
  /******************************** Shichao ********************************/
  mutex_->Lock();
  auto it = output_levels_.find(file->writable_file()->GetFileName());
  int output_level = it==output_levels_.end()? 0: it->second;
  mutex_->Unlock();

  if (output_level < knob_) {
    return block_based_table_factory_->NewTableBuilder(table_builder_options,
                                                       column_family_id, file);
  } else {
    return column_table_factory_->NewTableBuilder(table_builder_options,
                                                  column_family_id, file);
  }
  /******************************** Shichao ********************************/
}

std::string AdaptiveTableFactory::GetPrintableTableOptions() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];

  if (table_factory_to_write_) {
    snprintf(buffer, kBufferSize, "  write factory (%s) options:\n%s\n",
             (table_factory_to_write_->Name() ? table_factory_to_write_->Name()
                                              : ""),
             table_factory_to_write_->GetPrintableTableOptions().c_str());
    ret.append(buffer);
  }
  if (block_based_table_factory_) {
    snprintf(
        buffer, kBufferSize, "  %s options:\n%s\n",
        (block_based_table_factory_->Name() ? block_based_table_factory_->Name()
                                            : ""),
        block_based_table_factory_->GetPrintableTableOptions().c_str());
    ret.append(buffer);
  }
  /***************************** Shichao *****************************/
  if (column_table_factory_) {
    snprintf(buffer, kBufferSize, "  %s options:\n%s\n",
             column_table_factory_->Name() ? column_table_factory_->Name() : "",
             column_table_factory_->GetPrintableTableOptions().c_str());
    ret.append(buffer);
  }
  /***************************** Shichao *****************************/
  return ret;
}

/***************************** Shichao *****************************/
void AdaptiveTableFactory::SetWriteTableFactory(
    std::shared_ptr<TableFactory> table_factory_to_write) {
    table_factory_to_write_ = table_factory_to_write;
}

void AdaptiveTableFactory::SetOutputLevel(
    const std::string& file_name, int output_level) {
  mutex_->Lock();
  output_levels_[file_name] = output_level;
  mutex_->Unlock();
}
/***************************** Shichao *****************************/

extern TableFactory* NewAdaptiveTableFactory(
    std::shared_ptr<TableFactory> table_factory_to_write,
    std::shared_ptr<TableFactory> block_based_table_factory,
    std::shared_ptr<TableFactory> column_table_factory,  // Shichao
    int knob) {                                          // Shichao
  return new AdaptiveTableFactory(table_factory_to_write,
      block_based_table_factory, column_table_factory, knob);  // Shichao
}

}  // namespace vidardb
