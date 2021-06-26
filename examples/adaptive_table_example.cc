//  Copyright (c) 2020-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>
using namespace std;

#include "vidardb/comparator.h"
#include "vidardb/db.h"
#include "vidardb/file_iter.h"
#include "vidardb/options.h"
#include "vidardb/splitter.h"
#include "vidardb/status.h"
#include "vidardb/table.h"
using namespace vidardb;

// #define ROW_STORE
#define COLUMN_STORE

unsigned int M = 3;
string kDBPath = "/tmp/vidardb_adaptive_table_example";

int main(int argc, char* argv[]) {
  // remove existed db path
  int ret = system(string("rm -rf " + kDBPath).c_str());

  // open database
  DB* db;
  Options options;
  options.splitter.reset(NewEncodingSplitter());
  options.OptimizeAdaptiveLevelStyleCompaction();

  // adaptive table factory
  #ifdef ROW_STORE
  const int knob = -1;
  #endif
  #ifdef COLUMN_STORE
  const int knob = 0;
  #endif

  shared_ptr<TableFactory> block_based_table(NewBlockBasedTableFactory());
  shared_ptr<TableFactory> column_table(NewColumnTableFactory());
  ColumnTableOptions* column_opts =
      static_cast<ColumnTableOptions*>(column_table->GetOptions());
  column_opts->column_count = M;
  for (auto i = 0u; i < column_opts->column_count; i++) {
    column_opts->value_comparators.push_back(BytewiseComparator());
  }
  options.table_factory.reset(NewAdaptiveTableFactory(block_based_table,
      block_based_table, column_table, knob));

  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // insert data
  WriteOptions write_options;
  // write_options.sync = true;
  s = db->Put(write_options, "1",
              options.splitter->Stitch({"chen1", "33", "hangzhou"}));
  assert(s.ok());
  s = db->Put(write_options, "2",
              options.splitter->Stitch({"wang2", "32", "wuhan"}));
  assert(s.ok());
  s = db->Put(write_options, "3",
              options.splitter->Stitch({"zhao3", "35", "nanjing"}));
  assert(s.ok());
  s = db->Put(write_options, "4",
              options.splitter->Stitch({"liao4", "28", "beijing"}));
  assert(s.ok());
  s = db->Put(write_options, "5",
              options.splitter->Stitch({"jiang5", "30", "shanghai"}));
  assert(s.ok());
  s = db->Put(write_options, "6",
              options.splitter->Stitch({"lian6", "30", "changsha"}));
  assert(s.ok());
//  s = db->Delete(write_options, "1");
//  assert(s.ok());
//  s = db->Put(write_options, "3",
//              options.splitter->Stitch({"zhao333", "35", "nanjing"}));
//  assert(s.ok());
//  s = db->Put(write_options, "6",
//              options.splitter->Stitch({"lian666", "30", "changsha"}));
//  assert(s.ok());
//  s = db->Put(write_options, "1",
//              options.splitter->Stitch({"chen1111", "33", "hangzhou"}));
//  assert(s.ok());
//  s = db->Delete(write_options, "3");
//  assert(s.ok());

  // test memtable or sstable
  s = db->Flush(FlushOptions());
  assert(s.ok());

  ReadOptions ro;
  ro.columns = {1, 2};

  FileIter* iter = dynamic_cast<FileIter*>(db->NewFileIterator(ro));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    vector<vector<MinMax>> v;
    s = iter->GetMinMax(v);
    assert(s.ok() || s.IsNotFound());
    if (!s.ok()) continue;

    // block_bits is set for illustration purpose here.
    vector<bool> block_bits(1, true);
    int N = 1024 * 1024;
    char* buf = new char[N];
    uint64_t count;
    s = iter->RangeQuery(block_bits, buf, N, &count);
    assert(s.ok());

    uint64_t* end = reinterpret_cast<uint64_t*>(buf + N);
    for (auto c : ro.columns) {
      for (int i = 0; i < count; ++i) {
        uint64_t offset = *(--end), size = *(--end);
        cout << Slice(buf + offset, size).ToString() << " ";
      }
      cout << endl;
    }
    delete[] buf;
  }
  delete iter;

  delete db;
  return 0;
}
