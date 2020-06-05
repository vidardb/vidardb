// Copyright (c) 2020-present, VidarDB, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>

#include "vidardb/db.h"
#include "vidardb/options.h"
#include "vidardb/splitter.h"
#include "vidardb/status.h"
#include "vidardb/table.h"

using namespace std;
using namespace vidardb;

enum kTableType { ROW, COLUMN };
const unsigned int kColumn = 3;
const string kDBPath = "/tmp/vidardb_adaptive_table_factory_test";

void TestAdaptiveTableFactory(bool flush, kTableType table, size_t capacity,
                              vector<uint32_t> cols) {
  cout << ">> capacity: " << capacity << ", cols: { ";
  for (auto& col : cols) {
    cout << col << " ";
  }
  cout << "}" << endl;

  int ret = system(string("rm -rf " + kDBPath).c_str());

  Options options;
  options.create_if_missing = true;
  options.splitter.reset(NewEncodingSplitter());
  #ifndef VIDARDB_LITE
  options.OptimizeAdaptiveLevelStyleCompaction();
  #endif
  int knob = -1;  // row
  if (table == COLUMN) {
    knob = 0;
  }

  shared_ptr<TableFactory> block_based_table(NewBlockBasedTableFactory());
  shared_ptr<TableFactory> column_table(NewColumnTableFactory());
  ColumnTableOptions* column_opts =
      static_cast<ColumnTableOptions*>(column_table->GetOptions());
  column_opts->column_count = kColumn;
  options.table_factory.reset(NewAdaptiveTableFactory(
      block_based_table, block_based_table, column_table, knob));

  DB* db;
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  WriteOptions wo;
  s = db->Put(wo, "1", options.splitter->Stitch({"chen1", "33", "hangzhou"}));
  assert(s.ok());
  s = db->Put(wo, "2", options.splitter->Stitch({"wang2", "32", "wuhan"}));
  assert(s.ok());
  s = db->Put(wo, "3", options.splitter->Stitch({"zhao3", "35", "nanjing"}));
  assert(s.ok());
  s = db->Put(wo, "4", options.splitter->Stitch({"liao4", "28", "beijing"}));
  assert(s.ok());
  s = db->Put(wo, "5", options.splitter->Stitch({"jiang5", "30", "shanghai"}));
  assert(s.ok());
  s = db->Put(wo, "6", options.splitter->Stitch({"lian6", "30", "changsha"}));
  assert(s.ok());
  s = db->Delete(wo, "1");
  assert(s.ok());
  s = db->Put(wo, "3", options.splitter->Stitch({"zhao333", "35", "nanjing"}));
  assert(s.ok());
  s = db->Put(wo, "6", options.splitter->Stitch({"lian666", "30", "changsha"}));
  assert(s.ok());
  s = db->Put(wo, "1",
              options.splitter->Stitch({"chen1111", "33", "hangzhou"}));
  assert(s.ok());
  s = db->Delete(wo, "3");
  assert(s.ok());

  if (flush) {  // flush to disk
    s = db->Flush(FlushOptions());
    assert(s.ok());
  }

  ReadOptions ro;
  ro.batch_capacity = capacity;
  ro.columns = cols;

  Range range("1", kRangeQueryMax);

  list<RangeQueryKeyVal> res;
  bool next = true;
  while (next) {
    next = db->RangeQuery(ro, range, res, &s);
    assert(s.ok());
    for (auto it : res) {
      cout << it.user_key << "=[";

      vector<Slice> vals(options.splitter->Split(it.user_val));
      if (cols.size() == 1 && cols[0] == 0) {
        assert(vals.size() == 0);
      } else {
        assert(vals.size() == cols.size());
      }

      for (auto i = 0u; i < vals.size(); i++) {
        cout << vals[i].ToString();
        if (i < vals.size() - 1) {
          cout << ", ";
        };
      }
      cout << "] ";
    }
    cout << " key_size=" << ro.result_key_size;
    cout << ", val_size=" << ro.result_val_size << endl;

    if (capacity > 0) {
      assert(ro.result_key_size + ro.result_val_size <= capacity);
    }
  }

  delete db;
  cout << endl;
}

int main() {
  TestAdaptiveTableFactory(false, ROW, 0, {1, 3});
  TestAdaptiveTableFactory(false, ROW, 20, {1, 3});
  TestAdaptiveTableFactory(false, ROW, 40, {1, 3});
  TestAdaptiveTableFactory(false, ROW, 100, {1, 3});
  TestAdaptiveTableFactory(false, ROW, 10, {0});
  TestAdaptiveTableFactory(true, ROW, 0, {1, 3});
  TestAdaptiveTableFactory(true, ROW, 20, {1, 3});
  TestAdaptiveTableFactory(true, ROW, 40, {1, 3});
  TestAdaptiveTableFactory(true, ROW, 100, {1, 3});
  TestAdaptiveTableFactory(true, ROW, 10, {0});

  TestAdaptiveTableFactory(false, COLUMN, 0, {1, 3});
  TestAdaptiveTableFactory(false, COLUMN, 20, {1, 3});
  TestAdaptiveTableFactory(false, COLUMN, 40, {1, 3});
  TestAdaptiveTableFactory(false, COLUMN, 100, {1, 3});
  TestAdaptiveTableFactory(false, COLUMN, 10, {0});
  TestAdaptiveTableFactory(true, COLUMN, 0, {1, 3});
  TestAdaptiveTableFactory(true, COLUMN, 20, {1, 3});
  TestAdaptiveTableFactory(true, COLUMN, 40, {1, 3});
  TestAdaptiveTableFactory(true, COLUMN, 100, {1, 3});
  TestAdaptiveTableFactory(true, COLUMN, 10, {0});

  return 0;
}
