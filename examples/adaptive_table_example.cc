//  Copyright (c) 2020-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>
using namespace std;

#include "vidardb/db.h"
#include "vidardb/status.h"
#include "vidardb/options.h"
#include "vidardb/splitter.h"
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
  options.create_if_missing = true;
  options.OptimizeAdaptiveLevelStyleCompaction();

  const Splitter* splitter = NewEncodingSplitter();
  options.splitter = splitter;

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
  options.table_factory.reset(NewAdaptiveTableFactory(block_based_table,
      block_based_table, column_table, knob));

  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // insert data
  WriteOptions write_options;
  // write_options.sync = true;
  s = db->Put(write_options, "1",
              splitter->Stitch(vector<Slice>{"chen1", "33", "hangzhou"}));
  assert(s.ok());
  s = db->Put(write_options, "2",
              splitter->Stitch(vector<Slice>{"wang2", "32", "wuhan"}));
  assert(s.ok());
  s = db->Put(write_options, "3",
              splitter->Stitch(vector<Slice>{"zhao3", "35", "nanjing"}));
  assert(s.ok());
  s = db->Put(write_options, "4",
              splitter->Stitch(vector<Slice>{"liao4", "28", "beijing"}));
  assert(s.ok());
  s = db->Put(write_options, "5",
              splitter->Stitch(vector<Slice>{"jiang5", "30", "shanghai"}));
  assert(s.ok());
  s = db->Put(write_options, "6",
              splitter->Stitch(vector<Slice>{"lian6", "30", "changsha"}));
  assert(s.ok());
  s = db->Delete(write_options, "1");
  assert(s.ok());
  s = db->Put(write_options, "3",
              splitter->Stitch(vector<Slice>{"zhao333", "35", "nanjing"}));
  assert(s.ok());
  s = db->Put(write_options, "6",
              splitter->Stitch(vector<Slice>{"lian666", "30", "changsha"}));
  assert(s.ok());
  s = db->Put(write_options, "1",
              splitter->Stitch(vector<Slice>{"chen1111", "33", "hangzhou"}));
  assert(s.ok());
  s = db->Delete(write_options, "3");
  assert(s.ok());

  // test memtable or sstable
  s = db->Flush(FlushOptions());
  assert(s.ok());

  ReadOptions read_options;
  // read_options.batch_capacity = 0; // full search
  read_options.batch_capacity = 2; // in batch
  read_options.columns = {1, 3};

  // Range range; // full search
  // Range range("2", "5"); // [2, 5]
  Range range("1", "6"); // [1, 6]
  // Range range("1", kRangeQueryMax); // [1, max]

  list<RangeQueryKeyVal> res;
  bool next = true;
  while (next) { // range query loop
    next = db->RangeQuery(read_options, range, res, &s);
    assert(s.ok());
    for (auto it : res) {
      cout << it.user_key << "=[";
      vector<Slice> vals = splitter->Split(it.user_val);
      for (auto i = 0u; i < vals.size(); i++) {
        cout << vals[i].ToString();
        if (i < vals.size() - 1) {
          cout << ", ";
        };
      }
      cout << "] ";
    }
    cout << endl;
  }

  delete db, splitter;
  return 0;
}
