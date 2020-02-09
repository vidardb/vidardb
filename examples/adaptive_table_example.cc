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
string kDBPath = "/tmp/adaptive_table_example";

int main(int argc, char* argv[]) {
  // remove existed db path
  system("rm -rf /tmp/adaptive_table_example");

  // open database
  DB* db; // db ref
  Options options;
  options.OptimizeAdaptiveLevelStyleCompaction();
  options.create_if_missing = true;

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
  column_opts->column_num = M;
  options.table_factory.reset(NewAdaptiveTableFactory(block_based_table,
    block_based_table, column_table, knob));

  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // insert data
  #ifdef COLUMN_STORE
  WriteOptions write_options;
  // write_options.sync = true;
  Splitter *splitter = column_opts->splitter.get();
  s = db->Put(write_options, "1", 
      splitter->Stitch(vector<string>{"chen1", "33", "hangzhou"}));
  assert(s.ok());
  s = db->Put(write_options, "2", 
      splitter->Stitch(vector<string>{"wang2", "32", "wuhan"}));
  assert(s.ok());
  s = db->Put(write_options, "3", 
      splitter->Stitch(vector<string>{"zhao3", "35", "nanjing"}));
  assert(s.ok());
  s = db->Put(write_options, "4", 
      splitter->Stitch(vector<string>{"liao4", "28", "beijing"}));
  assert(s.ok());
  s = db->Put(write_options, "5", 
      splitter->Stitch(vector<string>{"jiang5", "30", "shanghai"}));
  assert(s.ok());
  s = db->Put(write_options, "6", 
      splitter->Stitch(vector<string>{"lian6", "30", "changsha"}));
  assert(s.ok());
  s = db->Delete(write_options, "1");
  assert(s.ok());
  s = db->Put(write_options, "3", 
      splitter->Stitch(vector<string>{"zhao333", "35", "nanjing"}));
  assert(s.ok());
  s = db->Put(write_options, "6", 
      splitter->Stitch(vector<string>{"lian666", "30", "changsha"}));
  assert(s.ok());
  s = db->Put(write_options, "1", 
      splitter->Stitch(vector<string>{"chen1111", "33", "hangzhou"}));
  assert(s.ok());
  s = db->Delete(write_options, "3");
  assert(s.ok());
  #endif
  #ifdef ROW_STORE
  WriteOptions write_options;
  // write_options.sync = true;
  s = db->Put(write_options, "1", "data1");
  assert(s.ok());
  s = db->Put(write_options, "2", "data2");
  assert(s.ok());
  s = db->Put(write_options, "3", "data3");
  assert(s.ok());
  s = db->Put(write_options, "4", "data4");
  assert(s.ok());
  s = db->Put(write_options, "5", "data5");
  assert(s.ok());
  s = db->Put(write_options, "6", "data6");
  assert(s.ok());
  s = db->Delete(write_options, "1");
  assert(s.ok());
  s = db->Put(write_options, "3", "data333");
  assert(s.ok());
  s = db->Put(write_options, "6", "data666");
  assert(s.ok());
  s = db->Put(write_options, "1", "data1111");
  assert(s.ok());
  s = db->Delete(write_options, "3");
  assert(s.ok());
  #endif

  // test memtable or sstable
  s = db->Flush(FlushOptions());
  assert(s.ok());

  ReadOptions read_options;
  // read_options.batch_capacity = 0; // full search
  read_options.batch_capacity = 2; // in batch

//  Range range; // full search
  // Range range("2", "5"); // [2, 5]
  Range range("1", "6"); // [1, 6]
//  Range range("1", kRangeQueryMax); // [1, max]

  #ifdef COLUMN_STORE
  list<RangeQueryKeyVal> res;
  bool next = true;
  while (next) { // range query loop
    next = db->RangeQuery(read_options, range, res, &s);
    assert(s.ok());
    for (auto it : res) {
      cout << it.user_key << "=[";
      vector<string> vals(splitter->Split(it.user_val));
      for (auto i = 0u; i < vals.size(); i++) {
        cout << vals[i];
        if (i < vals.size() - 1) {
          cout << ", ";
        };
      }
      cout << "] ";
    }
    cout << endl;
  }
  #endif
  #ifdef ROW_STORE
  list<RangeQueryKeyVal> res;
  bool next = true;
  while (next) { // range query loop
    next = db->RangeQuery(read_options, range, res, &s);
    assert(s.ok());
    for (auto it : res) {
      cout << it.user_key << "=" << it.user_val << " ";
    }
    cout << endl;
  }
  #endif

  delete db;
  return 0;
}
