//  Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>
using namespace std;

#include "vidardb/db.h"
#include "vidardb/status.h"
#include "vidardb/options.h"
#include "vidardb/splitter.h"
#include "../table/adaptive_table_factory.h"
using namespace vidardb;

unsigned int M = 3;
string kDBPath = "/tmp/range_query_column_example";

int main(int argc, char* argv[]) {
  // remove existed db path
  system("rm -rf /tmp/range_query_column_example");

  // open database
  DB* db; // db ref
  Options options;
  options.create_if_missing = true;

  // column table
  // const Splitter* splitter = PipeSplitter();
  const Splitter* splitter = EncodingSplitter();
  TableFactory* table_factory = NewColumnTableFactory();
  static_cast<ColumnTableOptions*>(table_factory->GetOptions())->column_num = M;
  static_cast<ColumnTableOptions*>(table_factory->GetOptions())->splitter = splitter;
  options.table_factory.reset(table_factory);

  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // insert data
  WriteOptions write_options;
  // write_options.sync = true;
  s = db->Put(write_options, "1", 
    splitter->Join(std::vector<std::string>{"chen1", "33", "hangzhou"}));
  assert(s.ok());
  s = db->Put(write_options, "2", 
    splitter->Join(std::vector<std::string>{"wang2", "32", "wuhan"}));
  assert(s.ok());
  s = db->Put(write_options, "3", 
    splitter->Join(std::vector<std::string>{"zhao3", "35", "nanjing"}));
  assert(s.ok());
  s = db->Put(write_options, "4", 
    splitter->Join(std::vector<std::string>{"liao4", "28", "beijing"}));
  assert(s.ok());
  s = db->Put(write_options, "5", 
    splitter->Join(std::vector<std::string>{"jiang5", "30", "shanghai"}));
  assert(s.ok());
  s = db->Put(write_options, "6", 
    splitter->Join(std::vector<std::string>{"lian6", "30", "changsha"}));
  assert(s.ok());
  s = db->Delete(write_options, "1");
  assert(s.ok());
  s = db->Put(write_options, "3", 
    splitter->Join(std::vector<std::string>{"zhao333", "35", "nanjing"}));
  assert(s.ok());
  s = db->Put(write_options, "6", 
    splitter->Join(std::vector<std::string>{"lian666", "30", "changsha"}));
  assert(s.ok());
  s = db->Put(write_options, "1", 
    splitter->Join(std::vector<std::string>{"chen1111", "33", "hangzhou"}));
  assert(s.ok());
  s = db->Delete(write_options, "3");
  assert(s.ok());

  // test column sstable or memtable
  s = db->Flush(FlushOptions());
  assert(s.ok());

  ReadOptions read_options;
  // read_options.batch_capacity = 0; // full search // ok
  read_options.batch_capacity = 2; // in batch // ok

//  Range range; // full search // ok
  // Range range("2", "5"); // [2, 5] // ok
  Range range("1", "6"); // [1, 6] // ok
//  Range range("1", kRangeQueryMax); // [1, max] // ok

  list<RangeQueryKeyVal> res;
  bool next = true;
  while (next) { // range query loop
    next = db->RangeQuery(read_options, range, res, &s);
    assert(s.ok());
    for (auto it : res) {
      cout << it.user_key << "=[";
      std::vector<std::string> vals = splitter->Split(it.user_val);
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

  delete db;
  return 0;
}
