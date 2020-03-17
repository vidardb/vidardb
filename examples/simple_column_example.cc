//  Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>
using namespace std;

#include "vidardb/db.h"
#include "vidardb/options.h"
#include "vidardb/splitter.h"
#include "vidardb/table.h"
using namespace vidardb;

unsigned int M = 2;
string kDBPath = "/tmp/vidardb_simple_example" + to_string(M);

int main() {
  system(string("rm -rf " + kDBPath).c_str());

  DB* db;
  Options options;
  options.create_if_missing = true;

  TableFactory* table_factory = NewColumnTableFactory();
  ColumnTableOptions* opts =
      static_cast<ColumnTableOptions*>(table_factory->GetOptions());
  opts->column_count = M;
  opts->splitter.reset(new PipeSplitter());
  options.table_factory.reset(table_factory);

  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  s = db->Put(WriteOptions(), "column1", "val1|val2");
  s = db->Put(WriteOptions(), "column2", "val3|val4");
  db->Flush(FlushOptions());
  if (!s.ok()) cout << "Put not ok!" << endl;

  ReadOptions ro;
  ro.columns = {1};

  list<RangeQueryKeyVal> resRQ;
  bool next = true;
  while (next) {  // range query loop
    next = db->RangeQuery(ro, Range(), resRQ, &s);
    assert(s.ok());
    for (auto it = resRQ.begin(); it != resRQ.end(); it++) {
      cout << it->user_val << " ";
    }
    cout << endl;
  }

  string val;
  s = db->Get(ro, "column2", &val);
  if (!s.ok()) cout << "Get not ok!" << endl;
  cout << "Get: " << val << endl;

  Iterator* it = db->NewIterator(ro);
  it->Seek("column1");
  if (it->Valid()) {
    cout << "value: " << it->value().ToString() << endl;
  }
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    cout << "key: " << it->key().ToString()
         << " value: " << it->value().ToString() << endl;
    s = db->Delete(WriteOptions(), it->key());
    if (!s.ok()) cout << "Delete not ok!" << endl;
  }
  delete it;

  delete db;

  cout << "finished!" << endl;
  return 0;
}
