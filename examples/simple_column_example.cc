//  Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
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
#include "vidardb/table.h"
using namespace vidardb;

unsigned int M = 3;
string kDBPath = "/tmp/vidardb_simple_column_example";

int main() {
  int ret = system(string("rm -rf " + kDBPath).c_str());

  DB* db;
  Options options;
  options.splitter.reset(NewPipeSplitter());

  TableFactory* table_factory = NewColumnTableFactory();
  ColumnTableOptions* opts =
      static_cast<ColumnTableOptions*>(table_factory->GetOptions());
  opts->column_count = M;
  for (auto i = 0u; i < opts->column_count; i++) {
    opts->value_comparators.push_back(BytewiseComparator());
  }
  options.table_factory.reset(table_factory);

  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  s = db->Put(WriteOptions(), "column1",
              options.splitter->Stitch({"val11", "val12", "val13"}));
  s = db->Put(WriteOptions(), "column2",
              options.splitter->Stitch({"val21", "val22", "val23"}));
  assert(s.ok());

  // test memtable or sstable
  s = db->Flush(FlushOptions());
  assert(s.ok());

  ReadOptions ro;
  ro.columns = {1, 3};
//  ro.columns = {0};

  cout << "Range Query: ";
  FileIter* file_iter = dynamic_cast<FileIter*>(db->NewFileIterator(ro));
  for (file_iter->SeekToFirst(); file_iter->Valid(); file_iter->Next()) {
    FileIter::FileType type;
    vector<vector<MinMax>> v;
    s = file_iter->GetMinMax(type, v);
    assert(s.ok());

    // block_bits is set for illustration purpose here.
    std::vector<bool> block_bits(1, true);
    vector<RangeQueryKeyVal> res;
    s = file_iter->RangeQuery(block_bits, res);
    assert(s.ok());
    for (auto& it : res) {
      cout << it.user_key << ": " << it.user_val << " ";
    }
  }
  delete file_iter;
  cout << endl;

  string val;
  s = db->Get(ro, "column2", &val);
  if (!s.ok()) cout << "Get not ok!" << endl;
  cout << "Get column2: " << val << endl;

  Iterator* it = db->NewIterator(ro);
  it->Seek("column1");
  if (it->Valid()) {
    cout << "column1 value: " << it->value().ToString() << endl;
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
