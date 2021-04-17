// Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

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

const unsigned int kColumn = 3;
const string kDBPath = "/tmp/vidardb_range_query_column_test";

void TestColumnRangeQuery(bool flush, vector<uint32_t> cols) {
  cout << "cols: { ";
  for (auto col : cols) {
    cout << col << " ";
  }
  cout << "}" << endl;

  int ret = system(string("rm -rf " + kDBPath).c_str());

  Options options;
  options.create_if_missing = true;
  options.splitter.reset(NewEncodingSplitter());

  TableFactory* table_factory = NewColumnTableFactory();
  ColumnTableOptions* opts =
      static_cast<ColumnTableOptions*>(table_factory->GetOptions());
  opts->column_count = kColumn;
  for (auto i = 0u; i < opts->column_count; i++) {
    opts->value_comparators.push_back(BytewiseComparator());
  }
  options.table_factory.reset(table_factory);

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
//  s = db->Delete(wo, "1");
//  assert(s.ok());
//  s = db->Put(wo, "3", options.splitter->Stitch({"zhao333", "35", "nanjing"}));
//  assert(s.ok());
//  s = db->Put(wo, "6", options.splitter->Stitch({"lian666", "30", "changsha"}));
//  assert(s.ok());
//  s = db->Put(wo, "1",
//              options.splitter->Stitch({"chen1111", "33", "hangzhou"}));
//  assert(s.ok());
//  s = db->Delete(wo, "3");
//  assert(s.ok());

  if (flush) {
    s = db->Flush(FlushOptions());
    assert(s.ok());
  }

  ReadOptions ro;
  ro.columns = cols;

  FileIter* iter = dynamic_cast<FileIter*>(db->NewFileIterator(ro));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    FileIter::FileType type;
    vector<vector<MinMax>> v;
    s = iter->GetMinMax(type, v);
    assert(s.ok());

    // block_bits is set for illustration purpose here.
    vector<bool> block_bits(1, true);
    vector<RangeQueryKeyVal> res;
    s = iter->RangeQuery(block_bits, res);
    assert(s.ok());

    cout << "{ ";
    for (auto& it : res) {
      cout << it.user_key << "=[";
      vector<Slice> vals(options.splitter->Split(it.user_val));
      for (auto i = 0u; i < vals.size(); i++) {
        cout << vals[i].ToString();
        if (i < vals.size() - 1) {
          cout << ", ";
        };
      }
      cout << "] ";
    }
    cout << "} " << endl;
  }
  delete iter;

  delete db;
  cout << endl;
}

int main() {
  TestColumnRangeQuery(false, {1, 3});
  TestColumnRangeQuery(false, {0});

  TestColumnRangeQuery(true, {1, 3});
  TestColumnRangeQuery(true, {0});
  return 0;
}
