// Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
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

const unsigned int kColumn = 3;
const string kDBPath = "/tmp/vidardb_range_query_column_test";

void TestColumnRangeQuery(bool flush, size_t capacity, vector<uint32_t> cols) {
  int ret = system(string("rm -rf " + kDBPath).c_str());

  Options options;
  options.create_if_missing = true;
  options.splitter.reset(NewEncodingSplitter());

  TableFactory* table_factory = NewColumnTableFactory();
  ColumnTableOptions* opts =
      static_cast<ColumnTableOptions*>(table_factory->GetOptions());
  opts->column_count = kColumn;
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

  if (flush) {
    s = db->Flush(FlushOptions());
    assert(s.ok());
  }

  ReadOptions ro;
  ro.batch_capacity = capacity;
  ro.columns = cols;

  Range range("1", "6");

  list<RangeQueryKeyVal> res;
  bool next = true;
  while (next) {
    size_t total_key_size = 0, total_val_size = 0;
    next = db->RangeQuery(ro, range, res, &s);
    assert(s.ok());

    cout << "{ ";
    for (auto it : res) {
      total_key_size += it.user_key.size();
      total_val_size += it.user_val.size();
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
    cout << "} key_size=" << ro.result_key_size;
    cout << ", val_size=" << ro.result_val_size << endl;

    assert(total_key_size == ro.result_key_size);
    assert(total_val_size == ro.result_val_size);
    if (capacity > 0) {
      assert(res.size() <= capacity);
    }
  }

  delete db;
}

int main() {
  TestColumnRangeQuery(false, 0, {1, 3});
  TestColumnRangeQuery(false, 1, {1, 3});
  TestColumnRangeQuery(false, 2, {1, 3});
  TestColumnRangeQuery(false, 5, {1, 3});
  TestColumnRangeQuery(false, 2, {0});

  TestColumnRangeQuery(true, 0, {1, 3});
  TestColumnRangeQuery(true, 1, {1, 3});
  TestColumnRangeQuery(true, 2, {1, 3});
  TestColumnRangeQuery(true, 5, {1, 3});
  TestColumnRangeQuery(true, 2, {0});
  return 0;
}
