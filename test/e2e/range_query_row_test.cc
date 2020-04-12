// Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>

#include "vidardb/db.h"
#include "vidardb/options.h"
#include "vidardb/status.h"

using namespace std;
using namespace vidardb;

const string kDBPath = "/tmp/vidardb_range_query_row_test";

void TestRowRangeQuery(bool flush, size_t capacity) {
  int ret = system(string("rm -rf " + kDBPath).c_str());

  Options options;
  options.create_if_missing = true;

  DB* db;
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  WriteOptions wo;
  s = db->Put(wo, "1", "data1");
  assert(s.ok());
  s = db->Put(wo, "2", "data2");
  assert(s.ok());
  s = db->Put(wo, "3", "data3");
  assert(s.ok());
  s = db->Put(wo, "4", "data4");
  assert(s.ok());
  s = db->Put(wo, "5", "data5");
  assert(s.ok());
  s = db->Put(wo, "6", "data6");
  assert(s.ok());
  s = db->Delete(wo, "1");
  assert(s.ok());
  s = db->Put(wo, "3", "data333");
  assert(s.ok());
  s = db->Put(wo, "6", "data666");
  assert(s.ok());
  s = db->Put(wo, "1", "data1111");
  assert(s.ok());
  s = db->Delete(wo, "3");
  assert(s.ok());

  if (flush) {  // flush to disk
    s = db->Flush(FlushOptions());
    assert(s.ok());
  }

  ReadOptions ro;
  ro.batch_capacity = capacity;

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
      cout << it.user_key << "=" << it.user_val << " ";
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
  TestRowRangeQuery(false, 0);
  TestRowRangeQuery(false, 1);
  TestRowRangeQuery(false, 2);
  TestRowRangeQuery(false, 5);

  TestRowRangeQuery(true, 0);
  TestRowRangeQuery(true, 1);
  TestRowRangeQuery(true, 2);
  TestRowRangeQuery(true, 5);
  return 0;
}
