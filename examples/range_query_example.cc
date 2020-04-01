//  Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>
using namespace std;

#include "vidardb/db.h"
#include "vidardb/status.h"
#include "vidardb/options.h"
using namespace vidardb;

string kDBPath = "/tmp/vidardb_range_query_example";

int main(int argc, char* argv[]) {
  // remove existed db path
  system(string("rm -rf " + kDBPath).c_str());

  // open database
  DB* db;
  Options options;
  options.create_if_missing = true;

  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // insert data
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

  // test blocked sstable or memtable
  s = db->Flush(FlushOptions());
  assert(s.ok());

  ReadOptions read_options;
  // read_options.batch_capacity = 0; // full search
  read_options.batch_capacity = 2; // in batch

  // Range range; // full search
  // Range range("2", "4"); // [2, 4]
  Range range("1", "6"); // [1, 6]
  // Range range("1", kRangeQueryMax); // [1, max]

  list<RangeQueryKeyVal> res;
  bool next = true;
  while (next) { // range query loop
    size_t total_key_size = 0, total_val_size = 0;
    next = db->RangeQuery(read_options, range, res, &s);
    assert(s.ok());
    cout<< "{ ";
    for (auto it : res) {
      total_key_size += it.user_key.size();
      total_val_size += it.user_val.size();
      cout << it.user_key << "=" << it.user_val << " ";
    }
    cout << "} key_size=" << read_options.result_key_size;
    cout << ", val_size=" << read_options.result_val_size << endl;
    assert(total_key_size == read_options.result_key_size);
    assert(total_val_size == read_options.result_val_size);
  }

  delete db;
  return 0;
}
