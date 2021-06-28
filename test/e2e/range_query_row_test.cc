// Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>
using namespace std;

#include "vidardb/db.h"
#include "vidardb/file_iter.h"
#include "vidardb/options.h"
#include "vidardb/status.h"
using namespace vidardb;

const string kDBPath = "/tmp/vidardb_range_query_row_test";

void TestRowRangeQuery(bool flush) {
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
//  s = db->Delete(wo, "1");
//  assert(s.ok());
//  s = db->Put(wo, "3", "data333");
//  assert(s.ok());
//  s = db->Put(wo, "6", "data666");
//  assert(s.ok());
//  s = db->Put(wo, "1", "data1111");
//  assert(s.ok());
//  s = db->Delete(wo, "3");
//  assert(s.ok());

  if (flush) {  // flush to disk
    s = db->Flush(FlushOptions());
    assert(s.ok());
  }

  ReadOptions ro;
  FileIter* iter = dynamic_cast<FileIter*>(db->NewFileIterator(ro));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    vector<vector<MinMax>> v;
    iter->GetMinMax(v);

    // block_bits is set for illustration purpose here.
    vector<bool> block_bits(1, true);
    //    vector<RangeQueryKeyVal> res;
    //    iter->RangeQuery(block_bits, res);
    //    for (auto& it : res) {
    //      cout << it.user_key << ": " << it.user_val << endl;
    //    }
  }
  delete iter;

  delete db;
  cout << endl;
}

int main() {
  //  TestRowRangeQuery(false);
  //  TestRowRangeQuery(true);

  return 0;
}
