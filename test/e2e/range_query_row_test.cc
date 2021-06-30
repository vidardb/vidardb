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
    s = iter->GetMinMax(v);
    assert(s.ok() || s.IsNotFound());
    if (s.IsNotFound()) continue;

    // block_bits is set for illustration purpose here.
    vector<bool> block_bits(1, true);
    int N = 1024 * 1024;
    char* buf = new char[N];
    uint64_t valid_count, total_count;
    s = iter->RangeQuery(block_bits, buf, N, &valid_count, &total_count);
    assert(s.ok());

    char* limit = buf + N;
    uint64_t* end = reinterpret_cast<uint64_t*>(limit);
    // for (auto c : ro.columns) {  cannot use this since columns is empty
    for (int c = 0; c < 2; ++c) {
      for (int i = 0; i < valid_count; ++i) {
        uint64_t offset = *(--end), size = *(--end);
        cout << Slice(buf + offset, size).ToString() << " ";
      }
      cout << endl;
      limit -= total_count * 2 * sizeof(uint64_t);
      end = reinterpret_cast<uint64_t*>(limit);
    }
    delete[] buf;
  }
  delete iter;

  delete db;
  cout << endl;
}

int main() {
  TestRowRangeQuery(false);
  TestRowRangeQuery(true);

  return 0;
}
