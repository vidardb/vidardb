//  Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>
using namespace std;

#include "vidardb/db.h"
#include "vidardb/file_iter.h"
#include "vidardb/options.h"
#include "vidardb/status.h"
using namespace vidardb;

string kDBPath = "/tmp/vidardb_range_query_example";

int main(int argc, char* argv[]) {
  // remove existed db path
  int ret = system(string("rm -rf " + kDBPath).c_str());

  // open database
  DB* db;
  Options options;
  options.splitter.reset(NewPipeSplitter());
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
//  s = db->Delete(write_options, "1");
//  assert(s.ok());
//  s = db->Put(write_options, "3", "data333");
//  assert(s.ok());
//  s = db->Put(write_options, "6", "data666");
//  assert(s.ok());
//  s = db->Put(write_options, "1", "data1111");
//  assert(s.ok());
//  s = db->Delete(write_options, "3");
//  assert(s.ok());

  // test blocked sstable or memtable
  s = db->Flush(FlushOptions());
  assert(s.ok());

  ReadOptions ro;
  ro.columns = {0, 1};

  FileIter* iter = dynamic_cast<FileIter*>(db->NewFileIterator(ro));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    vector<vector<MinMax>> v;
    iter->GetMinMax(v);

    // block_bits is set for illustration purpose here.
    vector<bool> block_bits(1, true);
    vector<RangeQueryKeyVal> res;
    iter->RangeQuery(block_bits, res);
    for (auto& it : res) {
      cout << it.user_key << ": " << it.user_val << endl;
    }
  }
  delete iter;

  delete db;
  return 0;
}
