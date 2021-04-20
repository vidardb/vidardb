// Copyright (c) 2020-present, VidarDB, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <cstdio>
#include <iostream>
#include <string>
using namespace std;

#include "vidardb/db.h"
#include "vidardb/options.h"
#include "vidardb/slice.h"
#include "vidardb/splitter.h"
using namespace vidardb;

const string kDBPath = "/tmp/vidardb_simple_row_test";

void TestSimpleRowStore(bool flush) {
  int ret = system(string("rm -rf " + kDBPath).c_str());

  Options options;
  #ifndef VIDARDB_LITE
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  #endif
  options.create_if_missing = true;
  options.splitter.reset(NewPipeSplitter());

  DB* db;
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  s = db->Put(WriteOptions(), "key1",
              options.splitter->Stitch({"val11", "val12"}));
  assert(s.ok());

  if (flush) {  // flush to disk
    s = db->Flush(FlushOptions());
    assert(s.ok());
  }

  ReadOptions ro;
  ro.columns = {1};

  string value;
  s = db->Get(ro, "key1", &value);
  assert(s.ok());
  cout << "key1: " << value << endl;
  assert(value == "val11");

  WriteBatch batch;
  batch.Delete("key1");
  batch.Put("key2", options.splitter->Stitch({"val21", "val22"}));
  s = db->Write(WriteOptions(), &batch);
  assert(s.ok());

  if (flush) {  // flush to disk
    s = db->Flush(FlushOptions());
    assert(s.ok());
  }

  s = db->Get(ro, "key1", &value);
  assert(s.IsNotFound());

  db->Get(ro, "key2", &value);
  cout << "key2: " << value << endl;
  assert(value == "val21");

  Iterator* iter = db->NewIterator(ro);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    string key = iter->key().ToString();
    string val = iter->value().ToString();
    cout << "key: " << key << ", "
         << "val: " << val << endl;
    assert(key == "key2");
    assert(val == "val21");
  }

  delete iter;
  delete db;
  cout << endl;
}

int main() {
  TestSimpleRowStore(false);
  TestSimpleRowStore(true);
  return 0;
}
