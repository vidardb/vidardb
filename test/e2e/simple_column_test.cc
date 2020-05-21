// Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>

#include "vidardb/db.h"
#include "vidardb/options.h"
#include "vidardb/splitter.h"
#include "vidardb/table.h"

using namespace vidardb;

const unsigned int kColumn = 3;  // value columns
const std::string kDBPath = "/tmp/vidardb_simple_column_test";

void TestSimpleColumnStore(bool flush) {
  int ret = system(std::string("rm -rf " + kDBPath).c_str());

  Options options;
  options.create_if_missing = true;
  options.splitter.reset(NewPipeSplitter());

  TableFactory* table_factory = NewColumnTableFactory();
  ColumnTableOptions* opts =
      static_cast<ColumnTableOptions*>(table_factory->GetOptions());
  opts->column_count = kColumn;
  options.table_factory.reset(table_factory);

  DB* db;
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  s = db->Put(WriteOptions(), "key1",
              options.splitter->Stitch({"val11", "val12", "val13"}));
  assert(s.ok());
  s = db->Put(WriteOptions(), "key2",
              options.splitter->Stitch({"val21", "val22", "val23"}));
  assert(s.ok());

  if (flush) {  // flush to disk
    s = db->Flush(FlushOptions());
    assert(s.ok());
  }

  ReadOptions ro;
  ro.columns = {1, 3};

  std::list<RangeQueryKeyVal> res;
  bool next = true;
  while (next) {
    next = db->RangeQuery(ro, Range(), res, &s);
    assert(s.ok());

    for (auto it = res.begin(); it != res.end(); it++) {
      std::cout << "key: " << it->user_key << ", "
                << "val: " << it->user_val << std::endl;
      assert(it->user_key == "key1" || it->user_key == "key2");
      assert(it->user_val == "val11|val13" || it->user_val == "val21|val23");
    }
  }

  std::string value;
  s = db->Get(ro, "key2", &value);
  assert(s.ok());
  std::cout << "key2: " << value << std::endl;
  assert(value == "val21|val23");

  Iterator* it = db->NewIterator(ro);
  it->Seek("key1");
  assert(it->Valid());
  value = it->value().ToString();
  std::cout << "key1: " << value << std::endl;
  assert(value == "val11|val13");

  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::string key = it->key().ToString();
    std::string val = it->value().ToString();
    std::cout << "key: " << key << ", "
              << "val: " << val << std::endl;
    assert(key == "key1" || key == "key2");
    assert(val == "val11|val13" || val == "val21|val23");

    s = db->Delete(WriteOptions(), key);
    assert(s.ok());
  }

  delete it;
  delete db;
  std::cout << std::endl;
}

int main() {
  TestSimpleColumnStore(false);
  TestSimpleColumnStore(true);
  return 0;
}
