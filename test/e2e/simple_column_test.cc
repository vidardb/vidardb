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
#include "vidardb/table.h"

using namespace vidardb;

const unsigned int kColumn = 3;  // value columns
const string kDBPath = "/tmp/vidardb_simple_column_test";

void TestSimpleColumnStore(bool flush) {
  int ret = system(string("rm -rf " + kDBPath).c_str());

  Options options;
  options.create_if_missing = true;
  options.splitter.reset(NewPipeSplitter());

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

  FileIter* file_iter = dynamic_cast<FileIter*>(db->NewFileIterator(ro));
  for (file_iter->SeekToFirst(); file_iter->Valid(); file_iter->Next()) {
    vector<vector<MinMax>> v;
    s = file_iter->GetMinMax(v);
    assert(s.ok() || s.IsNotFound());
    if (s.IsNotFound()) continue;

    // block_bits is set for illustration purpose here.
    vector<bool> block_bits(1, true);
    int N = 1024 * 1024;
    char* buf = new char[N];
    uint64_t count;
    s = file_iter->RangeQuery(block_bits, buf, N, &count);
    assert(s.ok());

    uint64_t* end = reinterpret_cast<uint64_t*>(buf + N);
    for (auto c : ro.columns) {
      for (int i = 0; i < count; ++i) {
        uint64_t offset = *(--end), size = *(--end);
        cout << Slice(buf + offset, size).ToString() << " ";
      }
      cout << endl;
    }
    delete[] buf;
  }
  delete file_iter;

  string value;
  s = db->Get(ro, "key2", &value);
  assert(s.ok());
  cout << "key2: " << value << endl;
  assert(value == "val21|val23");

  Iterator* it = db->NewIterator(ro);
  it->Seek("key1");
  assert(it->Valid());
  value = it->value().ToString();
  cout << "key1: " << value << endl;
  assert(value == "val11|val13");

  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    string key = it->key().ToString();
    string val = it->value().ToString();
    cout << "key: " << key << ", "
         << "val: " << val << endl;
    assert(key == "key1" || key == "key2");
    assert(val == "val11|val13" || val == "val21|val23");

    s = db->Delete(WriteOptions(), key);
    assert(s.ok());
  }

  delete it;
  delete db;
  cout << endl;
}

int main() {
  //  TestSimpleColumnStore(false);
  TestSimpleColumnStore(true);
  return 0;
}
