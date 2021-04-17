// Copyright (c) 2020-present, VidarDB, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// 1. PREREQUISITE:
//
// The tpch lineitem dataset should be prepared ahead of time.
// You can run the following command in the VidarDB Benchmark repo
// https://github.com/vidardb/Benchmark
//
// make gen-data
//
// 2. USAGE:
//
// DATASET=xxx/lineitem.tbl ./range_query_tpch_test
//

#include <stdlib.h>
#include <fstream>
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

enum kTableType { ROW, COLUMN };
const unsigned int kColumn = 14;  // lineitem
const string kDBPath = "/tmp/vidardb_range_query_tpch_test";
const string delim = "|";
const char* kDataSet = "DATASET";

int IndexOf(const char* str, const char c, const int n) {
  if (n == 0) {
    return -1;
  }

  const char* res = str;
  for (int i = 1; i <= n; ++i) {
    res = strchr(res, c);
    if (!res) {
      return strlen(str);
    } else if (i != n) {
      res++;
    }
  }

  return res - str;
}

string GetNthAttr(const string& str, const int n) {
  int i = IndexOf(str.c_str(), '|', n);
  int j = IndexOf(str.c_str(), '|', n + 1);
  return string(str.substr(i + 1, j - i - 1));
}

void PutFixed32(string* dst, uint32_t value) {
  char buf[sizeof(value)];
  buf[0] = (value >> 24) & 0xff;
  buf[1] = (value >> 16) & 0xff;
  buf[2] = (value >> 8) & 0xff;
  buf[3] = value & 0xff;
  dst->append(buf, sizeof(buf));
}

void EncodeAttr(const string& s, string& k, string& v) {
  string orderKey, lineNumber;
  PutFixed32(&orderKey, stoul(GetNthAttr(s, 0)));
  PutFixed32(&lineNumber, stoul(GetNthAttr(s, 3)));

  string partKey(GetNthAttr(s, 1));
  string suppKey(GetNthAttr(s, 2));
  string quantity(GetNthAttr(s, 4));
  string extendedPrice(GetNthAttr(s, 5));
  string discount(GetNthAttr(s, 6));
  string tax(GetNthAttr(s, 7));
  string returnFlag(GetNthAttr(s, 8));
  string lineStatus(GetNthAttr(s, 9));
  string shipDate(GetNthAttr(s, 10));
  string commitDate(GetNthAttr(s, 11));
  string receiptDate(GetNthAttr(s, 12));
  string shipInstruct(GetNthAttr(s, 13));
  string shipMode(GetNthAttr(s, 14));
  string comment(GetNthAttr(s, 15));

  k.assign(orderKey + delim + lineNumber);
  v.assign(partKey + delim + suppKey + delim + quantity + delim +
           extendedPrice + delim + discount + delim + tax + delim + returnFlag +
           delim + lineStatus + delim + shipDate + delim + commitDate + delim +
           receiptDate + delim + shipInstruct + delim + shipMode + delim +
           comment);
}

void TestTpchRangeQuery(bool flush, kTableType table, vector<uint32_t> cols) {
  cout << "cols: { ";
  for (auto col : cols) {
    cout << col << " ";
  }
  cout << "}" << endl;

  if (!getenv(kDataSet)) {
    cout << "err: missing dataset" << endl;
    return;
  }

  int ret = system(string("rm -rf " + kDBPath).c_str());

  Options options;
  options.create_if_missing = true;
  options.splitter.reset(NewPipeSplitter());  // delim
  options.OptimizeAdaptiveLevelStyleCompaction();

  int knob = -1;  // row
  if (table == COLUMN) {
    knob = 0;
  }

  shared_ptr<TableFactory> block_based_table(NewBlockBasedTableFactory());
  shared_ptr<TableFactory> column_table(NewColumnTableFactory());
  ColumnTableOptions* column_opts =
      static_cast<ColumnTableOptions*>(column_table->GetOptions());
  column_opts->column_count = kColumn;
  for (auto i = 0u; i < column_opts->column_count; i++) {
    column_opts->value_comparators.push_back(BytewiseComparator());
  }
  options.table_factory.reset(NewAdaptiveTableFactory(
      block_based_table, block_based_table, column_table, knob));

  DB* db;
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // init db dataset
  cout << "init dataset ..." << endl;
  ifstream in(string(getenv(kDataSet)));
  for (string line; getline(in, line);) {
    string key, value;
    EncodeAttr(line.substr(0, line.size() - 1), key, value);

    Status s = db->Put(WriteOptions(), key, value);
    if (!s.ok()) {
      cout << s.ToString() << endl;
    }
  }
  in.close();

  if (flush) {  // flush to disk
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
  // TestTpchRangeQuery(false, ROW, {});
  TestTpchRangeQuery(false, COLUMN, {});
  // TestTpchRangeQuery(true, ROW, {});
  // TestTpchRangeQuery(true, COLUMN, {});
  return 0;
}
