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

#include "vidardb/db.h"
#include "vidardb/options.h"
#include "vidardb/splitter.h"
#include "vidardb/status.h"
#include "vidardb/table.h"

using namespace vidardb;

enum kTableType { ROW, COLUMN };
const unsigned int kColumn = 14;  // lineitem
const std::string kDBPath = "/tmp/vidardb_range_query_tpch_test";
const std::string delim = "|";
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

std::string GetNthAttr(const std::string& str, const int n) {
  int i = IndexOf(str.c_str(), '|', n);
  int j = IndexOf(str.c_str(), '|', n + 1);
  return std::string(str.substr(i + 1, j - i - 1));
}

void PutFixed32(std::string* dst, uint32_t value) {
  char buf[sizeof(value)];
  buf[0] = (value >> 24) & 0xff;
  buf[1] = (value >> 16) & 0xff;
  buf[2] = (value >> 8) & 0xff;
  buf[3] = value & 0xff;
  dst->append(buf, sizeof(buf));
}

void EncodeAttr(const std::string& s, std::string& k, std::string& v) {
  std::string orderKey, lineNumber;
  PutFixed32(&orderKey, std::stoul(GetNthAttr(s, 0)));
  PutFixed32(&lineNumber, std::stoul(GetNthAttr(s, 3)));

  std::string partKey(GetNthAttr(s, 1));
  std::string suppKey(GetNthAttr(s, 2));
  std::string quantity(GetNthAttr(s, 4));
  std::string extendedPrice(GetNthAttr(s, 5));
  std::string discount(GetNthAttr(s, 6));
  std::string tax(GetNthAttr(s, 7));
  std::string returnFlag(GetNthAttr(s, 8));
  std::string lineStatus(GetNthAttr(s, 9));
  std::string shipDate(GetNthAttr(s, 10));
  std::string commitDate(GetNthAttr(s, 11));
  std::string receiptDate(GetNthAttr(s, 12));
  std::string shipInstruct(GetNthAttr(s, 13));
  std::string shipMode(GetNthAttr(s, 14));
  std::string comment(GetNthAttr(s, 15));

  k.assign(orderKey + delim + lineNumber);
  v.assign(partKey + delim + suppKey + delim + quantity + delim +
           extendedPrice + delim + discount + delim + tax + delim + returnFlag +
           delim + lineStatus + delim + shipDate + delim + commitDate + delim +
           receiptDate + delim + shipInstruct + delim + shipMode + delim +
           comment);
}

void TestTpchRangeQuery(bool flush, kTableType table, size_t capacity,
                        std::vector<uint32_t> cols) {
  std::cout << ">> capacity: " << capacity << ", cols: { ";
  for (auto& col : cols) {
    std::cout << col << " ";
  }
  std::cout << "}" << std::endl;

  if (!getenv(kDataSet)) {
    std::cout << "err: missing dataset" << std::endl;
    return;
  }

  int ret = system(std::string("rm -rf " + kDBPath).c_str());

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
  options.table_factory.reset(NewAdaptiveTableFactory(
      block_based_table, block_based_table, column_table, knob));

  DB* db;
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // init db dataset
  std::cout << "init dataset ..." << std::endl;
  std::ifstream in(std::string(getenv(kDataSet)));
  for (std::string line; getline(in, line);) {
    std::string key, value;
    EncodeAttr(line.substr(0, line.size() - 1), key, value);

    Status s = db->Put(WriteOptions(), key, value);
    if (!s.ok()) {
      std::cout << s.ToString() << std::endl;
    }
  }
  in.close();

  if (flush) {  // flush to disk
    s = db->Flush(FlushOptions());
    assert(s.ok());
  }

  ReadOptions ro;
  ro.batch_capacity = capacity;
  ro.columns = cols;

  // full scan
  Range range(kRangeQueryMin, kRangeQueryMax);
  std::cout << "range query ..." << std::endl;
  std::list<RangeQueryKeyVal> res;
  bool next = true;
  for (auto batch_index = 0u; next; batch_index++) {
    std::cout << "index=" << batch_index;

    next = db->RangeQuery(ro, range, res, &s);
    if (!s.ok()) {
      std::cout << s.ToString() << std::endl;
    }
    assert(s.ok());

    for (auto it : res) {
      // std::cout << it.user_key << "=[";

      std::vector<Slice> vals(options.splitter->Split(it.user_val));
      if (cols.size() == 1 && cols[0] == 0) {
        assert(vals.size() == 0);
      } else if (!cols.empty()) {
        assert(vals.size() == cols.size());
      }

      // for (auto i = 0u; i < vals.size(); i++) {
      //   std::cout << vals[i].ToString();
      //   if (i < vals.size() - 1) {
      //     std::cout << ", ";
      //   };
      // }
      // std::cout << "] ";
    }

    size_t result_total_size = ro.result_key_size + ro.result_val_size;
    std::cout << ", result_key_size=" << ro.result_key_size;
    std::cout << ", result_val_size=" << ro.result_val_size;
    std::cout << ", result_total_size=" << result_total_size;
    std::cout << ", capacity=" << capacity << std::endl;
    if (capacity > 0) {
      assert(result_total_size <= capacity);
    }
  }

  delete db;
  std::cout << std::endl;
}

int main() {
  // TestTpchRangeQuery(false, ROW, 4096 * 20, {});
  TestTpchRangeQuery(false, COLUMN, 4096 * 20, {});
  // TestTpchRangeQuery(true, ROW, 4096 * 20, {});
  // TestTpchRangeQuery(true, COLUMN, 4096 * 20, {});
  return 0;
}
