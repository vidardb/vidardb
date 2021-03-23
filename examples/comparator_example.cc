// Copyright (c) 2020-present, VidarDB, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <string.h>

#include <iostream>
using namespace std;

#include "vidardb/db.h"
#include "vidardb/status.h"
#include "vidardb/options.h"
#include "vidardb/comparator.h"
#include "vidardb/table.h"
using namespace vidardb;

// #define ROW_STORE
#define COLUMN_STORE

unsigned int M = 3;
string kDBPath = "/tmp/vidardb_comparator_example";

// Customized Comparator provides the reversed lexicographic ordering for keys
class CustomizedComparator : public Comparator {
 public:
  CustomizedComparator() { }

  virtual const char* Name() const override {
    return "CustomizedComparator";
  }

  virtual int Compare(const Slice& a, const Slice& b) const override {
    return -a.compare(b); // reversed lexicographic ordering
  }

  virtual bool Equal(const Slice& a, const Slice& b) const override {
    return a == b;
  }

  virtual void FindShortestSeparator(std::string* start,
                                     const Slice& limit) const override {
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      uint8_t start_byte = static_cast<uint8_t>((*start)[diff_index]);
      uint8_t limit_byte = static_cast<uint8_t>(limit[diff_index]);
      if (start_byte >= limit_byte || (diff_index == start->size() - 1)) {
        // Cannot shorten since limit is smaller than start or start is
        // already the shortest possible.
        return;
      }
      assert(start_byte < limit_byte);

      if (diff_index < limit.size() - 1 || start_byte + 1 < limit_byte) {
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
      } else {
        //     v
        // A A 1 A A A
        // A A 2
        //
        // Incrementing the current byte will make start bigger than limit, we
        // will skip this byte, and find the first non 0xFF byte in start and
        // increment it.
        diff_index++;

        while (diff_index < start->size()) {
          // Keep moving until we find the first non 0xFF byte to
          // increment it
          if (static_cast<uint8_t>((*start)[diff_index]) <
              static_cast<uint8_t>(0xff)) {
            (*start)[diff_index]++;
            start->resize(diff_index + 1);
            break;
          }
          diff_index++;
        }
      }
      assert(Compare(*start, limit) < 0);
    }
  }

  virtual void FindShortSuccessor(std::string* key) const override {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i+1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};

int main(int argc, char* argv[]) {
  // remove existed db path
  int ret = system(string("rm -rf " + kDBPath).c_str());

  // open database
  DB* db;
  Options options;
  options.splitter.reset(NewEncodingSplitter());
  options.OptimizeAdaptiveLevelStyleCompaction();

  // adaptive table factory
  #ifdef ROW_STORE
  const int knob = -1;
  #endif
  #ifdef COLUMN_STORE
  const int knob = 0;
  #endif

  shared_ptr<TableFactory> block_based_table(NewBlockBasedTableFactory());
  shared_ptr<TableFactory> column_table(NewColumnTableFactory());
  ColumnTableOptions* column_opts =
      static_cast<ColumnTableOptions*>(column_table->GetOptions());
  column_opts->column_count = M;
  for (auto i = 0u; i < column_opts->column_count; i++) {
    column_opts->column_comparators.push_back(BytewiseComparator());
  }
  options.table_factory.reset(NewAdaptiveTableFactory(block_based_table,
      block_based_table, column_table, knob));

  // 1. expected key order: 1 2 4 5 6
  // const Comparator* comparator = BytewiseComparator();
  // 2. expected key order: 6 5 4 2 1
  const Comparator* comparator = new CustomizedComparator();
  // specify the customized comparator
  options.comparator = comparator;

  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // insert data
  WriteOptions write_options;
  s = db->Put(write_options, "1",
              options.splitter->Stitch({"chen1", "33", "hangzhou"}));
  assert(s.ok());
  s = db->Put(write_options, "2",
              options.splitter->Stitch({"wang2", "32", "wuhan"}));
  assert(s.ok());
  s = db->Put(write_options, "3",
              options.splitter->Stitch({"zhao3", "35", "nanjing"}));
  assert(s.ok());
  s = db->Put(write_options, "4",
              options.splitter->Stitch({"liao4", "28", "beijing"}));
  assert(s.ok());
  s = db->Put(write_options, "5",
              options.splitter->Stitch({"jiang5", "30", "shanghai"}));
  assert(s.ok());
  s = db->Put(write_options, "6",
              options.splitter->Stitch({"lian6", "30", "changsha"}));
  assert(s.ok());
  s = db->Delete(write_options, "1");
  assert(s.ok());
  s = db->Put(write_options, "3",
              options.splitter->Stitch({"zhao333", "35", "nanjing"}));
  assert(s.ok());
  s = db->Put(write_options, "6",
              options.splitter->Stitch({"lian666", "30", "changsha"}));
  assert(s.ok());
  s = db->Put(write_options, "1",
              options.splitter->Stitch({"chen1111", "33", "hangzhou"}));
  assert(s.ok());
  s = db->Delete(write_options, "3");
  assert(s.ok());

  // test memtable or sstable
  s = db->Flush(FlushOptions());
  assert(s.ok());

  // full scan
  cout << "=> full scan:" << endl;
  ReadOptions ro;
  Iterator* iter = db->NewIterator(ro);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    cout << "key: " << iter->key().ToString() << ", value: { ";
    vector<Slice> vals(options.splitter->Split(iter->value()));
    for (Slice& s: vals) {
      cout << s.ToString() << " ";
    }
    cout << "}" << endl;
  }
  delete iter;

  // range query
  cout << "=> range query:" << endl;
  ro.batch_capacity = 50;  // in batch (byte)
  ro.columns = {1, 3};
  Range range;

  list<RangeQueryKeyVal> res;
  bool next = true;
  while (next) { // range query loop
    next = db->RangeQuery(ro, range, res, &s);
    assert(s.ok());
    for (auto it : res) {
      cout << it.user_key << "=[";
      vector<Slice> vals(options.splitter->Split(it.user_val));
      for (auto i = 0u; i < vals.size(); i++) {
        cout << vals[i].ToString();
        if (i < vals.size() - 1) {
          cout << ", ";
        }
      }
      cout << "] ";
    }
    cout << " key_size=" << ro.result_key_size;
    cout << ", val_size=" << ro.result_val_size << endl;
  }

  if (strcmp(comparator->Name(), "CustomizedComparator") == 0) {
    delete comparator;
  }
  delete db;
  return 0;
}
