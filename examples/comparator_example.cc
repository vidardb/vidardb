// Copyright (c) 2020-present, VidarDB, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>
using namespace std;

#include "vidardb/db.h"
#include "vidardb/status.h"
#include "vidardb/options.h"
#include "vidardb/comparator.h"
using namespace vidardb;

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
  options.create_if_missing = true;

  // 1. expected key order: 1 2 4 5 6
  //const Comparator* comparator = BytewiseComparator();
  // 2. expected key order: 6 5 4 2 1
  const Comparator* comparator = new CustomizedComparator();
  // specify the customized comparator
  options.comparator = comparator;

  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // insert data
  WriteOptions write_options;
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

  ReadOptions ro;
  Iterator* iter = db->NewIterator(ro);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    cout << "key: " << iter->key().ToString()
         << " value: " << iter->value().ToString() << endl;
    ;
  }

  if (comparator->Name() == "CustomizedComparator") {
    delete comparator;
  }
  delete iter, db;
  return 0;
}
