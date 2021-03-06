//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <stdio.h>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "memtable/memtable.h"
#include "db/write_batch_internal.h"
#include "vidardb/db.h"
#include "vidardb/env.h"
#include "vidardb/iterator.h"
#include "vidardb/table.h"
#include "table/block.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace vidardb {

static std::string RandomString(Random* rnd, int len) {
  std::string r;
  test::RandomString(rnd, len, &r);
  return r;
}
std::string GenerateKey(int primary_key, int secondary_key, int padding_size,
                        Random *rnd) {
  char buf[50];
  char *p = &buf[0];
  snprintf(buf, sizeof(buf), "%6d%4d", primary_key, secondary_key);
  std::string k(p);
  if (padding_size) {
    k += RandomString(rnd, padding_size);
  }

  return k;
}

// Generate random key value pairs.
// The generated key will be sorted. You can tune the parameters to generated
// different kinds of test key/value pairs for different scenario.
void GenerateRandomKVs(std::vector<std::string> *keys,
                       std::vector<std::string> *values, const int from,
                       const int len, const int step = 1,
                       const int padding_size = 0,
                       const int keys_share_prefix = 1) {
  Random rnd(302);

  // generate different prefix
  for (int i = from; i < from + len; i += step) {
    // generating keys that shares the prefix
    for (int j = 0; j < keys_share_prefix; ++j) {
      keys->emplace_back(GenerateKey(i, j, padding_size, &rnd));

      // 100 bytes values
      values->emplace_back(RandomString(&rnd, 100));
    }
  }
}

class BlockTest : public testing::Test {};

// block test
TEST_F(BlockTest, SimpleTest) {
  Random rnd(301);
  Options options = Options();
  std::unique_ptr<InternalKeyComparator> ic;
  ic.reset(new test::PlainInternalKeyComparator(options.comparator));

  std::vector<std::string> keys;
  std::vector<std::string> values;
  BlockBuilder builder(16);
  int num_records = 100000;

  GenerateRandomKVs(&keys, &values, 0, num_records);
  // add a bunch of records to a block
  for (int i = 0; i < num_records; i++) {
    builder.Add(keys[i], values[i]);
  }

  // read serialized contents of the block
  Slice rawblock = builder.Finish();

  // create block reader
  BlockContents contents;
  contents.data = rawblock;
  contents.cachable = false;
  Block reader(std::move(contents));

  // read contents of block sequentially
  int count = 0;
  InternalIterator *iter = reader.NewIterator(options.comparator);
  for (iter->SeekToFirst();iter->Valid(); count++, iter->Next()) {

    // read kv from block
    Slice k = iter->key();
    Slice v = iter->value();

    // compare with lookaside array
    ASSERT_EQ(k.ToString().compare(keys[count]), 0);
    ASSERT_EQ(v.ToString().compare(values[count]), 0);
  }
  delete iter;

  // read block contents randomly
  iter = reader.NewIterator(options.comparator);
  for (int i = 0; i < num_records; i++) {

    // find a random key in the lookaside array
    int index = rnd.Uniform(num_records);
    Slice k(keys[index]);

    // search in block for this key
    iter->Seek(k);
    ASSERT_TRUE(iter->Valid());
    Slice v = iter->value();
    ASSERT_EQ(v.ToString().compare(values[index]), 0);
  }
  delete iter;
}

// return the block contents
BlockContents GetBlockContents(std::unique_ptr<BlockBuilder> *builder,
                               const std::vector<std::string> &keys,
                               const std::vector<std::string> &values,
                               const int prefix_group_size = 1) {
  builder->reset(new BlockBuilder(1 /* restart interval */));

  // Add only half of the keys
  for (size_t i = 0; i < keys.size(); ++i) {
    (*builder)->Add(keys[i], values[i]);
  }
  Slice rawblock = (*builder)->Finish();

  BlockContents contents;
  contents.data = rawblock;
  contents.cachable = false;

  return contents;
}

void CheckBlockContents(BlockContents contents, const int max_key,
                        const std::vector<std::string> &keys,
                        const std::vector<std::string> &values) {
  // create block reader
  BlockContents contents_ref(contents.data, contents.cachable,
                             contents.compression_type);
  Block reader1(std::move(contents));
  Block reader2(std::move(contents_ref));

  std::unique_ptr<InternalIterator> regular_iter(
      reader2.NewIterator(BytewiseComparator()));

  // Seek existent keys
  for (size_t i = 0; i < keys.size(); i++) {
    regular_iter->Seek(keys[i]);
    ASSERT_OK(regular_iter->status());
    ASSERT_TRUE(regular_iter->Valid());

    Slice v = regular_iter->value();
    ASSERT_EQ(v.ToString().compare(values[i]), 0);
  }

  // Seek non-existent keys.
  // For hash index, if no key with a given prefix is not found, iterator will
  // simply be set as invalid; whereas the binary search based iterator will
  // return the one that is closest.
  for (int i = 1; i < max_key - 1; i += 2) {
    auto key = GenerateKey(i, 0, 0, nullptr);
    regular_iter->Seek(key);
    ASSERT_TRUE(regular_iter->Valid());
  }
}

// In this test case, no two key share same prefix.
TEST_F(BlockTest, SimpleIndexHash) {
  const int kMaxKey = 100000;
  std::vector<std::string> keys;
  std::vector<std::string> values;
  GenerateRandomKVs(&keys, &values, 0 /* first key id */,
                    kMaxKey /* last key id */, 2 /* step */,
                    8 /* padding size (8 bytes randomly generated suffix) */);

  std::unique_ptr<BlockBuilder> builder;
  auto contents = GetBlockContents(&builder, keys, values);

  CheckBlockContents(std::move(contents), kMaxKey, keys, values);
}

TEST_F(BlockTest, IndexHashWithSharedPrefix) {
  const int kMaxKey = 100000;
  // for each prefix, there will be 5 keys starts with it.
  const int kPrefixGroup = 5;
  std::vector<std::string> keys;
  std::vector<std::string> values;
  // Generate keys with same prefix.
  GenerateRandomKVs(&keys, &values, 0,  // first key id
                    kMaxKey,            // last key id
                    2,                  // step
                    10,                 // padding size,
                    kPrefixGroup);

  std::unique_ptr<BlockBuilder> builder;
  auto contents = GetBlockContents(&builder, keys, values, kPrefixGroup);

  CheckBlockContents(std::move(contents), kMaxKey, keys, values);
}

}  // namespace vidardb

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
