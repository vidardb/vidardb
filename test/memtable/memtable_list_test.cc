//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <algorithm>
#include <string>
#include <vector>
#include "memtable/memtable_list.h"
#include "db/version_set.h"
#include "db/write_controller.h"
#include "db/writebuffer.h"
#include "vidardb/db.h"
#include "vidardb/status.h"
#include "util/testutil.h"
#include "util/string_util.h"
#include "util/testharness.h"

namespace vidardb {

class MemTableListTest : public testing::Test {
 public:
  std::string dbname;
  DB* db;
  Options options;

  MemTableListTest() : db(nullptr) {
    dbname = test::TmpDir() + "/memtable_list_test";
  }

  // Create a test db if not yet created
  void CreateDB() {
    if (db == nullptr) {
      options.create_if_missing = true;
      DestroyDB(dbname, options);
      Status s = DB::Open(options, dbname, &db);
      EXPECT_OK(s);
    }
  }

  ~MemTableListTest() {
    if (db) {
      delete db;
      DestroyDB(dbname, options);
    }
  }

  // Calls MemTableList::InstallMemtableFlushResults() and sets up all
  // structures needed to call this function.
  Status Mock_InstallMemtableFlushResults(
      MemTableList* list, const MutableCFOptions& mutable_cf_options,
      const std::vector<MemTable*>& m, std::vector<MemTable*>* to_delete) {
    // Create a mock Logger
    test::NullLogger logger;
    LogBuffer log_buffer(DEBUG_LEVEL, &logger);

    // Create a mock VersionSet
    DBOptions db_options;
    EnvOptions env_options;
    shared_ptr<Cache> table_cache(NewLRUCache(50000, 16));
    WriteBuffer write_buffer(db_options.db_write_buffer_size);
    WriteController write_controller(10000000u);

    CreateDB();
    VersionSet versions(dbname, &db_options, env_options, table_cache.get(),
                        &write_buffer, &write_controller);

    // Create mock default ColumnFamilyData
    ColumnFamilyOptions cf_options;
    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.emplace_back(kDefaultColumnFamilyName, cf_options);
    EXPECT_OK(versions.Recover(column_families, false));

    auto column_family_set = versions.GetColumnFamilySet();
    auto cfd = column_family_set->GetColumnFamily(0);
    EXPECT_TRUE(cfd != nullptr);

    // Create dummy mutex.
    InstrumentedMutex mutex;
    InstrumentedMutexLock l(&mutex);

    return list->InstallMemtableFlushResults(cfd, mutable_cf_options, m,
                                             &versions, &mutex, 1, to_delete,
                                             nullptr, &log_buffer);
  }
};

TEST_F(MemTableListTest, Empty) {
  // Create an empty MemTableList and validate basic functions.
  MemTableList list(1, 0);

  ASSERT_EQ(0, list.NumNotFlushed());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));
  ASSERT_FALSE(list.IsFlushPending());

  std::vector<MemTable*> mems;
  list.PickMemtablesToFlush(&mems);
  ASSERT_EQ(0, mems.size());

  std::vector<MemTable*> to_delete;
  list.current()->Unref(&to_delete);
  ASSERT_EQ(0, to_delete.size());
}

TEST_F(MemTableListTest, GetTest) {
  // Create MemTableList
  int min_write_buffer_number_to_merge = 2;
  int max_write_buffer_number_to_maintain = 0;
  MemTableList list(min_write_buffer_number_to_merge,
                    max_write_buffer_number_to_maintain);

  SequenceNumber seq = 1;
  std::string value;
  Status s;
  std::vector<MemTable*> to_delete;

  LookupKey lkey("key1", seq);
  ReadOptions ro;
  bool found = list.current()->Get(ro, lkey, &value, &s);
  ASSERT_FALSE(found);

  // Create a MemTable
  InternalKeyComparator cmp(BytewiseComparator());
  auto factory = std::make_shared<SkipListFactory>();
  options.memtable_factory = factory;
  ImmutableCFOptions ioptions(options);

  WriteBuffer wb(options.db_write_buffer_size);
  MemTable* mem =
      new MemTable(cmp, ioptions, MutableCFOptions(options, ioptions), &wb,
                   kMaxSequenceNumber);
  mem->Ref();

  // Write some keys to this memtable.
  mem->Add(++seq, kTypeDeletion, "key1", "");
  mem->Add(++seq, kTypeValue, "key2", "value2");
  mem->Add(++seq, kTypeValue, "key1", "value1");
  mem->Add(++seq, kTypeValue, "key2", "value2.2");

  // Fetch the newly written keys
  found = mem->Get(ro, LookupKey("key1", seq), &value, &s);
  ASSERT_TRUE(s.ok() && found);
  ASSERT_EQ(value, "value1");

  found = mem->Get(ro, LookupKey("key1", 2), &value, &s);
  // MemTable found out that this key is *not* found (at this sequence#)
  ASSERT_TRUE(found && s.IsNotFound());

  found = mem->Get(ro, LookupKey("key2", seq), &value, &s);
  ASSERT_TRUE(s.ok() && found);
  ASSERT_EQ(value, "value2.2");

  ASSERT_EQ(4, mem->num_entries());
  ASSERT_EQ(1, mem->num_deletes());

  // Add memtable to list
  list.Add(mem, &to_delete);

  SequenceNumber saved_seq = seq;

  // Create another memtable and write some keys to it
  WriteBuffer wb2(options.db_write_buffer_size);
  MemTable* mem2 =
      new MemTable(cmp, ioptions, MutableCFOptions(options, ioptions), &wb2,
                   kMaxSequenceNumber);
  mem2->Ref();

  mem2->Add(++seq, kTypeDeletion, "key1", "");
  mem2->Add(++seq, kTypeValue, "key2", "value2.3");

  // Add second memtable to list
  list.Add(mem2, &to_delete);

  // Fetch keys via MemTableList
  found = list.current()->Get(ro, LookupKey("key1", seq), &value, &s);
  ASSERT_TRUE(found && s.IsNotFound());

  found = list.current()->Get(ro, LookupKey("key1", saved_seq), &value, &s);
  ASSERT_TRUE(s.ok() && found);
  ASSERT_EQ("value1", value);

  found = list.current()->Get(ro, LookupKey("key2", seq), &value, &s);
  ASSERT_TRUE(s.ok() && found);
  ASSERT_EQ(value, "value2.3");

  found = list.current()->Get(ro, LookupKey("key2", 1), &value, &s);
  ASSERT_FALSE(found);

  ASSERT_EQ(2, list.NumNotFlushed());

  list.current()->Unref(&to_delete);
  for (MemTable* m : to_delete) {
    delete m;
  }
}

TEST_F(MemTableListTest, GetFromHistoryTest) {
  // Create MemTableList
  int min_write_buffer_number_to_merge = 2;
  int max_write_buffer_number_to_maintain = 2;
  MemTableList list(min_write_buffer_number_to_merge,
                    max_write_buffer_number_to_maintain);

  SequenceNumber seq = 1;
  std::string value;
  Status s;
  std::vector<MemTable*> to_delete;

  LookupKey lkey("key1", seq);
  ReadOptions ro;
  bool found = list.current()->Get(ro, lkey, &value, &s);
  ASSERT_FALSE(found);

  // Create a MemTable
  InternalKeyComparator cmp(BytewiseComparator());
  auto factory = std::make_shared<SkipListFactory>();
  options.memtable_factory = factory;
  ImmutableCFOptions ioptions(options);

  WriteBuffer wb(options.db_write_buffer_size);
  MemTable* mem =
      new MemTable(cmp, ioptions, MutableCFOptions(options, ioptions), &wb,
                   kMaxSequenceNumber);
  mem->Ref();

  // Write some keys to this memtable.
  mem->Add(++seq, kTypeDeletion, "key1", "");
  mem->Add(++seq, kTypeValue, "key2", "value2");
  mem->Add(++seq, kTypeValue, "key2", "value2.2");

  // Fetch the newly written keys
  found = mem->Get(ro, LookupKey("key1", seq), &value, &s);
  // MemTable found out that this key is *not* found (at this sequence#)
  ASSERT_TRUE(found && s.IsNotFound());

  found = mem->Get(ro, LookupKey("key2", seq), &value, &s);
  ASSERT_TRUE(s.ok() && found);
  ASSERT_EQ(value, "value2.2");

  // Add memtable to list
  list.Add(mem, &to_delete);
  ASSERT_EQ(0, to_delete.size());

  // Fetch keys via MemTableList
  found = list.current()->Get(ro, LookupKey("key1", seq), &value, &s);
  ASSERT_TRUE(found && s.IsNotFound());

  found = list.current()->Get(ro, LookupKey("key2", seq), &value, &s);
  ASSERT_TRUE(s.ok() && found);
  ASSERT_EQ("value2.2", value);

  // Flush this memtable from the list.
  // (It will then be a part of the memtable history).
  std::vector<MemTable*> to_flush;
  list.PickMemtablesToFlush(&to_flush);
  ASSERT_EQ(1, to_flush.size());

  s = Mock_InstallMemtableFlushResults(
      &list, MutableCFOptions(options, ioptions), to_flush, &to_delete);
  ASSERT_OK(s);
  ASSERT_EQ(0, list.NumNotFlushed());
  ASSERT_EQ(1, list.NumFlushed());
  ASSERT_EQ(0, to_delete.size());

  // Verify keys are no longer in MemTableList
  found = list.current()->Get(ro, LookupKey("key1", seq), &value, &s);
  ASSERT_FALSE(found);

  found = list.current()->Get(ro, LookupKey("key2", seq), &value, &s);
  ASSERT_FALSE(found);

  // Verify keys are present in history
  found =
      list.current()->GetFromHistory(ro, LookupKey("key1", seq), &value, &s);
  ASSERT_TRUE(found && s.IsNotFound());

  found =
      list.current()->GetFromHistory(ro, LookupKey("key2", seq), &value, &s);
  ASSERT_TRUE(found);
  ASSERT_EQ("value2.2", value);

  // Create another memtable and write some keys to it
  WriteBuffer wb2(options.db_write_buffer_size);
  MemTable* mem2 =
      new MemTable(cmp, ioptions, MutableCFOptions(options, ioptions), &wb2,
                   kMaxSequenceNumber);
  mem2->Ref();

  mem2->Add(++seq, kTypeDeletion, "key1", "");
  mem2->Add(++seq, kTypeValue, "key3", "value3");

  // Add second memtable to list
  list.Add(mem2, &to_delete);
  ASSERT_EQ(0, to_delete.size());

  to_flush.clear();
  list.PickMemtablesToFlush(&to_flush);
  ASSERT_EQ(1, to_flush.size());

  // Flush second memtable
  s = Mock_InstallMemtableFlushResults(
      &list, MutableCFOptions(options, ioptions), to_flush, &to_delete);
  ASSERT_OK(s);
  ASSERT_EQ(0, list.NumNotFlushed());
  ASSERT_EQ(2, list.NumFlushed());
  ASSERT_EQ(0, to_delete.size());

  // Add a third memtable to push the first memtable out of the history
  WriteBuffer wb3(options.db_write_buffer_size);
  MemTable* mem3 =
      new MemTable(cmp, ioptions, MutableCFOptions(options, ioptions), &wb3,
                   kMaxSequenceNumber);
  mem3->Ref();
  list.Add(mem3, &to_delete);
  ASSERT_EQ(1, list.NumNotFlushed());
  ASSERT_EQ(1, list.NumFlushed());
  ASSERT_EQ(1, to_delete.size());

  // Verify keys are no longer in MemTableList
  found = list.current()->Get(ro, LookupKey("key1", seq), &value, &s);
  ASSERT_FALSE(found);

  found = list.current()->Get(ro, LookupKey("key2", seq), &value, &s);
  ASSERT_FALSE(found);

  found = list.current()->Get(ro, LookupKey("key3", seq), &value, &s);
  ASSERT_FALSE(found);

  // Verify that the second memtable's keys are in the history
  found =
      list.current()->GetFromHistory(ro, LookupKey("key1", seq), &value, &s);
  ASSERT_TRUE(found && s.IsNotFound());

  found =
      list.current()->GetFromHistory(ro, LookupKey("key3", seq), &value, &s);
  ASSERT_TRUE(found);
  ASSERT_EQ("value3", value);

  // Verify that key2 from the first memtable is no longer in the history
  found = list.current()->Get(ro, LookupKey("key2", seq), &value, &s);
  ASSERT_FALSE(found);

  // Cleanup
  list.current()->Unref(&to_delete);
  ASSERT_EQ(3, to_delete.size());
  for (MemTable* m : to_delete) {
    delete m;
  }
}

TEST_F(MemTableListTest, FlushPendingTest) {
  const int num_tables = 5;
  SequenceNumber seq = 1;
  Status s;

  auto factory = std::make_shared<SkipListFactory>();
  options.memtable_factory = factory;
  ImmutableCFOptions ioptions(options);
  InternalKeyComparator cmp(BytewiseComparator());
  WriteBuffer wb(options.db_write_buffer_size);
  std::vector<MemTable*> to_delete;

  // Create MemTableList
  int min_write_buffer_number_to_merge = 3;
  int max_write_buffer_number_to_maintain = 7;
  MemTableList list(min_write_buffer_number_to_merge,
                    max_write_buffer_number_to_maintain);

  // Create some MemTables
  std::vector<MemTable*> tables;
  MutableCFOptions mutable_cf_options(options, ioptions);
  for (int i = 0; i < num_tables; i++) {
    MemTable* mem = new MemTable(cmp, ioptions, mutable_cf_options, &wb,
                                 kMaxSequenceNumber);
    mem->Ref();

    std::string value;

    mem->Add(++seq, kTypeValue, "key1", ToString(i));
    mem->Add(++seq, kTypeValue, "keyN" + ToString(i), "valueN");
    mem->Add(++seq, kTypeValue, "keyX" + ToString(i), "value");
    mem->Add(++seq, kTypeValue, "keyM" + ToString(i), "valueM");
    mem->Add(++seq, kTypeDeletion, "keyX" + ToString(i), "");

    tables.push_back(mem);
  }

  // Nothing to flush
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));
  std::vector<MemTable*> to_flush;
  list.PickMemtablesToFlush(&to_flush);
  ASSERT_EQ(0, to_flush.size());

  // Request a flush even though there is nothing to flush
  list.FlushRequested();
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Attempt to 'flush' to clear request for flush
  list.PickMemtablesToFlush(&to_flush);
  ASSERT_EQ(0, to_flush.size());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Request a flush again
  list.FlushRequested();
  // No flush pending since the list is empty.
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Add 2 tables
  list.Add(tables[0], &to_delete);
  list.Add(tables[1], &to_delete);
  ASSERT_EQ(2, list.NumNotFlushed());
  ASSERT_EQ(0, to_delete.size());

  // Even though we have less than the minimum to flush, a flush is
  // pending since we had previously requested a flush and never called
  // PickMemtablesToFlush() to clear the flush.
  ASSERT_TRUE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Pick tables to flush
  list.PickMemtablesToFlush(&to_flush);
  ASSERT_EQ(2, to_flush.size());
  ASSERT_EQ(2, list.NumNotFlushed());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Revert flush
  list.RollbackMemtableFlush(to_flush, 0);
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));
  to_flush.clear();

  // Add another table
  list.Add(tables[2], &to_delete);
  // We now have the minimum to flush regardles of whether FlushRequested()
  // was called.
  ASSERT_TRUE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));
  ASSERT_EQ(0, to_delete.size());

  // Pick tables to flush
  list.PickMemtablesToFlush(&to_flush);
  ASSERT_EQ(3, to_flush.size());
  ASSERT_EQ(3, list.NumNotFlushed());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Pick tables to flush again
  std::vector<MemTable*> to_flush2;
  list.PickMemtablesToFlush(&to_flush2);
  ASSERT_EQ(0, to_flush2.size());
  ASSERT_EQ(3, list.NumNotFlushed());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Add another table
  list.Add(tables[3], &to_delete);
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));
  ASSERT_EQ(0, to_delete.size());

  // Request a flush again
  list.FlushRequested();
  ASSERT_TRUE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Pick tables to flush again
  list.PickMemtablesToFlush(&to_flush2);
  ASSERT_EQ(1, to_flush2.size());
  ASSERT_EQ(4, list.NumNotFlushed());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Rollback first pick of tables
  list.RollbackMemtableFlush(to_flush, 0);
  ASSERT_TRUE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));
  to_flush.clear();

  // Add another tables
  list.Add(tables[4], &to_delete);
  ASSERT_EQ(5, list.NumNotFlushed());
  // We now have the minimum to flush regardles of whether FlushRequested()
  ASSERT_TRUE(list.IsFlushPending());
  ASSERT_TRUE(list.imm_flush_needed.load(std::memory_order_acquire));
  ASSERT_EQ(0, to_delete.size());

  // Pick tables to flush
  list.PickMemtablesToFlush(&to_flush);
  // Should pick 4 of 5 since 1 table has been picked in to_flush2
  ASSERT_EQ(4, to_flush.size());
  ASSERT_EQ(5, list.NumNotFlushed());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Pick tables to flush again
  std::vector<MemTable*> to_flush3;
  ASSERT_EQ(0, to_flush3.size());  // nothing not in progress of being flushed
  ASSERT_EQ(5, list.NumNotFlushed());
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Flush the 4 memtables that were picked in to_flush
  s = Mock_InstallMemtableFlushResults(
      &list, MutableCFOptions(options, ioptions), to_flush, &to_delete);
  ASSERT_OK(s);

  // Note:  now to_flush contains tables[0,1,2,4].  to_flush2 contains
  // tables[3].
  // Current implementation will only commit memtables in the order they were
  // created.  So InstallMemtableFlushResults will install the first 3 tables
  // in to_flush and stop when it encounters a table not yet flushed.
  ASSERT_EQ(2, list.NumNotFlushed());
  int num_in_history = std::min(3, max_write_buffer_number_to_maintain);
  ASSERT_EQ(num_in_history, list.NumFlushed());
  ASSERT_EQ(5 - list.NumNotFlushed() - num_in_history, to_delete.size());

  // Request a flush again. Should be nothing to flush
  list.FlushRequested();
  ASSERT_FALSE(list.IsFlushPending());
  ASSERT_FALSE(list.imm_flush_needed.load(std::memory_order_acquire));

  // Flush the 1 memtable that was picked in to_flush2
  s = MemTableListTest::Mock_InstallMemtableFlushResults(
      &list, MutableCFOptions(options, ioptions), to_flush2, &to_delete);
  ASSERT_OK(s);

  // This will actually install 2 tables.  The 1 we told it to flush, and also
  // tables[4] which has been waiting for tables[3] to commit.
  ASSERT_EQ(0, list.NumNotFlushed());
  num_in_history = std::min(5, max_write_buffer_number_to_maintain);
  ASSERT_EQ(num_in_history, list.NumFlushed());
  ASSERT_EQ(5 - list.NumNotFlushed() - num_in_history, to_delete.size());

  for (const auto& m : to_delete) {
    // Refcount should be 0 after calling InstallMemtableFlushResults.
    // Verify this, by Ref'ing then UnRef'ing:
    m->Ref();
    ASSERT_EQ(m, m->Unref());
    delete m;
  }
  to_delete.clear();

  list.current()->Unref(&to_delete);
  int to_delete_size = std::min(5, max_write_buffer_number_to_maintain);
  ASSERT_EQ(to_delete_size, to_delete.size());

  for (const auto& m : to_delete) {
    // Refcount should be 0 after calling InstallMemtableFlushResults.
    // Verify this, by Ref'ing then UnRef'ing:
    m->Ref();
    ASSERT_EQ(m, m->Unref());
    delete m;
  }
  to_delete.clear();
}

}  // namespace vidardb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
