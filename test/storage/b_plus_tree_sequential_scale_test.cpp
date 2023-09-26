//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_sequential_scale_test.cpp
//
// Identification: test/storage/b_plus_tree_sequential_scale_test.cpp
//
// Copyright (c) 2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>
#include <random>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

// #include "../../src/include/buffer/buffer_pool_manager.h"
// #include "../../src/include/gtest/gtest.h"
// #include "../../src/include/storage/disk/disk_manager_memory.h"
// #include "../../src/include/storage/index/b_plus_tree.h"
// #include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

/**
 * This test should be passing with your Checkpoint 1 submission.
 */
TEST(BPlusTreeTests, ScaleTest) {  // NOLINT
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(30, disk_manager.get());

  // create and fetch header_page
  page_id_t page_id;
  auto *header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  int64_t scale = 5000;
  std::vector<int64_t> keys;
  for (int64_t key = 1; key < scale; key++) {
    keys.push_back(key);
  }

  // randomized the insertion order
  // auto rng = std::default_random_engine{};
  // std::shuffle(keys.begin(), keys.end(), rng);
  std::vector<RID> rids;
  // for (auto key : keys) {
  // std::cout << key << " ";
  // }
  for (auto key : keys) {
    rids.clear();
    rid.Set(static_cast<int32_t>(key >> 32), static_cast<int>(key & 0xFFFFFFFF));
    index_key.SetFromInteger(key);
    // int64_t value = key & 0xFFFFFFFF;
    // rid.Set(static_cast<int32_t>(key >> 32), value);
    // index_key.SetFromInteger(key);
    std::cout << "----------" << key << "------------" << std::endl;
    tree.Insert(index_key, rid, transaction);
    // std::cout << tree.DrawBPlusTree() << std::endl;
    // tree.Print(bpm);
    // char next;
    // std::cin >> next;
    // std::cout << key << std::endl;
    // auto ifempty = tree.Insert(index_key, rid, transaction);
    // std::cout << key << " " << ifempty << std::endl;
  }
  // tree.Print(bpm);
  //   index_key.SetFromInteger(133);
  //  auto ifempty = tree.GetValue(index_key, &rids);
  //   std::cout << "207"
  //             << " exist: " << ifempty << std::endl;
  // std::vector<RID> rids;
  std::cout << "---smooth line---" << std::endl;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    std::cout << key << std::endl;
    ASSERT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    ASSERT_EQ(rids[0].GetSlotNum(), value);
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}
}  // namespace bustub
