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
#include <cstdint>
#include <cstdio>
#include <iostream>
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
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 3, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  int64_t scale;
  std::cin >> scale;
  std::vector<int64_t> keys;
  std::vector<int64_t> remove_keys;
  for (int i = 1; i <= scale; i++) {
    keys.push_back(i);
  }

  // randomized the insertion order
  auto rng = std::default_random_engine{};
  std::shuffle(keys.begin(), keys.end(), rng);
  std::vector<RID> rids;
  for (auto key : keys) {
    std::cout << "i" << key << " ";
  }
  std::cout << std::endl;

  std::cout << "---插入smooth line---" << std::endl;
  // 插入
  {
    for (auto key : keys) {
      rid.Set(static_cast<int32_t>(key >> 32), static_cast<int>(key & 0xFFFFFFFF));
      index_key.SetFromInteger(key);
      std::cout << "----------" << key << "------------" << std::endl;
      tree.Insert(index_key, rid, transaction);
    }
  }

  std::cout << "---skewed删除smooth line---" << std::endl;
  // skewed删除  // 其实也没有很skewed吧，本来就是打乱过的
  int i = 0;
  for (auto remove_key = keys.begin(); i < scale / 2; remove_key++, i++) {
    std::cout << "--" << *remove_key << "--" << std::endl;
    remove_keys.push_back(*remove_key);
    rid.Set(static_cast<int32_t>(*remove_key >> 32), static_cast<int>(*remove_key & 0xFFFFFFFF));
    index_key.SetFromInteger(*remove_key);
    tree.Remove(index_key, transaction);
  }

  // std::cout << "---均匀删除smooth line---" << std::endl;
  // // diversity删除
  // {
  //   for (int i = 1; i <= 10*scale; i++) {
  //     remove_keys.push_back(i);
  //   }
  //   auto rng = std::default_random_engine{};
  //   std::shuffle(remove_keys.begin(), remove_keys.end(), rng);
  //   int i = 0;
  //   for (auto remove_key = remove_keys.begin(); i < scale/2; remove_key++, i++) {
  //     std::cout << "--" << *remove_key << "--" << std::endl;
  //     rid.Set(static_cast<int32_t>(*remove_key >> 32), static_cast<int>(*remove_key & 0xFFFFFFFF));
  //     index_key.SetFromInteger(*remove_key);
  //     tree.Remove(index_key, transaction);
  //   }
  // }

  std::cout << "---查找smooth line---" << std::endl;
  // 查找
  {
    for (auto key : keys) {
      rids.clear();
      index_key.SetFromInteger(key);
      // tree.GetValue(index_key, &rids);
      std::cout << key << std::endl;
      // 检查删除功能是否正常
      bool is_present = tree.GetValue(index_key, &rids);
      if (!is_present) {
        ASSERT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
      } else {
        ASSERT_EQ(rids.size(), 1);
        int64_t value = key & 0xFFFFFFFF;
        ASSERT_EQ(rids[0].GetSlotNum(), value);
      }
    }
  }

  std::cout << "---迭代查找smooth line---" << std::endl;
  // 迭代器查找
  {
    // int64_t i=1;
    for (auto it = tree.Begin(); it != tree.End(); ++it) {
      std::cout << (*it).second.GetSlotNum() << std::endl;
      ASSERT_EQ(std::find(remove_keys.begin(), remove_keys.end(), (*it).second.GetSlotNum()), remove_keys.end());
      ASSERT_NE(std::find(keys.begin(), keys.end(), (*it).second.GetSlotNum()), keys.end());
    }
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}
}  // namespace bustub
