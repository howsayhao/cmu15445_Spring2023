//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_concurrent_test.cpp
//
// Identification: test/storage/b_plus_tree_concurrent_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <chrono>  // NOLINT
#include <cstdint>
#include <cstdio>
#include <functional>
// #include <future>
#include <random>
#include <thread>  // NOLINT

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

#define INSERT_TEST
// #define REMOVE_TEST
// #define GET_TEST

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

// helper function to launch multiple threads
template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&...args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args..., thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// helper function to insert
void InsertHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree->Insert(index_key, rid, transaction);
  }
  delete transaction;
}

// helper function to seperate insert
void InsertHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                       int total_threads, __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree->Insert(index_key, rid, transaction);
    }
  }
  delete transaction;
}

// helper function to delete
void DeleteHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &remove_keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree->Remove(index_key, transaction);
  }
  delete transaction;
}

// helper function to seperate delete
void DeleteHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree,
                       const std::vector<int64_t> &remove_keys, int total_threads,
                       __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      index_key.SetFromInteger(key);
      tree->Remove(index_key, transaction);
    }
  }
  delete transaction;
}

void LookupHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                       int total_threads, __attribute__((unused)) uint64_t thread_itr = 0) {
  auto *transaction = new Transaction(0);
  GenericKey<8> index_key;
  RID rid;
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      std::vector<RID> result;
      bool res = tree->GetValue(index_key, &result, transaction);
      ASSERT_EQ(res, true);
      ASSERT_EQ(result.size(), 1);
      ASSERT_EQ(result[0], rid);
    }
  }
  delete transaction;
}

void LookupHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  uint64_t tid, __attribute__((unused)) uint64_t thread_itr = 0) {
  auto *transaction = new Transaction(static_cast<txn_id_t>(tid));
  GenericKey<8> index_key;
  RID rid;
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    std::vector<RID> result;
    bool res = tree->GetValue(index_key, &result, transaction);
    ASSERT_EQ(res, true);
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result[0], rid);
  }
  delete transaction;
}

// TEST(BPlusTreeConcurrentTest, InsertTest1) {
//   // create KeyComparator and index schema
//   auto key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema.get());

//   auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
//   auto *bpm = new BufferPoolManager(50, disk_manager.get());
//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bpm->NewPage(&page_id);
//   // create b+ tree
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator);
//   // keys to Insert
//   std::vector<int64_t> keys;
//   int64_t scale_factor = 100;
//   for (int64_t key = 1; key < scale_factor; key++) {
//     keys.push_back(key);
//   }
//   LaunchParallelTest(2, InsertHelper, &tree, keys);

//   std::vector<RID> rids;
//   GenericKey<8> index_key;
//   for (auto key : keys) {
//     rids.clear();
//     index_key.SetFromInteger(key);
//     tree.GetValue(index_key, &rids);
//     EXPECT_EQ(rids.size(), 1);

//     int64_t value = key & 0xFFFFFFFF;
//     EXPECT_EQ(rids[0].GetSlotNum(), value);
//   }

//   int64_t start_key = 1;
//   int64_t current_key = start_key;
//   index_key.SetFromInteger(start_key);
//   for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
//     auto location = (*iterator).second;
//     EXPECT_EQ(location.GetPageId(), 0);
//     EXPECT_EQ(location.GetSlotNum(), current_key);
//     current_key = current_key + 1;
//   }

//   EXPECT_EQ(current_key, keys.size() + 1);

//   bpm->UnpinPage(HEADER_PAGE_ID, true);
//   delete bpm;
// }

// TEST(BPlusTreeConcurrentTest, InsertTest2) {
//   // create KeyComparator and index schema
//   auto key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema.get());
//   auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
//   auto *bpm = new BufferPoolManager(50, disk_manager.get());
//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bpm->NewPage(&page_id);
//   // create b+ tree
//   std::cout << "leaf and internal max size :" << std::endl;
//   int64_t leaf_size;
//   int64_t internal_size;
//   std::cin >> leaf_size;
//   std::cin >> internal_size;
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator,
//                                                            leaf_size, internal_size);
//   // keys to Insert & Remove
//   std::cout << "key size and thread nums:" << std::endl;
//   std::vector<int64_t> keys;
//   std::vector<int64_t> init_keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
//   std::vector<int64_t> remove_keys;
//   int64_t scale_factor;
//   std::cin >> scale_factor;
//   for (int64_t key = 1; key < scale_factor; key++) {
//     keys.push_back(key);
//     if (key % 3 != 0) {
//       remove_keys.push_back(key);
//     }
//   }
//   auto rng = std::default_random_engine{};
//   std::shuffle(remove_keys.begin(), remove_keys.end(), rng);
//   int64_t thread_nums;
//   std::cin >> thread_nums;
// #ifdef GET_TEST
//   {  // 查找测试
//     InsertHelper(&tree, keys);
//     LaunchParallelTest(thread_nums, LookupHelperSplit, &tree, keys, thread_nums);
//   }
// #endif
// #ifdef INSERT_TEST
//   {  // 插入测试
//     // InsertHelper(&tree, init_keys);
//     // std::shuffle(keys.begin(), keys.end(), rng);
//     // auto clock_start = std::chrono::system_clock::now();
//     std::cout << "#runs: " << std::endl;
//     int64_t runs;
//     std::cin >> runs;
//     std::string filename;
//     std::cin >> filename;
//     for (int j = 0; j < runs; j++) {
//       LaunchParallelTest(thread_nums, InsertHelperSplit, &tree, keys, thread_nums);
//       std::cout << "-----------------------------------------" << std::endl;
//       tree.Draw(bpm, filename);
//       // DeleteHelper(&tree, keys);
//       std::vector<RID> rids;
//       GenericKey<8> index_key;
//       for (auto key : keys) {
//         rids.clear();
//         index_key.SetFromInteger(key);
//         tree.GetValue(index_key, &rids);
//         EXPECT_EQ(rids.size(), 1);

//         int64_t value = key & 0xFFFFFFFF;
//         EXPECT_EQ(rids[0].GetSlotNum(), value);
//       }
//       // auto clock_end = std::chrono::system_clock::now();
//       // double dr_ms = std::chrono::duration<double, std::milli>(clock_end - clock_start).count();
//       // 防止报错地址泄露，没有释放bpm以及buffer pool的各个page
//       // bpm->UnpinPage(HEADER_PAGE_ID, true);
//       // delete bpm;
//       // ASSERT_TRUE(dr_ms < 30000);

//       int64_t start_key = 1;
//       int64_t current_key = start_key;
//       index_key.SetFromInteger(start_key);
//       for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
//         auto location = (*iterator).second;
//         EXPECT_EQ(location.GetPageId(), 0);
//         EXPECT_EQ(location.GetSlotNum(), current_key);
//         current_key = current_key + 1;
//       }
//       DeleteHelper(&tree, keys);
//     }
//     // EXPECT_EQ(current_key, keys.size() + 1);
//     // DeleteHelper(&tree, keys);
//   }
// #endif
// #ifdef REMOVE_TEST
//   {  // 删除测试
//     InsertHelper(&tree, keys);
//     LaunchParallelTest(thread_nums, DeleteHelperSplit, &tree, remove_keys, thread_nums);
//     std::vector<RID> rids;
//     GenericKey<8> index_key;
//     for (auto key : keys) {
//       rids.clear();
//       index_key.SetFromInteger(key);
//       tree.GetValue(index_key, &rids);
//       if (std::find(remove_keys.begin(), remove_keys.end(), key) == remove_keys.end()) {
//         EXPECT_EQ(rids.size(), 1);
//         int64_t value = key & 0xFFFFFFFF;
//         EXPECT_EQ(rids[0].GetSlotNum(), value);
//       } else {
//         EXPECT_EQ(rids.size(), 0);
//       }
//     }
//     int64_t start_key = 1;
//     int64_t current_key = start_key;
//     index_key.SetFromInteger(start_key);
//     for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
//       auto location = (*iterator).second;
//       EXPECT_EQ(location.GetPageId(), 0);
//       if (std::find(remove_keys.begin(), remove_keys.end(), current_key) == remove_keys.end()) {
//         EXPECT_EQ(location.GetSlotNum(), current_key);
//       } else {
//         EXPECT_EQ(1, 0);
//       }
//       current_key = current_key + 1;
//     }
//   }
// #endif

//   bpm->UnpinPage(HEADER_PAGE_ID, true);
//   delete bpm;
// }


TEST(BPlusTreeConcurrentTest, Concurrent_InsertTest2) {
  std::cout << "leaf and internal max size :" << std::endl;
  int64_t leaf_size;
  int64_t internal_size;
  std::cin >> leaf_size;
  std::cin >> internal_size;
  std::cout << "key size and thread nums:" << std::endl;
  int64_t scale_factor;
  std::cin >> scale_factor;
  int64_t thread_nums;
  std::cin >> thread_nums;
  std::cout << "#runs and outfile: " << std::endl;
  int64_t runs;
  std::cin >> runs;
  std::string filename;
  std::cin >> filename;
  for (int i=0; i<runs; i++) {
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());
    auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
    auto *bpm = new BufferPoolManager(50, disk_manager.get());
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator,
                                                            leaf_size, internal_size);
    std::vector<int64_t> keys;
    for (int64_t key = 1; key < scale_factor; key++) {
      keys.push_back(key);
    }
    auto rng = std::default_random_engine{};
  
    LaunchParallelTest(thread_nums, InsertHelperSplit, &tree, keys, thread_nums);
    std::cout << "-----------------------------------------" << std::endl;
    // tree.Draw(bpm, filename);
    std::vector<RID> rids;
    GenericKey<8> index_key;
    for (auto key : keys) {
      rids.clear();
      index_key.SetFromInteger(key);
      tree.GetValue(index_key, &rids);
      EXPECT_EQ(rids.size(), 1);

      int64_t value = key & 0xFFFFFFFF;
      EXPECT_EQ(rids[0].GetSlotNum(), value);
    }

    // int64_t start_key = 1;
    // int64_t current_key = start_key;
    // index_key.SetFromInteger(start_key);
    // for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    //   auto location = (*iterator).second;
    //   EXPECT_EQ(location.GetPageId(), 0);
    //   EXPECT_EQ(location.GetSlotNum(), current_key);
    //   current_key = current_key + 1;
    // }

    bpm->UnpinPage(HEADER_PAGE_ID, true);
    delete bpm;
  }
}

// TEST(BPlusTreeConcurrentTest, DeleteTest2) {
//   // create KeyComparator and index schema
//   auto key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema.get());

//   auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
//   auto *bpm = new BufferPoolManager(50, disk_manager.get());
//   GenericKey<8> index_key;
//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bpm->NewPage(&page_id);
//   // create b+ tree
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator);

//   // sequential insert
//   std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
//   InsertHelper(&tree, keys);

//   std::vector<int64_t> remove_keys = {1, 4, 3, 2, 5, 6};
//   LaunchParallelTest(2, DeleteHelperSplit, &tree, remove_keys, 2);

//   int64_t start_key = 7;
//   int64_t current_key = start_key;
//   int64_t size = 0;
//   index_key.SetFromInteger(start_key);
//   for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
//     auto location = (*iterator).second;
//     EXPECT_EQ(location.GetPageId(), 0);
//     EXPECT_EQ(location.GetSlotNum(), current_key);
//     current_key = current_key + 1;
//     size = size + 1;
//   }

//   EXPECT_EQ(size, 4);

//   bpm->UnpinPage(HEADER_PAGE_ID, true);
//   delete bpm;
// }

// TEST(BPlusTreeConcurrentTest, MixTest1) {
//   // create KeyComparator and index schema
//   auto key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema.get());

//   auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
//   auto *bpm = new BufferPoolManager(50, disk_manager.get());

//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bpm->NewPage(&page_id);
//   // create b+ tree
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator);
//   GenericKey<8> index_key;
//   // first, populate index
//   std::vector<int64_t> keys = {1, 2, 3, 4, 5};
//   InsertHelper(&tree, keys);

//   // concurrent insert
//   keys.clear();
//   for (int i = 6; i <= 10; i++) {
//     keys.push_back(i);
//   }
//   LaunchParallelTest(1, InsertHelper, &tree, keys);
//   // concurrent delete
//   std::vector<int64_t> remove_keys = {1, 4, 3, 5, 6};
//   LaunchParallelTest(1, DeleteHelper, &tree, remove_keys);

//   int64_t start_key = 2;
//   int64_t size = 0;
//   index_key.SetFromInteger(start_key);
//   for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
//     size = size + 1;
//   }

//   EXPECT_EQ(size, 5);

//   bpm->UnpinPage(HEADER_PAGE_ID, true);
//   delete bpm;
// }

// TEST(BPlusTreeConcurrentTest, MixTest2) {
//   // create KeyComparator and index schema
//   auto key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema.get());

//   auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
//   auto *bpm = new BufferPoolManager(50, disk_manager.get());

//   // create and fetch header_page
//   page_id_t page_id;
//   auto *header_page = bpm->NewPage(&page_id);
//   (void)header_page;

//   // create b+ tree
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator);

//   // Add perserved_keys
//   std::vector<int64_t> perserved_keys;
//   std::vector<int64_t> dynamic_keys;
//   int64_t total_keys = 5000;
//   int64_t sieve = 5;
//   for (int64_t i = 1; i <= total_keys; i++) {
//     if (i % sieve == 0) {
//       perserved_keys.push_back(i);
//     } else {
//       dynamic_keys.push_back(i);
//     }
//   }
//   InsertHelper(&tree, perserved_keys, 1);
//   // Check there are 1000 keys in there
//   size_t size;

//   auto insert_task = [&](int tid) { InsertHelper(&tree, dynamic_keys, tid); };
//   auto delete_task = [&](int tid) { DeleteHelper(&tree, dynamic_keys, tid); };
//   auto lookup_task = [&](int tid) { LookupHelper(&tree, perserved_keys, tid); };

//   std::vector<std::thread> threads;
//   std::vector<std::function<void(int)>> tasks;
//   tasks.emplace_back(insert_task);
//   tasks.emplace_back(delete_task);
//   tasks.emplace_back(lookup_task);

//   size_t num_threads = 6;
//   for (size_t i = 0; i < num_threads; i++) {
//     threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
//   }
//   for (size_t i = 0; i < num_threads; i++) {
//     threads[i].join();
//   }

//   // Check all reserved keys exist
//   size = 0;

//   for (auto iter = tree.Begin(); iter != tree.End(); ++iter) {
//     const auto &pair = *iter;
//     if ((pair.first).ToString() % sieve == 0) {
//       size++;
//     }
//   }

//   ASSERT_EQ(size, perserved_keys.size());

//   bpm->UnpinPage(HEADER_PAGE_ID, true);
//   delete bpm;
// }

}  // namespace bustub
