//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
// TEST(PageGuardTest, SampleTest) {
//   const std::string db_name = "test.db";
//   const size_t buffer_pool_size = 5;
//   const size_t k = 2;

//   auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
//   auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

//   page_id_t page_id_temp, page_id_temp2;
//   auto *page0 = bpm->NewPage(&page_id_temp);
//   auto *page1 = bpm->NewPage(&page_id_temp2);

//   auto guarded_page = BasicPageGuard(bpm.get(), page0);
//   auto &&ref = guarded_page;
//   auto guarded_page_copy = BasicPageGuard(std::move(ref));
//   auto &&ref2 = guarded_page_copy;
//   guarded_page = std::move(ref2);
//   auto gpage2 = BasicPageGuard(bpm.get(), page1);
//   auto &&ref3 = gpage2;
//   guarded_page = std::move(ref3);

//   // EXPECT_NE(page0->GetData(), guarded_page.GetData());
//   // EXPECT_NE(page0->GetPageId(), guarded_page.PageId());
//   // EXPECT_EQ(nullptr, guarded_page_copy.GetPage());
//   // EXPECT_EQ(page0->GetData(), guarded_page_copy.GetData());
//   // EXPECT_EQ(page0->GetPageId(), guarded_page_copy.PageId());
//   EXPECT_EQ(page1->GetData(), guarded_page.GetData());
//   EXPECT_EQ(page1->GetPageId(), guarded_page.PageId());
//   EXPECT_EQ(1, page1->GetPinCount());
//   {
//     gpage2.Drop();
//     EXPECT_EQ(1, page1->GetPinCount());
//     guarded_page.Drop();
//     EXPECT_EQ(0, page1->GetPinCount());
//     EXPECT_EQ(INVALID_PAGE_ID, page0->GetPageId());
//   }

//   // Shutdown the disk manager and remove the temporary file we created.
//   disk_manager->ShutDown();
// }
TEST(PageGuardTest, ReadTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // test ~ReadPageGuard()
  {
    auto reader_guard = bpm->FetchPageRead(page_id_temp);
    EXPECT_EQ(2, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());
  std::cout << "1" << std::endl;
  // test ReadPageGuard(ReadPageGuard &&that)
  {
    auto reader_guard = bpm->FetchPageRead(page_id_temp);
    EXPECT_EQ(2, page0->GetPinCount());
    std::cout << "-------------" << std::endl;
    auto reader_guard_2 = ReadPageGuard(std::move(reader_guard));
    std::cout << "@@@" << std::endl;
    EXPECT_EQ(2, page0->GetPinCount());
    std::cout << "&&&" << std::endl;
  }
  EXPECT_EQ(1, page0->GetPinCount());
  std::cout << "2" << std::endl;
  // test ReadPageGuard::operator=(ReadPageGuard &&that)
  {
    auto reader_guard_1 = bpm->FetchPageRead(page_id_temp);
    auto reader_guard_2 = bpm->FetchPageRead(page_id_temp);
    EXPECT_EQ(3, page0->GetPinCount());
    reader_guard_1 = std::move(reader_guard_2);
    EXPECT_EQ(2, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());
  std::cout << "3" << std::endl;
  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

}  // namespace bustub
