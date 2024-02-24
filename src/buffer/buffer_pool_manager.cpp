//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
// #include <mutex>
#include <sstream>
#include <string>
#include <utility>
// #include <thread>

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

#include "include/common/config.h"
#include "include/common/logger.h"
#include "storage/index/b_plus_tree.h"

// #define ZHHAO_P2_BUFFERPOOL_DEBUG
// 10/29 对buffer pool的disk write/read部分进行优化，在多个进程需要fetch/new时，有些进程并不需要等待其他
// 进程磁盘操作的结果，应可以马上获取latch_锁，从而提高并发；

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  // frame_latch_ = new std::mutex[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  // delete[] frame_latch_;
}

auto BufferPoolManager::NewPage(page_id_t *page_id, AccessType access_type) -> Page * {
  frame_id_t frame_to_set;
  page_id_t dirty_page_id;
  bool dirty_flag{false};

  latch_.lock();
  if (!free_list_.empty()) {  // 检查free_list_并分配frame_id
    frame_to_set = this->free_list_.front();
    free_list_.pop_front();
  } else {
    if (replacer_->Evict(&frame_to_set)) {  // evict, 并清理旧page维护信息
      dirty_page_id = pages_[frame_to_set].GetPageId();
      page_table_.erase(dirty_page_id);
      if (pages_[frame_to_set].IsDirty()) {
        dirty_flag = true;
      }
    } else {
      latch_.unlock();
      return nullptr;
    }
  }
  // 申请新的page id
  *page_id = AllocatePage();
  // 初始化page在被分配frame的维护信息
  replacer_->RecordAccess(frame_to_set);
  replacer_->SetEvictable(frame_to_set, false);
  pages_[frame_to_set].is_dirty_ = false;
  pages_[frame_to_set].page_id_ = *page_id;
  pages_[frame_to_set].pin_count_ = 1;
  page_table_.insert(std::make_pair(*page_id, frame_to_set));

  // frame_latch_[frame_to_set].lock();
  // latch_.unlock();
  auto new_page = &pages_[frame_to_set];
  if (access_type == AccessType::Scan) {
    new_page->WLatch();
  }
  // latch_.unlock();
  auto dirty_data = pages_[frame_to_set].GetData();
  if (dirty_flag) {
    disk_manager_->WritePage(dirty_page_id, dirty_data);
  }
  latch_.unlock();
  // frame_latch_[frame_to_set].unlock();

  return new_page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  frame_id_t frame_to_fetch;
  bool dirty_flag{false};
  page_id_t dirty_page_id;
#ifdef ZHHAO_P2_BUFFERPOOL_DEBUG
  std::cout << "fetch page: " << page_id << "  | max_buffer_pool_size: " << this->replacer_->MaxSize()
            << "  | curr_buffer_pool_size: " << this->replacer_->GetSize()
            << "  | curr_evictable_nums: " << this->replacer_->GetEvictableSize()
            << "  | tread: " << std::this_thread::get_id() << std::endl;
#endif

  latch_.lock();
  if (page_table_.find(page_id) != page_table_.end()) {  // 该page就在frame中，更新记录和pin即可，然后尝试获取读写锁
    frame_to_fetch = page_table_.at(page_id);
    replacer_->RecordAccess(frame_to_fetch);
    replacer_->SetEvictable(frame_to_fetch, false);
    pages_[frame_to_fetch].pin_count_++;
    auto fetch_page = &pages_[frame_to_fetch];
    latch_.unlock();

    // frame_latch_[frame_to_fetch].lock();
    // latch_.unlock();
    if (access_type == AccessType::Get) {
      fetch_page->RLatch();
    } else if (access_type == AccessType::Scan) {
      fetch_page->WLatch();
    }
    // frame_latch_[frame_to_fetch].unlock();

    return fetch_page;
  }
  if (!free_list_.empty()) {  // 检查free_list_并分配frame
    // 但是至少在proj2里面fetch都是获取已经申请了page id的，而只有在free_list_满了后才可能被踢掉而进入这条语句，
    // 在proj2的一次运行中，free_list_满了后就不会回收，所以这里实际不会执行到
    frame_to_fetch = free_list_.front();
    free_list_.pop_front();
  } else {
    if (replacer_->Evict(&frame_to_fetch)) {
      dirty_page_id = pages_[frame_to_fetch].GetPageId();
      page_table_.erase(dirty_page_id);
      if (pages_[frame_to_fetch].IsDirty()) {
        dirty_flag = true;
      }
    } else {
      latch_.unlock();
      return nullptr;
    }
  }
  // 初始化page在被分配frame的维护信息
  replacer_->RecordAccess(frame_to_fetch);
  replacer_->SetEvictable(frame_to_fetch, false);
  pages_[frame_to_fetch].is_dirty_ = false;
  pages_[frame_to_fetch].page_id_ = page_id;
  pages_[frame_to_fetch].pin_count_ = 1;
  page_table_.insert(std::make_pair(page_id, frame_to_fetch));  // 得到frame后，马上装载进pgtbl里

  // frame_latch_[frame_to_fetch].lock();
  // latch_.unlock();
  // frame_latch_[frame_to_fetch].lock();
  auto fetch_page = &pages_[frame_to_fetch];
  // latch_.unlock();
  if (dirty_flag) {
    disk_manager_->WritePage(dirty_page_id, pages_[frame_to_fetch].GetData());
  }
  disk_manager_->ReadPage(page_id, pages_[frame_to_fetch].GetData());
  latch_.unlock();
  // frame_latch_[frame_to_fetch].unlock();

  if (access_type == AccessType::Get) {
    fetch_page->RLatch();
  } else if (access_type == AccessType::Scan) {
    fetch_page->WLatch();
  }

  return fetch_page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  auto frame_to_unpin = page_table_.at(page_id);
  if (pages_[frame_to_unpin].GetPinCount() <= 0) {
    if (pages_[frame_to_unpin].GetPinCount() < 0) {
      std::cout << "over-unpin before unpin again" << std::endl;
    }
    latch_.unlock();
    return false;
  }
  pages_[frame_to_unpin].pin_count_--;
  if (pages_[frame_to_unpin].GetPinCount() <= 0) {
    if (pages_[frame_to_unpin].GetPinCount() < 0) {
      std::cout << "over-unpin after this unpin" << std::endl;
    }
    replacer_->SetEvictable(frame_to_unpin, true);
  }
  if (is_dirty) {  // 因为一个线程的false不代表所有线程的false
    pages_[frame_to_unpin].is_dirty_ = true;
  }

  latch_.unlock();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }

  auto frame_to_flush = page_table_.at(page_id);
  disk_manager_->WritePage(page_id, pages_[frame_to_flush].GetData());
  pages_[frame_to_flush].is_dirty_ = false;

  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPages() {
  latch_.lock();
  for (auto it : page_table_) {
    if (!FlushPage(it.first)) {
      latch_.unlock();
      BUSTUB_ASSERT("wrong when flushing all pages, at page id: {}.", it.first);
      latch_.lock();
    }
  }
  latch_.unlock();
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return true;
  }
  auto frame_to_delete = page_table_.at(page_id);
  if (pages_[frame_to_delete].GetPinCount() != 0) {
    latch_.unlock();
    return false;
  }
  // 我觉得还是要写回disk的
  if (pages_[frame_to_delete].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_to_delete].GetPageId(),
                             pages_[frame_to_delete].GetData());  // dirty不按规矩来
  }
  // 删掉memory里的痕迹
  page_table_.erase(page_id);
  replacer_->Remove(frame_to_delete);
  free_list_.push_back(frame_to_delete);
  pages_[frame_to_delete].page_id_ = INVALID_PAGE_ID;
  pages_[frame_to_delete].is_dirty_ = false;
  pages_[frame_to_delete].pin_count_ = 0;

  DeallocatePage(page_id);

  latch_.unlock();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

// PageGuard
auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
#ifdef ZHHAO_P2_BUFFERPOOL_DEBUG
  std::cout << "fetch basic page: " << page_id << std::endl;
#endif
  auto fetch_page = FetchPage(page_id);
  return {this, fetch_page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
#ifdef ZHHAO_P2_BUFFERPOOL_DEBUG
  std::cout << "fetch read page: " << page_id << std::endl;
#endif
  auto fetch_page = FetchPage(page_id, AccessType::Get);
  // fetch_page->RLatch();
  return {this, fetch_page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
#ifdef ZHHAO_P2_BUFFERPOOL_DEBUG
  std::cout << "fetch write page: " << page_id << std::endl;
#endif
  auto fetch_page = FetchPage(page_id, AccessType::Scan);
  // fetch_page->WLatch();
  return {this, fetch_page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id, AccessType access_type) -> BasicPageGuard {
  auto fetch_page = NewPage(page_id, access_type);
#ifdef ZHHAO_P2_BUFFERPOOL_DEBUG
  std::cout << "new basic page: " << *page_id << std::endl;
#endif

  return {this, fetch_page};
}

}  // namespace bustub
