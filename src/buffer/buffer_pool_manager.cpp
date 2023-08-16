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

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  frame_id_t frame_to_set;
  latch_.lock();
  if (free_list_.empty()) {  // 没有空的frame了，需要找Replacer腾出空间
    if (replacer_->Evict(&frame_to_set)) {  // 若成功则说明是处于UnPin的状态，即evictable // 只有各个线程对该page/frame
                                            // unpin才允许evict
      page_table_.erase(pages_[frame_to_set].GetPageId());
      if (pages_[frame_to_set].IsDirty()) {
        disk_manager_->WritePage(pages_[frame_to_set].GetPageId(), pages_[frame_to_set].GetData());
      }
      pages_[frame_to_set].SetClear();
    } else {
      latch_.unlock();
      return nullptr;
    }
  } else {
    frame_to_set = this->free_list_.back();
    free_list_.pop_back();
    pages_[frame_to_set].SetClear();
  }
  *page_id = AllocatePage();
  page_table_.insert(std::make_pair(*page_id, frame_to_set));

  this->pages_[frame_to_set].SetId(*page_id);
  this->pages_[frame_to_set].SetPin();
  replacer_->RecordAccess(frame_to_set);
  replacer_->SetEvictable(frame_to_set, false);

  auto return_page = &pages_[frame_to_set];
  latch_.unlock();

  return return_page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end() && replacer_->Size() <= 0) {
    latch_.unlock();
    return nullptr;
  }
  frame_id_t frame_to_fetch;
  if (page_table_.find(page_id) == page_table_.end()) {  // 尚未装载到pgtbl中，因为没有对应的frame给到这个page_id
    if (free_list_.empty()) {                            // 要么是需要踢出一些frame
      replacer_->Evict(&frame_to_fetch);
      page_table_.erase(pages_[frame_to_fetch].GetPageId());
    } else {  // 要么是frame还有没用的
      frame_to_fetch = free_list_.back();
      free_list_.pop_back();
    }
    page_table_.insert(std::make_pair(page_id, frame_to_fetch));  // 得到frame后，马上装载进pgtbl里
  } else {  // 是在原已有的page上读入，那么没有必要清除这个frame的history
    frame_to_fetch = page_table_.at(page_id);
  }
  // 不管是哪种情况，现在已经得到了frame_to_fetch，并都载入了pgtbl，且得到了一个已有数据的page，需要对page的meta
  // data做一些初始化
  if (pages_[frame_to_fetch].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_to_fetch].GetPageId(), pages_[frame_to_fetch].GetData());
  }
  pages_[frame_to_fetch].SetClear();
  pages_[frame_to_fetch].SetId(page_id);
  pages_[frame_to_fetch].SetPin();

  disk_manager_->ReadPage(page_id, pages_[frame_to_fetch].GetData());
  // snprintf(pages_[frame_to_fetch].GetData(), BUSTUB_PAGE_SIZE, "Hello");
  replacer_->RecordAccess(frame_to_fetch);
  replacer_->SetEvictable(frame_to_fetch, false);

  auto fetch_page = &pages_[frame_to_fetch];
  latch_.unlock();
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
    latch_.unlock();
    return false;
  }
  pages_[frame_to_unpin].UnPin();
  if (pages_[frame_to_unpin].GetPinCount() <= 0) {
    replacer_->SetEvictable(frame_to_unpin, true);
  }
  if (is_dirty) {
    pages_[frame_to_unpin].SetDirtyFlag(true);
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
  pages_[frame_to_flush].SetDirtyFlag(false);

  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPages() {}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { return false; }

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
