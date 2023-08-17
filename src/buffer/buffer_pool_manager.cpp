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
  // 分配好给新page的页框frame
  if (free_list_.empty()) {  // 没有空的frame了，需要找Replacer腾出空间
    if (replacer_->Evict(&frame_to_set)) {  // 若成功则说明是处于UnPin的状态，即evictable // 只有各个线程对该page/frame
                                            // unpin才允许evict
      page_table_.erase(pages_[frame_to_set].GetPageId());
      if (pages_[frame_to_set].IsDirty()) {
        disk_manager_->WritePage(pages_[frame_to_set].GetPageId(), pages_[frame_to_set].GetData());
      }
    } else {
      latch_.unlock();
      return nullptr;
    }
  } else {
    frame_to_set = this->free_list_.front();
    free_list_.pop_front();
  }
  // 都要对新的page的meta信息做初始化
  *page_id = AllocatePage();
  pages_[frame_to_set].is_dirty_ = false;
  pages_[frame_to_set].page_id_ = *page_id;
  pages_[frame_to_set].pin_count_ = 1;
  page_table_.insert(std::make_pair(*page_id, frame_to_set));
  // 更新在LRUK-Replacer中的记录信息
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
      frame_to_fetch = free_list_.front();
      free_list_.pop_front();
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
  pages_[frame_to_fetch].page_id_ = page_id;
  pages_[frame_to_fetch].pin_count_ = 1;
  pages_[frame_to_fetch].is_dirty_ = false;

  disk_manager_->ReadPage(page_id, pages_[frame_to_fetch].GetData());
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
  pages_[frame_to_unpin].pin_count_--;
  if (pages_[frame_to_unpin].GetPinCount() <= 0) {
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
  for (auto it : page_table_) {
    if (!FlushPage(it.first)) {
      BUSTUB_ASSERT("wrong when flushing all pages, at page id: {}.", it.first);
    }
  }
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
  page_table_.erase(page_id);
  replacer_->Remove(frame_to_delete);
  free_list_.push_back(frame_to_delete);
  pages_[frame_to_delete].page_id_ = INVALID_PAGE_ID;
  pages_[frame_to_delete].is_dirty_ = false;
  pages_[frame_to_delete].pin_count_ = 0;

  DeallocatePage(page_id);

  latch_.unlock();
  return false;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
