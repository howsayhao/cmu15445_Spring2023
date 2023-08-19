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
  std::cout << "new page & frame:" << *page_id << " " << frame_to_set << std::endl;
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
  if (page_table_.find(page_id) == page_table_.end() && replacer_->Size() <= 0 && free_list_.empty()) {
    latch_.unlock();
    return nullptr;
  }
  std::cout << "fetch page:" << page_id << std::endl;
  frame_id_t frame_to_fetch;
  if (page_table_.find(page_id) == page_table_.end()) {  // 尚未装载到pgtbl中，因为没有对应的frame给到这个page_id
    std::cout << "not found in pgtbl" << std::endl;
    if (free_list_.empty()) {  // 要么是需要踢出一些frame
      std::cout << "free_list is empty" << std::endl;
      replacer_->Evict(&frame_to_fetch);
      page_table_.erase(pages_[frame_to_fetch].GetPageId());
      // pages_[frame_to_fetch].pin_count_++;
      pages_[frame_to_fetch].pin_count_ = 1;
    } else {  // 要么是frame还有没用的
      std::cout << "free_list not empty" << std::endl;
      frame_to_fetch = free_list_.front();
      free_list_.pop_front();
      pages_[frame_to_fetch].pin_count_ = 1;
    }
    page_table_.insert(std::make_pair(page_id, frame_to_fetch));  // 得到frame后，马上装载进pgtbl里
  } else {  // 是在原已有的page上读入，那么没有必要清除这个frame的history
    std::cout << "found in pgtbl" << std::endl;
    frame_to_fetch = page_table_.at(page_id);
    replacer_->RecordAccess(frame_to_fetch);
    replacer_->SetEvictable(frame_to_fetch, false);
    pages_[frame_to_fetch].pin_count_++;
    auto fetch_page = &pages_[frame_to_fetch];
    latch_.unlock();
    return fetch_page;
    // pages_[frame_to_fetch].pin_count_++;
  }
  // 不管是哪种情况，现在已经得到了frame_to_fetch，并都载入了pgtbl，且得到了一个已有数据的page，需要对page的meta
  // data做一些初始化
  std::cout << "original page & original data & frame_to_fetch:" << pages_[frame_to_fetch].page_id_ << " "
            << pages_[frame_to_fetch].GetData() << " " << frame_to_fetch << std::endl;
  // if (pages_[frame_to_fetch].IsDirty() && pages_[frame_to_fetch].GetPageId() != page_id) {
  if (pages_[frame_to_fetch].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_to_fetch].GetPageId(), pages_[frame_to_fetch].GetData());  // dirty不按规矩来
    std::cout << "dirty override" << std::endl;
  }
  pages_[frame_to_fetch].page_id_ = page_id;
  // pages_[frame_to_fetch].pin_count_ = 1;
  pages_[frame_to_fetch].is_dirty_ = false;

  disk_manager_->ReadPage(page_id, pages_[frame_to_fetch].GetData());
  // 除了在原有page上读入，其他的都不需要设为dirty，因为就算dirty刚刚也已经写回disk了
  // pages_[frame_to_fetch].is_dirty_ = true;
  std::cout << "get data:" << pages_[frame_to_fetch].GetData() << std::endl;
  replacer_->RecordAccess(frame_to_fetch);
  replacer_->SetEvictable(frame_to_fetch, false);

  auto fetch_page = &pages_[frame_to_fetch];
  latch_.unlock();
  return fetch_page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  latch_.lock();
  std::cout << "unpin page & is_dirty:" << page_id << " " << is_dirty << std::endl;
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    std::cout << "unpin false 1" << std::endl;
    return false;
  }
  auto frame_to_unpin = page_table_.at(page_id);
  if (pages_[frame_to_unpin].GetPinCount() <= 0) {
    latch_.unlock();
    std::cout << "unpin false 2" << std::endl;
    return false;
  }
  pages_[frame_to_unpin].pin_count_--;
  std::cout << "after unpin, left pin count : " << pages_[frame_to_unpin].GetPinCount() << std::endl;
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
  std::cout << "flush page:" << page_id << std::endl;
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
  std::cout << "flush all pages" << std::endl;
  for (auto it : page_table_) {
    if (!FlushPage(it.first)) {
      BUSTUB_ASSERT("wrong when flushing all pages, at page id: {}.", it.first);
    }
  }
  latch_.unlock();
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();
  std::cout << "delete page:" << page_id << std::endl;
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
    disk_manager_->WritePage(pages_[frame_to_delete].GetPageId(), pages_[frame_to_delete].GetData());
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
  std::cout << "fetch basic page: " << page_id << std::endl;
  auto fetch_page = FetchPage(page_id);
  return {this, fetch_page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  std::cout << "fetch read page: " << page_id << std::endl;
  auto fetch_page = FetchPage(page_id);
  fetch_page->RLatch();
  return {this, fetch_page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  std::cout << "fetch write page: " << page_id << std::endl;
  auto fetch_page = FetchPage(page_id);
  fetch_page->WLatch();
  return {this, fetch_page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  std::cout << "new basic page: " << page_id << std::endl;
  auto fetch_page = NewPage(page_id);
  return {this, fetch_page};
}

}  // namespace bustub
