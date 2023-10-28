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
#include <sstream>
// #include <thread>

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

#include "include/common/config.h"
#include "include/common/logger.h"
#include "storage/index/b_plus_tree.h"

// #define ZHHAO_P2_BUFFERPOOL_DEBUG

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
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
  // PROJ_2#2，DEBUG_LOG
  // 这一处的逻辑是这样的，如果没有这个，因为会出现还没有来得及对某page做修改就因为evictable被驱逐了，此时
  // 有个进程会要去用这个page，已知page id，但没有写回则disk_manager不认为自己分配了该page，会报错，返回空数据
  // 但返回空数据本身没有影响，因为逻辑上还没有修改过的数据返回空数据也符合预期，这也是为什么虽然报错但基本的查询结果都没问题
  // 分析：我在b_plus_tree的实现中，在获取新page时为NewPageGuarded; guard =
  // FetchGuarded;前者是有返回值的，但我没有绑定变量
  // 这使得新New的guard遵循RAII设计自主释放了，因而可以被驱逐；所以我再接着想fetch时就可能会得到空数据；
  // 可如果仅仅是这个，问题貌似不是太大，无非是爆一些warning而已
  // 正常的写回没有问题，因为我b_plus_tree是唯一使用并调用page的应用层，而每次调用page我都有AsMut，所以脏位设置应该没有遗漏
  // disk_manager_->WritePage(*page_id, pages_[frame_to_set].GetData());

  auto return_page = &pages_[frame_to_set];

  latch_.unlock();
  return return_page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  latch_.lock();
#ifdef ZHHAO_P2_BUFFERPOOL_DEBUG
  std::cout << "fetch page: " << page_id << "  | max_buffer_pool_size: " << this->replacer_->MaxSize()
            << "  | curr_buffer_pool_size: " << this->replacer_->GetSize()
            << "  | curr_evictable_nums: " << this->replacer_->GetEvictableSize()
            << "  | tread: " << std::this_thread::get_id() << std::endl;
#endif
  if (page_table_.find(page_id) == page_table_.end() && replacer_->GetEvictableSize() <= 0 && free_list_.empty()) {
    latch_.unlock();
    return nullptr;
  }
  frame_id_t frame_to_fetch;
  if (page_table_.find(page_id) == page_table_.end()) {  // 尚未装载到pgtbl中，因为没有对应的frame给到这个page_id
    if (free_list_.empty()) {                            // 要么是需要踢出一些frame的
      replacer_->Evict(&frame_to_fetch);
      page_table_.erase(pages_[frame_to_fetch].GetPageId());
    } else {  // 要么是free_list里frame还有没用的
      frame_to_fetch = free_list_.front();
      free_list_.pop_front();
    }
    page_table_.insert(std::make_pair(page_id, frame_to_fetch));  // 得到frame后，马上装载进pgtbl里
  } else {  // 是在原已有的page上读入，那么没有必要清除这个frame的history
    frame_to_fetch = page_table_.at(page_id);
    auto fetch_page = &pages_[frame_to_fetch];
    replacer_->RecordAccess(frame_to_fetch);
    replacer_->SetEvictable(frame_to_fetch, false);
    pages_[frame_to_fetch].pin_count_++;
    latch_.unlock();
    return fetch_page;
  }
  // 不管是哪种情况，现在已经得到了frame_to_fetch，并都载入了pgtbl，且得到了一个已有数据的page，需要对page的meta
  // data做一些初始化
  auto fetch_page = &pages_[frame_to_fetch];
  if (pages_[frame_to_fetch].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_to_fetch].GetPageId(), pages_[frame_to_fetch].GetData());  //
  }
  pages_[frame_to_fetch].pin_count_ = 1;
  pages_[frame_to_fetch].page_id_ = page_id;
  pages_[frame_to_fetch].is_dirty_ = false;

  disk_manager_->ReadPage(page_id, pages_[frame_to_fetch].GetData());
  replacer_->RecordAccess(frame_to_fetch);
  replacer_->SetEvictable(frame_to_fetch, false);

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
  auto fetch_page = FetchPage(page_id);
  fetch_page->RLatch();
  return {this, fetch_page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
#ifdef ZHHAO_P2_BUFFERPOOL_DEBUG
  std::cout << "fetch write page: " << page_id << std::endl;
#endif
  auto fetch_page = FetchPage(page_id);
  fetch_page->WLatch();
  return {this, fetch_page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto fetch_page = NewPage(page_id);
#ifdef ZHHAO_P2_BUFFERPOOL_DEBUG
  std::cout << "new basic page: " << *page_id << std::endl;
#endif

  return {this, fetch_page};
}

}  // namespace bustub
