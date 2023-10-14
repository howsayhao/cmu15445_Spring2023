//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

// #define ZHHAO_P2_DEBUG

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  bool inf = false;
  bool evict_true = false;
  size_t max_time_stamp = 0;
  frame_id_t frame_to_evict{0};  // 没有意义的初始化0，所以需要evict_true辅助
  this->latch_.lock();
  for (auto const &it : this->node_store_) {  // it扫到后是pair类型
    if (it.second.EvictableTrue()) {
      evict_true = true;
      auto diff_stamp = this->current_timestamp_ - it.second.History().front();
      if (it.second.HistoryEntry() < this->k_) {  // Classical LRU
        if (!(inf && max_time_stamp > diff_stamp)) {
          inf = true;
          max_time_stamp = diff_stamp;
          frame_to_evict = it.first;
        }
      } else if (!inf) {  // LRU-K
        if (max_time_stamp < diff_stamp) {
          max_time_stamp = diff_stamp;
          frame_to_evict = it.first;
        }
      }
    }
  }
  if (!evict_true) {
    this->latch_.unlock();
    return false;
  }
  *frame_id = frame_to_evict;
  this->node_store_.erase(frame_to_evict);
  this->curr_size_--;
  this->latch_.unlock();
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  latch_.lock();
  if (static_cast<size_t>(frame_id) >= this->replacer_size_ || frame_id < 0) {
    latch_.unlock();
    BUSTUB_ASSERT("id {} :out of replacer_size_ range", frame_id);
  }
#ifdef ZHHAO_P2_DEBUG
  std::cout << "frame get accessed: " << frame_id << "  | current_buffer_size: " << node_store_.size() << std::endl;
#endif
  if (this->node_store_.find(frame_id) == this->node_store_.end()) {
    this->node_store_.insert(std::make_pair(frame_id, LRUKNode(k_)));
  }
  this->node_store_.at(frame_id).Access(this->current_timestamp_);
  this->current_timestamp_++;
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  if (static_cast<size_t>(frame_id) >= this->replacer_size_ || frame_id < 0) {
    latch_.unlock();
    BUSTUB_ASSERT("id {} :out of replacer_size_ range", frame_id);
  }
  if (this->node_store_.find(frame_id) == this->node_store_.end()) {
    latch_.unlock();
    BUSTUB_ASSERT("id {} :no such frame yet", frame_id);
  }
  auto tmp = &this->node_store_.at(frame_id);
  if (set_evictable && !tmp->EvictableTrue()) {
    tmp->VerseEvictable();
    this->curr_size_++;
  } else if (!set_evictable && tmp->EvictableTrue()) {
    tmp->VerseEvictable();
    this->curr_size_--;
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  if (static_cast<size_t>(frame_id) >= this->replacer_size_ || frame_id < 0) {
    latch_.unlock();
    BUSTUB_ASSERT("id {} :out of replacer_size_ range", frame_id);
  }
  if (this->node_store_.find(frame_id) == this->node_store_.end()) {
    latch_.unlock();
    return;
  }
  if (!this->node_store_.at(frame_id).EvictableTrue()) {
    latch_.unlock();
    BUSTUB_ASSERT("id {} :not evictable when trying to remove it", frame_id);
  }
  this->node_store_.erase(frame_id);
  this->curr_size_--;
  latch_.unlock();
}

auto LRUKReplacer::GetEvictableSize() -> size_t {
  latch_.lock();
  auto current_size = this->curr_size_;
  latch_.unlock();
  return current_size;
}

auto LRUKReplacer::GetSize() -> size_t {
  latch_.lock();
  auto current_size = this->node_store_.size();
  latch_.unlock();
  return current_size;
}

}  // namespace bustub
