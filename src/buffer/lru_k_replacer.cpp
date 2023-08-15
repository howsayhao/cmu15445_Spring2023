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

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  bool inf = false;
  bool evict_true = false;
  size_t max_time_stamp = 0;
  frame_id_t frame_to_evict{0}; // 没有意义的初始化0，所以需要evict_true辅助
  for (auto const &it : this->node_store_) { // it扫到后是pair类型
    if (it.second.EvictableTrue()) {
      evict_true = true;
      if (it.second.HistoryEntry() < this->k_) { // Classical LRU
        if (not (inf && max_time_stamp>=(this->current_timestamp_ - it.second.History().back()))) {
          inf = true;
          max_time_stamp = this->current_timestamp_ - it.second.History().back();
          frame_to_evict = it.first;
        }
      } else if (!inf) { // LRU-K
        auto count = this->k_;
        size_t kth_stamp = 0;
        auto iterator = it.second.History();
        for (auto kth = iterator.rbegin(); count>0; kth++,count--) {
          if (count == 1) {
            kth_stamp = *kth;
          }
        }
        if (max_time_stamp < (this->current_timestamp_ - kth_stamp)) {
          max_time_stamp = this->current_timestamp_ - kth_stamp;
          frame_to_evict = it.first;
        }
      }
    }
  }
  if (not evict_true) {
    return false;
  }
  *frame_id = frame_to_evict;
  this->node_store_.erase(frame_to_evict);
  this->curr_size_ --;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (static_cast<size_t>(frame_id) > this->replacer_size_ || frame_id <= 0) {
    BUSTUB_ASSERT("id {} :out of replacer_size_ range", frame_id);
  }
  if (this->node_store_.find(frame_id) == this->node_store_.end()) {
    this->node_store_.insert(std::make_pair(frame_id, LRUKNode()));
  }
  this->node_store_.at(frame_id).Access(this->current_timestamp_);
  this->current_timestamp_ ++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (static_cast<size_t>(frame_id) > this->replacer_size_ || frame_id <= 0) {
    BUSTUB_ASSERT("id {} :out of replacer_size_ range", frame_id);
  }
  if (this->node_store_.find(frame_id) == this->node_store_.end()) {
    BUSTUB_ASSERT("id {} :no such frame yet", frame_id);
  }
  auto tmp = & this->node_store_.at(frame_id);
  if (set_evictable && not tmp->EvictableTrue()) {
    tmp->VerseEvictable();
    this->curr_size_ ++;
  } else if (not set_evictable && tmp->EvictableTrue()) {
    tmp->VerseEvictable();
    this->curr_size_ --;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (static_cast<size_t>(frame_id) > this->replacer_size_ || frame_id <= 0) {
    BUSTUB_ASSERT("id {} :out of replacer_size_ range", frame_id);
  }
  if (this->node_store_.find(frame_id) == this->node_store_.end()) {
    return ;
  }
  if (not this->node_store_.at(frame_id).EvictableTrue()) {
    BUSTUB_ASSERT("id {} :not evictable when trying to remove it", frame_id);
  }
  this->node_store_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return this->curr_size_; }

}  // namespace bustub
