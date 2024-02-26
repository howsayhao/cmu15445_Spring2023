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
#include <cstdint>
#include <iostream>
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  if (curr_size_ == 0) {
    latch_.unlock();
    return false;
  }
  size_t lruk_stamp = SIZE_MAX;
  size_t timestamp = SIZE_MAX;
  frame_id_t frame_to_evict = INT32_MIN;
  for (auto &kv : node_store_) {
    if (kv.second.is_evictable_) {
      if (kv.second.history_.size() < k_) {
        lruk_stamp = INT32_MIN;
        if (kv.second.history_.back() < timestamp) {
          timestamp = kv.second.history_.back();
          frame_to_evict = kv.first;
        }
      } else {
        if (kv.second.history_.back() < lruk_stamp) {
          lruk_stamp = kv.second.history_.back();
          frame_to_evict = kv.first;
        }
      }
    }
  }

  if (frame_to_evict == INT32_MIN) {
    latch_.unlock();
    return false;
  }
  node_store_.erase(frame_to_evict);
  *frame_id = frame_to_evict;
  curr_size_--;
  latch_.unlock();
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (static_cast<size_t>(frame_id) >= replacer_size_ || frame_id < 0) {
    BUSTUB_ASSERT("id {} :out of replacer_size_ range", frame_id);
  }
  latch_.lock();
  current_timestamp_++;
  if (this->node_store_.find(frame_id) == this->node_store_.end()) {
    node_store_[frame_id] = LRUKNode{{current_timestamp_}, k_, frame_id, false};
  } else {
    node_store_[frame_id].history_.push_front(current_timestamp_);
    if (node_store_[frame_id].history_.size() > k_) {
      node_store_[frame_id].history_.pop_back();
    }
  }
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (static_cast<size_t>(frame_id) >= replacer_size_ || frame_id < 0) {
    BUSTUB_ASSERT("id {} :out of replacer_size_ range", frame_id);
  }
  latch_.lock();
  if (node_store_.find(frame_id) != node_store_.end()) {
    if (node_store_[frame_id].is_evictable_ && !set_evictable) {
      node_store_[frame_id].is_evictable_ = false;
      curr_size_--;
    } else if (!node_store_[frame_id].is_evictable_ && set_evictable) {
      node_store_[frame_id].is_evictable_ = true;
      curr_size_++;
    }
  } else {
    latch_.unlock();
    throw ExecutionException("frame is not in node_store_.");
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  if (static_cast<size_t>(frame_id) >= replacer_size_ || frame_id < 0) {
    latch_.unlock();
    BUSTUB_ASSERT("id {} :out of replacer_size_ range", frame_id);
  }
  if (node_store_.find(frame_id) == node_store_.end()) {
    latch_.unlock();
    return;
  }
  if (!node_store_[frame_id].is_evictable_) {
    latch_.unlock();
    throw ExecutionException("frame is not evictable.");
  }
  node_store_.erase(frame_id);
  curr_size_--;
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock scoped_latch(latch_);
  return curr_size_;
}

}  // namespace bustub
