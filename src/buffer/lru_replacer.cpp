//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { max_size_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock clock_lock{latch_};
  if (this->Size() == 0) {
    return false;
  }
  *frame_id = lru_queue_.front();
  lru_hash_.erase(*frame_id);
  lru_queue_.pop_front();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock clock_lock{latch_};
  if (lru_hash_.count(frame_id) != 0) {
    lru_queue_.erase(lru_hash_[frame_id]);
    lru_hash_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock clock_lock{latch_};
  if (lru_hash_.count(frame_id) == 0) {
    lru_queue_.push_back(frame_id);
    lru_hash_[frame_id] = prev(lru_queue_.end());
  }
}

size_t LRUReplacer::Size() { return lru_queue_.size(); }

}  // namespace bustub
