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

LRUReplacer::LRUReplacer(size_t num_pages) { max_pages_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  //   replacer_mutex_.lock();
  if (id_list_.empty()) {
    // replacer_mutex_.unlock();
    return false;
  }

  *frame_id = id_list_.back();
  id_list_.erase(id_map_[*frame_id]);
  id_map_.erase(*frame_id);
  //   replacer_mutex_.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  //   replacer_mutex_.lock();
  if (id_map_.count(frame_id)) {
    id_list_.erase(id_map_[frame_id]);
    id_map_.erase(frame_id);
  }
  //   replacer_mutex_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  //   replacer_mutex_.lock();
  if (num_pages_ == max_pages_) {  // todo: replacer is already full
    // replacer_mutex_.unlock();
    return;
  }

  if (!id_map_.count(frame_id)) {
    id_list_.push_front(frame_id);
    id_map_[frame_id] = id_list_.cbegin();
    ++num_pages_;
  }
  //   replacer_mutex_.unlock();
}

size_t LRUReplacer::Size() {
  //   replacer_mutex_.lock();
  int sz = id_list_.size();
  //   replacer_mutex_.unlock();
  return sz;
}

}  // namespace bustub
