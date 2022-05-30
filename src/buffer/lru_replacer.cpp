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
  std::lock_guard<std::mutex> lock(replacer_mutex_);
  if (id_list_.empty()) {
    return false;
  }

  *frame_id = id_list_.back();
  id_list_.pop_back();
  id_map_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(replacer_mutex_);
  if (id_map_.find(frame_id) != id_map_.end()) {
    id_list_.erase(id_map_[frame_id]);
    id_map_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(replacer_mutex_);
  if (id_list_.size() == max_pages_) {  // replacer 已满
    int id = id_list_.back();
    id_list_.erase(id_map_[id]);
    id_map_.erase(id);
  }

  if (id_map_.find(frame_id) == id_map_.end()) {
    id_list_.push_front(frame_id);
    id_map_[frame_id] = id_list_.cbegin();
  }
}

size_t LRUReplacer::Size() { return id_list_.size(); }

}  // namespace bustub
