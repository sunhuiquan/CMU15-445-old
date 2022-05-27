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
  if (id_list_.empty()) {
    return false;
  }
  *frame_id = id_list_.back();
  id_list_.remove(*frame_id);
  id_map_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (id_map_.count(frame_id)) {
    id_list_.remove(frame_id);
    id_map_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (num_pages_ == max_pages_) return;  // todo: replacer is already full
  if (!id_map_.count(frame_id)) {
    id_list_.push_front(frame_id);
    id_map_[frame_id] = id_list_.front();
    ++num_pages_;
  }
}

size_t LRUReplacer::Size() { return id_list_.size(); }

}  // namespace bustub
