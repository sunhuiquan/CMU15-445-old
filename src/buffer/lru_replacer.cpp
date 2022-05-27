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

LRUReplacer::LRUReplacer(size_t num_pages) { replacer_size_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  auto end = id_list_.crend();
  for (auto it = id_list_.crbegin(); it != end; ++it) {
    if (it->second == false) {
      *frame_id = it->first;
      return true;
    }
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  // ???
  if (!id_map_.count(frame_id)) {
    id_list_.push_front(std::pair<int, bool>{frame_id, true});
    id_map_[frame_id] = id_list_.front();
    ++replacer_size_;
  } else {
    id_list_.remove(id_map_[frame_id]);
    id_list_.push_front(std::pair<int, bool>{frame_id, true});
    id_map_[frame_id] = id_list_.front();
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (id_map_.count(frame_id)) {
    id_map_[frame_id].second = false;
  }
}

size_t LRUReplacer::Size() {
  int count = 0;
  auto end = id_list_.crend();
  for (auto it = id_list_.crbegin(); it != end; ++it) {
    if (it->second == false) {
      ++count;
    }
  }
  return count;
}

}  // namespace bustub
