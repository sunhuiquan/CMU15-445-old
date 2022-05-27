//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  if (page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end())
    return false;                                                     // 无效 page_id 或未在 page_table_ 中跟踪
  if (pages_[page_table_[page_id]].GetPinCount() != 0) return false;  // 确保不是 Pin 状态

  if (pages_[page_table_[page_id]].IsDirty()) {  // 脏页，写回磁盘
    disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].GetData());
  }
  page_table_[page_id] = INVALID_PAGE_ID;  // 设置未使用标志
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  for (const auto &p : page_table_) {
    FlushPgImp(p.first);
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  frame_id_t frame_id = -1;
  for (const frame_id_t i : free_list_) {
    frame_id = i;
    break;
  }
  if (frame_id == -1 && !replacer_->Victim(&frame_id)) {
    return nullptr;
  }

  *page_id = AllocatePage();
  page_table_[*page_id] = frame_id;

  //     /** Zeroes out the data that is held within the page. */
  //   inline void ResetMemory() { memset(data_, OFFSET_PAGE_START, PAGE_SIZE); }

  //   /** The actual data that is stored within a page. */
  //   char data_[PAGE_SIZE]{};
  //   /** The ID of this page. */
  //   page_id_t page_id_ = INVALID_PAGE_ID;
  //   /** The pin count of this page. */
  //   int pin_count_ = 0;
  //   /** True if the page is dirty, i.e. it is different from its corresponding page on disk. */
  //   bool is_dirty_ = false;

  return &pages_[frame_id];
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  if (page_id == INVALID_PAGE_ID) return nullptr;

  if (page_table_.find(page_id) != page_table_.end()) {
    replacer_->Pin(page_table_[page_id]);
    return &pages_[page_table_[page_id]];
  }

  frame_id_t frame_id = -1;
  for (const frame_id_t i : free_list_) {
    frame_id = i;
    break;
  }
  if (frame_id == -1 && !replacer_->Victim(&frame_id)) {
    return nullptr;
  }
  if (pages_[page_table_[page_id]].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].GetData());
  }

  page_id_t new_page_id;
  Page *new_page = NewPgImp(&page_id);
  if (!new_page) return nullptr;

  disk_manager_->ReadPage(new_page_id, new_page->GetData());
  return new_page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  return false;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) { return false; }

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
