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
    return false;  // 无效 page_id 或未在 page_table_ 中跟踪

  if (pages_[page_table_[page_id]].IsDirty())  // 脏页，写回磁盘
    disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].GetData());
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
    free_list_.remove(i);
    break;
  }
  if (frame_id == -1) {
    if (!replacer_->Victim(&frame_id))
      return nullptr;
    else {
      LOG_DEBUG("pagetable: %d\n", pages_[frame_id].GetPageId());
      page_table_.erase(pages_[frame_id].GetPageId());
      if (pages_[frame_id].IsDirty()) {
        disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      }
    }
  }
  pages_[frame_id].ClearPage();  // 初始化清零
  pages_[frame_id].IncreasePinCount();

  *page_id = AllocatePage();  // 获取 page_id
  page_table_[*page_id] = frame_id;
  pages_[frame_id].page_id_ = *page_id;
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
    pages_[page_table_[page_id]].IncreasePinCount();
    return &pages_[page_table_[page_id]];
  }

  //   LOG_DEBUG("fetch page_id: %d\n", page_id);

  page_id_t new_page_id;
  Page *new_page = NewPgImp(&new_page_id);  // 这里面已经 IncreasePinCount，因为单纯的 NewPage 也要 Pin，避免 Pin 两次
  if (!new_page) return nullptr;
  disk_manager_->ReadPage(page_id, new_page->GetData());

  return new_page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  if (page_table_.find(page_id) == page_table_.end()) return true;
  Page &page = pages_[page_table_[page_id]];
  if (page.GetPinCount() != 0) return false;

  if (pages_[page_table_[page_id]].IsDirty())
    disk_manager_->WritePage(pages_[page_table_[page_id]].GetPageId(), pages_[page_table_[page_id]].GetData());

  free_list_.push_back(page_table_[page_id]);
  page_table_.erase(page_id);

  // 不用清空，因为 NewPgImp 新分配时会自动清空

  DeallocatePage(page_id);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  if (page_table_.find(page_id) == page_table_.end()) return true;
  Page &page = pages_[page_table_[page_id]];
  if (page.GetPinCount() <= 0) return false;

  page.DecreasePinCount();
  if (is_dirty) page.SetDirty();
  if (!page.GetPinCount()) replacer_->Unpin(page_table_[page_id]);
  return true;
}

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
