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

#include "common/logger.h"
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
  std::scoped_lock lock(latch_);

  return FlushPgImpInner(page_id);
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::scoped_lock lock(latch_);

  for (auto &pg_it : page_table_) {
    FlushPgImpInner(pg_it.first);
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock lock(latch_);

  // get a usable frame
  frame_id_t usable_fid = GetUsableFrame();
  if (-1 == usable_fid) {
    return nullptr;
  }

  // build map between page and frame
  page_id_t new_pg_id = AllocatePage();
  page_table_[new_pg_id] = usable_fid;

  // clean and set page
  Page *new_pg = &(pages_[usable_fid]);
  ResetPage(new_pg);
  new_pg->page_id_ = new_pg_id;
  new_pg->pin_count_ = 1;

  *page_id = new_pg->page_id_;
  return new_pg;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  std::scoped_lock lock(latch_);
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  // if exist
  if (page_table_.count(page_id) != 0) {
    // return straightly (inc rc)
    frame_id_t f_id = page_table_[page_id];
    Page *ret_pg = &(pages_[f_id]);
    ret_pg->pin_count_ += 1;

    // pin it, just remove from replacer
    replacer_->Pin(f_id);

    return ret_pg;
  }

  // if not exist
  // get a usable frame
  frame_id_t usable_f_id = GetUsableFrame();
  if (-1 == usable_f_id) {
    return nullptr;
  }

  // read page to frame, page's page_id shouldn't be updated by ReadPage
  Page *pg = &(pages_[usable_f_id]);
  ResetPage(pg);
  pg->page_id_ = page_id;
  disk_manager_->ReadPage(page_id, pg->data_);
  pg->pin_count_ += 1;

  // build map and rc
  page_table_[page_id] = usable_f_id;

  return pg;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::scoped_lock lock(latch_);

  // if not exist page return false
  if (0 == page_table_.count(page_id)) {
    return false;
  }

  // if exist page, rc dscr, set dirty(just allow false to true before being written back)
  frame_id_t f_id = page_table_[page_id];
  Page *pg = &(pages_[f_id]);
  pg->is_dirty_ = (pg->is_dirty_ ? pg->is_dirty_ : is_dirty);
  pg->pin_count_ -= 1;
  // it is impossible
  if (pg->pin_count_ < 0) {
    return false;
  }
  // if rc is zero, write the dirty, and frame should be placed into replacer
  if (0 == pg->pin_count_) {
    replacer_->Unpin(f_id);
  }

  return true;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::scoped_lock lock(latch_);

  if (0 == page_table_.count(page_id)) {
    return true;
  }

  frame_id_t f_id = page_table_[page_id];
  Page *pg = &(pages_[f_id]);
  if (pg->GetPinCount() > 0) {
    LOG_ERROR("pin count:%d", pg->GetPinCount());
    return false;
  }

  // remove from pg_tbl, replacer, disk
  page_table_.erase(page_id);
  replacer_->Pin(f_id);
  DeallocatePage(page_id);

  // clean and add to free_list()
  ResetPage(pg);
  free_list_.push_back(f_id);

  return true;
}

frame_id_t BufferPoolManagerInstance::GetUsableFrame() {
  // pool full
  if (free_list_.empty() && 0 == replacer_->Size()) {
    return -1;
  }

  // get frame to return
  // try to get from free_list
  frame_id_t ret_fid;
  if (!free_list_.empty()) {
    ret_fid = free_list_.front();
    free_list_.pop_front();
  } else {
    // + try to get from replacer
    // + if dirty, flush it to disk, only support this strategy
    // + remove the map in page_table
    // it is impossible to be false here
    assert(replacer_->Victim(&ret_fid));
    Page *pg = &(pages_[ret_fid]);
    page_id_t vict_pg_id = pg->GetPageId();
    if (pg->is_dirty_) {
      disk_manager_->WritePage(vict_pg_id, pg->GetData());
      pg->is_dirty_ = false;
    }
    page_table_.erase(vict_pg_id);
  }

  return ret_fid;
}

void BufferPoolManagerInstance::ResetPage(Page *pg) {
  // clean the page and return
  pg->ResetMemory();
  pg->page_id_ = INVALID_PAGE_ID;
  pg->is_dirty_ = false;
  pg->pin_count_ = 0;
}

bool BufferPoolManagerInstance::FlushPgImpInner(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  if (0 == page_table_.count(page_id)) {
    return false;
  }

  // As I think, flushing a clean page should not be an error
  Page *pg = &(pages_[page_table_[page_id]]);
  if (pg->IsDirty()) {
    disk_manager_->WritePage(page_id, pg->GetData());
    pg->is_dirty_ = false;
  }

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
