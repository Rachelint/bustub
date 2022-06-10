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

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock lock(latch_);

  // if empty return false
  if (index_.empty()) {
    return false;
  }

  // if no empty
  //  find it, and set to output
  //  remove it from list_ and index_
  auto back = list_.back();
  *frame_id = back;
  index_.erase(back);
  list_.pop_back();

  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);

  // may victim, and pin what?
  if (index_.count(frame_id) != 0) {
    // remove it from list and index
    auto remove_iter = index_[frame_id];
    list_.erase(remove_iter);
    index_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);

  // unpin twice will not change anything
  if (0 == index_.count(frame_id)) {
    // add it to list_'s front and index_
    list_.push_front(frame_id);
    index_[frame_id] = list_.begin();
  }
}

size_t LRUReplacer::Size() {
  std::scoped_lock lock(latch_);

  return index_.empty() ? 0 : index_.size();
}

}  // namespace bustub
