//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdlib>
#include <iostream>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {
/*****************************************************************************
 * Tools
 *****************************************************************************/
namespace {
void BuildMapAnd(HashTableDirectoryPage *dir_pg, page_id_t pg_id, const uint32_t least_ld_one_more_bit,
                 const uint32_t gd, const uint32_t ld, const bool dscr) {
  // new entrys
  std::queue<uint32_t> que;
  que.push(least_ld_one_more_bit);
  for (uint32_t d = ld; d < gd; d++) {
    uint32_t q_size = que.size();
    for (uint32_t i = 0; i < q_size; i++) {
      uint32_t dir_idx0 = que.front();
      que.pop();
      uint32_t dir_idx1 = dir_idx0 | (1 << d);
      que.push(dir_idx0);
      que.push(dir_idx1);
    }
  }

  // build map for new entries
  while (!que.empty()) {
    uint32_t dir_idx = que.front();
    que.pop();
    dir_pg->SetBucketPageId(dir_idx, pg_id);
    if (dscr) {
      dir_pg->DecrLocalDepth(dir_idx);
    } else {
      dir_pg->IncrLocalDepth(dir_idx);
    }
  }
}

void BuildMapAndIncr(HashTableDirectoryPage *dir_pg, page_id_t pg_id, const uint32_t least_ld_one_more_bit,
                     const uint32_t gd, const uint32_t ld) {
  BuildMapAnd(dir_pg, pg_id, least_ld_one_more_bit, gd, ld, false);
}

void BuildMapAndDscr(HashTableDirectoryPage *dir_pg, page_id_t pg_id, const uint32_t least_ld_one_more_bit,
                     const uint32_t gd, const uint32_t ld) {
  BuildMapAnd(dir_pg, pg_id, least_ld_one_more_bit, gd, ld, true);
}

struct WrapRwLatch {
  explicit WrapRwLatch(ReaderWriterLatch *rwlatch) : rwlatch_(rwlatch) {}

  void RLatch() { rwlatch_->RLock(); }

  void RUnlatch() { rwlatch_->RUnlock(); }

  void WLatch() { rwlatch_->WLock(); }

  void WUnlatch() { rwlatch_->WUnlock(); }

  ReaderWriterLatch *rwlatch_;
};

struct TableRlatch {
  explicit TableRlatch(ReaderWriterLatch *rwlatch) : rwlatch_(rwlatch) { rwlatch_->RLock(); }

  ~TableRlatch() { rwlatch_->RUnlock(); }

  ReaderWriterLatch *rwlatch_;
};

struct TableWlatch {
  explicit TableWlatch(ReaderWriterLatch *rwlatch) : rwlatch_(rwlatch) { rwlatch_->WLock(); }

  ~TableWlatch() { rwlatch_->WUnlock(); }

  ReaderWriterLatch *rwlatch_;
};

struct BuckRlatch {
  explicit BuckRlatch(Page *rwlatch) : rwlatch_(rwlatch) { rwlatch_->RLatch(); }

  ~BuckRlatch() { rwlatch_->RUnlatch(); }

  Page *rwlatch_;
};

struct BuckWlatch {
  explicit BuckWlatch(Page *rwlatch) : rwlatch_(rwlatch) { rwlatch_->WLatch(); }

  ~BuckWlatch() { rwlatch_->WUnlatch(); }

  Page *rwlatch_;
};
}  // namespace

#define FETCH_DIR_PAGE(dir_pg)          \
  /* NOLINTNEXTLINE */                  \
  auto dir_pg = FetchDirectoryPage();   \
  if (nullptr == (dir_pg)) {            \
    LOG_ERROR("fetch dir_page failed"); \
    return false;                       \
  }

#define FETCH_BUCK_PAGE(buck_pg, buck_pg_id, key, dir_pg) \
  page_id_t buck_pg_id = KeyToPageId(key, dir_pg);        \
  /* NOLINTNEXTLINE */                                    \
  auto buck_pg = FetchBucketPage(buck_pg_id);             \
  if (nullptr == (buck_pg)) {                             \
    LOG_ERROR("fetch buck_page failed");                  \
    return false;                                         \
  }

#define UNPIN_PAGE(pg_id, is_dirty) assert(buffer_pool_manager_->UnpinPage(pg_id, is_dirty))
#define DELETE_PAGE(pg_id) assert(buffer_pool_manager_->DeletePage(pg_id))

#define TABLE_RLATCH(rwlatch) TableRlatch rl(&(rwlatch))
#define TABLE_WLATCH(rwlatch) TableWlatch wl(&(rwlatch))
#define BUCK_RLATCH(buck) BuckRlatch rl(reinterpret_cast<Page *>(buck))
#define BUCK_WLATCH(buck) BuckWlatch wl(reinterpret_cast<Page *>(buck))

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  Page *pg = buffer_pool_manager_->FetchPage(directory_page_id_);
  if (nullptr == pg) {
    return nullptr;
  }
  return reinterpret_cast<HashTableDirectoryPage *>(pg->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page *pg = buffer_pool_manager_->FetchPage(bucket_page_id);
  if (nullptr == pg) {
    return nullptr;
  }
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(pg->GetData());
}

/*****************************************************************************
 * BUILD
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  // the page_id should be passed by catalog? but in this project, we just allocate a new dir page every time.
  // LOG_DEBUG("hashtable create");
  auto raw_dir_pg = buffer_pool_manager_->NewPage(&directory_page_id_);
  assert(nullptr != raw_dir_pg);
  page_id_t first_buck_pid;
  assert(nullptr != buffer_pool_manager_->NewPage(&first_buck_pid));
  UNPIN_PAGE(directory_page_id_, false);
  UNPIN_PAGE(first_buck_pid, false);

  // set the dir_page
  auto dir_pg = reinterpret_cast<HashTableDirectoryPage *>(raw_dir_pg->GetData());
  dir_pg->SetPageId(directory_page_id_);
  dir_pg->SetLocalDepth(0, 0);
  dir_pg->SetBucketPageId(0, first_buck_pid);
  dir_pg->SetLSN(0);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  if (nullptr == result) {
    LOG_ERROR("result is null");
    return false;
  }

  TABLE_RLATCH(table_latch_);
  FETCH_DIR_PAGE(dir_pg);
  FETCH_BUCK_PAGE(buck_pg, buck_pg_id, key, dir_pg);
  bool ret;
  {
    BUCK_RLATCH(buck_pg);
    ret = buck_pg->GetValue(key, comparator_, result);
  }
  UNPIN_PAGE(directory_page_id_, false);
  UNPIN_PAGE(buck_pg_id, false);
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  {
    TABLE_RLATCH(table_latch_);
    FETCH_DIR_PAGE(dir_pg);
    FETCH_BUCK_PAGE(buck_pg, buck_pg_id, key, dir_pg);

    {
      BUCK_WLATCH(buck_pg);
      if (!buck_pg->IsFull()) {
        if (!buck_pg->Insert(key, value, comparator_)) {
          UNPIN_PAGE(buck_pg_id, false);
          UNPIN_PAGE(directory_page_id_, false);
          return false;
        }
        UNPIN_PAGE(buck_pg_id, true);
        UNPIN_PAGE(directory_page_id_, false);
        return true;
      }
    }

    // split insert
    UNPIN_PAGE(buck_pg_id, false);
    UNPIN_PAGE(directory_page_id_, false);
  }

  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  TABLE_WLATCH(table_latch_);
  bool ret = true;
  FETCH_DIR_PAGE(dir_pg);

  // +check whether same kv
  bool non_exist = true;
  FETCH_BUCK_PAGE(buck_pg, buck_pg_id, key, dir_pg);
  for (uint32_t i = 0; i < buck_pg->Capacity(); i++) {
    if ((0 == comparator_(buck_pg->KeyAt(i), key)) && (buck_pg->ValueAt(i) == value)) {
      non_exist = false;
      break;
    }
  }
  UNPIN_PAGE(buck_pg_id, true);
  if (!non_exist) {
    UNPIN_PAGE(directory_page_id_, false);
    return false;
  }

  // +create and build map, should loop
  while (true) {
    FETCH_BUCK_PAGE(buck_pg, buck_pg_id, key, dir_pg);
    BUCK_WLATCH(buck_pg);
    // can inserted
    if (!buck_pg->IsFull()) {
      if (!buck_pg->Insert(key, value, comparator_)) {
        LOG_ERROR("insert after split failed");
        ret = false;
      }
      UNPIN_PAGE(buck_pg_id, ret);
      break;
    }
    // split
    if (!SplitOnce(key, dir_pg, buck_pg)) {
      ret = false;
      UNPIN_PAGE(buck_pg_id, ret);
      break;
    }

    UNPIN_PAGE(buck_pg_id, true);
  }

  UNPIN_PAGE(directory_page_id_, true);
  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitOnce(const KeyType &key, HashTableDirectoryPage *dir_pg, HASH_TABLE_BUCKET_TYPE *buck_pg) {
  if (dir_pg->IsFull()) {
    return false;
  }

  // +prepare ctx
  uint32_t gd = dir_pg->GetGlobalDepth();
  uint32_t buck_idx = KeyToDirectoryIndex(key, dir_pg);
  page_id_t buck_pg_id = KeyToPageId(key, dir_pg);
  uint32_t ld = dir_pg->GetLocalDepth(buck_idx);
  assert(gd >= ld);
  // new a page
  page_id_t new_pg_id;
  auto new_pg = buffer_pool_manager_->NewPage(&new_pg_id);
  if (nullptr == new_pg) {
    LOG_ERROR("new buck page failed");
    return false;
  }
  auto new_buck_pg = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(new_pg->GetData());

  // +dir_pg's grow
  if (gd == ld) {
    // LOG_DEBUG("dir need to grow, gd:%d, ld:%d", gd, ld);
    // the map rebuild logic is wrapped in this
    dir_pg->IncrGlobalDepth();
    gd += 1;
  }

  // +buck's split
  // get least ld bit
  uint32_t least_ld_bit = Hash(key) & dir_pg->GetLocalDepthMask(buck_idx);

  // rebuild map
  ld += 1;
  page_id_t pg_id0 = buck_pg_id;
  page_id_t pg_id1 = new_pg_id;
  BuildMapAndIncr(dir_pg, pg_id0, least_ld_bit, gd, ld);
  BuildMapAndIncr(dir_pg, pg_id1, least_ld_bit | (1 << (ld - 1)), gd, ld);

  // + data move
  // should split data, use RemoveAt and InsertAt
  // Remove value in page0
  // insert the removed one into page1
  for (uint32_t i = 0, j = 0; i < buck_pg->Capacity(); i++) {
    auto key = buck_pg->KeyAt(i);
    auto value = buck_pg->ValueAt(i);
    if ((KeyToDirectoryIndex(key, dir_pg) & (1 << (ld - 1))) > 0) {
      buck_pg->RemoveAt(i);
      new_buck_pg->InsertAt(j, key, value);
      j++;
    }
  }

  UNPIN_PAGE(pg_id1, true);

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  bool empty;
  {
    TABLE_RLATCH(table_latch_);
    // +remove entry
    FETCH_DIR_PAGE(dir_pg);
    FETCH_BUCK_PAGE(buck_pg, buck_pg_id, key, dir_pg);

    {
      BUCK_WLATCH(buck_pg);
      if (!buck_pg->Remove(key, value, comparator_)) {
        // LOG_ERROR("remove from buck failed");
        UNPIN_PAGE(buck_pg_id, false);
        UNPIN_PAGE(directory_page_id_, false);
        return false;
      }
      empty = buck_pg->IsEmpty();
    }

    UNPIN_PAGE(buck_pg_id, true);
    UNPIN_PAGE(directory_page_id_, false);
  }

  // merge
  if (empty) {
    return Merge(transaction, key, value);
  }
  return true;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  TABLE_WLATCH(table_latch_);
  // get all buck_idx, img_buck_idx, img_pg_id
  FETCH_DIR_PAGE(dir_pg);
  while (true) {
    // get image and check
    // reverse the ld_th bit of origin buck idx
    uint32_t buck_idx = KeyToDirectoryIndex(key, dir_pg);
    uint32_t ld = dir_pg->GetLocalDepth(buck_idx);
    if (0 == ld) {
      break;
    }
    uint32_t buck_high_bit = dir_pg->GetLocalHighBit(buck_idx);
    // for example, ???|1101 -> ???|0101
    uint32_t img_buck_idx = buck_idx ^ buck_high_bit;
    page_id_t pg_id = dir_pg->GetBucketPageId(buck_idx);
    page_id_t img_pg_id = dir_pg->GetBucketPageId(img_buck_idx);
    uint32_t img_ld = dir_pg->GetLocalDepth(img_buck_idx);

    FETCH_BUCK_PAGE(buck_pg, buck_pg_id, key, dir_pg);
    {
      BUCK_RLATCH(buck_pg);
      assert(buck_pg_id == pg_id);
      if (!buck_pg->IsEmpty() || ld != img_ld) {
        UNPIN_PAGE(buck_pg_id, false);
        break;
      }
    }
    UNPIN_PAGE(buck_pg_id, false);

    // rebuild and dscr
    uint32_t proto_buck_idx = buck_idx & dir_pg->GetLocalDepthMask(buck_idx);
    uint32_t img_proto_buck_idx = proto_buck_idx ^ buck_high_bit;
    BuildMapAndDscr(dir_pg, img_pg_id, proto_buck_idx, dir_pg->GetGlobalDepth(), ld);
    BuildMapAndDscr(dir_pg, img_pg_id, img_proto_buck_idx, dir_pg->GetGlobalDepth(), ld);

    // shrink the global
    if (dir_pg->CanShrink()) {
      dir_pg->DecrGlobalDepth();
    }
    // clean
    DELETE_PAGE(pg_id);
  }

  UNPIN_PAGE(directory_page_id_, true);

  return true;
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
