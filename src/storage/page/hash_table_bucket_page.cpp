//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  assert(result != nullptr);

  bool ret = false;
  for (uint32_t scan_pos = 0; scan_pos < BUCKET_ARRAY_SIZE; scan_pos++) {
    if (!IsOccupied(scan_pos)) {
      break;
    }

    if (IsReadable(scan_pos)) {
      // not tomb check whether it is same as exist kv pair
      if (0 == cmp(key, array_[scan_pos].first)) {
        result->push_back(array_[scan_pos].second);
        ret = true;
      }
    }
  }
  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  // should not to consider latch here, add it in the hashtable layer
  // should not be full, because has check in caller
  // todo I think assert is ok, just for passing test
  if (IsFull()) {
    return false;
  }

  // support non-unique key, but (key, value) not allowed to be same
  // search an tombstone or empty as insert pos(tombstone first)
  // all these should be collected in one scan
  bool get_insert_pos = false;
  uint32_t insert_pos;
  for (uint32_t scan_pos = 0; scan_pos < BUCKET_ARRAY_SIZE; scan_pos++) {
    if (!IsOccupied(scan_pos)) {
      if (!get_insert_pos) {
        insert_pos = scan_pos;
        get_insert_pos = true;
      }
      break;
    }

    if (IsReadable(scan_pos)) {
      // not tomb check whether it is same as exist kv pair
      if (0 == cmp(key, array_[scan_pos].first) && value == array_[scan_pos].second) {
        return false;
      }
    } else {
      // tomb, reuse it
      insert_pos = scan_pos;
      get_insert_pos = true;
    }
  }
  // insert it, make pos valid(occupied and readable)
  InsertAt(insert_pos, key, value);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  if (IsEmpty()) {
    return false;
  }

  for (uint32_t scan_pos = 0; scan_pos < BUCKET_ARRAY_SIZE; scan_pos++) {
    if (!IsOccupied(scan_pos)) {
      break;
    }
    // if remove removed, return false
    if (IsReadable(scan_pos) && (0 == cmp(key, array_[scan_pos].first) && value == array_[scan_pos].second)) {
      RemoveAt(scan_pos);
      // can not remove a removed kv
      return true;
    }
  }

  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  readable_[bucket_idx / (8 * sizeof(char))] &= (static_cast<uint8_t>(-1) ^ (1 << (bucket_idx % (8 * sizeof(char)))));
  num_readable_ -= 1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::InsertAt(uint32_t bucket_idx, KeyType key, ValueType value) {
  SetOccupied(bucket_idx);
  SetReadable(bucket_idx);
  array_[bucket_idx] = std::make_pair(key, value);
  num_readable_ += 1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  return ((occupied_[bucket_idx / (8 * sizeof(char))]) & (1 << (bucket_idx % (8 * sizeof(char))))) > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  occupied_[bucket_idx / (8 * sizeof(char))] |= (1 << (bucket_idx % (8 * sizeof(char))));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  return ((readable_[bucket_idx / (8 * sizeof(char))]) & (1 << (bucket_idx % (8 * sizeof(char))))) > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  readable_[bucket_idx / (8 * sizeof(char))] |= (1 << (bucket_idx % (8 * sizeof(char))));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  return BUCKET_ARRAY_SIZE == num_readable_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  return num_readable_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  return num_readable_ == 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
