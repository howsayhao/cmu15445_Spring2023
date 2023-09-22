//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator(std::string name,  BufferPoolManager *buffer_pool_manager,
                const KeyComparator &comparator, page_id_t curr_page_id, int curr_slot, KeyType curr_key, ValueType curr_val, MappingType &curr_map,
                int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);
  explicit IndexIterator(const KeyComparator &comparator);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;
  auto IsEmpty() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool { return curr_page_id_==itr.curr_page_id_ && curr_slot_==itr.curr_slot_; }

  auto operator!=(const IndexIterator &itr) const -> bool { return curr_page_id_!=itr.curr_page_id_ || curr_slot_!=itr.curr_slot_; }

 private:
  // 全局信息
  std::string iterator_name_;
  BufferPoolManager *bpm_;
  KeyComparator comparator_;
  // 当前信息
  page_id_t curr_page_id_;
  int curr_slot_;
  KeyType curr_key_;
  ValueType curr_val_;
  MappingType curr_map_;
  // 查询信息
  int leaf_max_size_;
  int internal_max_size_;
};

}  // namespace bustub
