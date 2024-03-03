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
#include <string>
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                page_id_t curr_page_id, int curr_slot, KeyType curr_key, ValueType curr_val, MappingType &curr_map,
                int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);
  explicit IndexIterator(const KeyComparator &comparator);
  ~IndexIterator();  // NOLINT

  auto operator=(const INDEXITERATOR_TYPE &other) -> INDEXITERATOR_TYPE & {
    if (this != &other) {
      // 处理对象成员的赋值
      iterator_name_ = other.iterator_name_;
      // bpm_ = other.bpm_;
      // comparator_ = other.comparator_;
      curr_page_id_ = other.curr_page_id_;
      curr_slot_ = other.curr_slot_;
      curr_key_ = other.curr_key_;
      curr_val_ = other.curr_val_;
      curr_map_ = other.curr_map_;
      leaf_max_size_ = other.leaf_max_size_;
      internal_max_size_ = other.internal_max_size_;
    }
    return *this;
  }  // 实现来自gpt，以满足index_scan_executor对迭代器重定义init的要求

  auto IsEnd() const -> bool;
  void SetEnd();
  // auto IsEmpty() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    if (this->IsEnd() || itr.IsEnd()) {
      // std::cout << "operator==" << this->IsEnd() << " " << itr.IsEnd() << std::endl;
      return itr.IsEnd() && this->IsEnd();
    }
    return curr_page_id_ == itr.curr_page_id_ && curr_slot_ == itr.curr_slot_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return !(*this == itr);
    // return curr_page_id_!=itr.curr_page_id_ || curr_slot_!=itr.curr_slot_;
  }

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
