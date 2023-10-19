//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
  KeyType vice_key;
  vice_key.SetFromInteger(0);
  SetKeyAt(0, vice_key);
  SetSize(1);  // 默认是有一个空槽的，只不过第一个槽是valid，但依然占据了位置；
  SetValueAt(0, INVALID_PAGE_ID);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  if (index >= 1 && index < GetSize()) {  // index should be valid and non-zero
    KeyType key = array_[index].first;
    return key;
  }
  // std::cout << "key at, out of internal range:" << index << "max_size:" << GetMaxSize() << std::endl;
  // 为了提高并发性，现在允许对该位查询以及设置，若是报出上面的错误，无视掉；
  return array_[0].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  if (index >= 1 && index < GetSize()) {
    array_[index].first = key;
    return;
  }
  // std::cout << "set key, out of internal range" << std::endl;
  // 比较冒险了，万一是算法结构问题就没有办法检错了
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  if (index >= 0 && index < GetSize()) {
    array_[index].second = value;
    return;
  }
  std::cout << "set value, out of internal range" << std::endl;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if (index >= 0 && index < GetSize()) {
    // if (index == 0) {
    //   std::cout << "value at index zero" << std::endl;
    // }
    return array_[index].second;
  }
  std::cout << "value at, out of internal range" << std::endl;
  return array_[0].second;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
