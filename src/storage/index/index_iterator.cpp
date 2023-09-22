/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(std::string name,  BufferPoolManager *buffer_pool_manager,
                                  const KeyComparator &comparator, page_id_t curr_page_id, int curr_slot, KeyType curr_key, ValueType curr_val, MappingType &curr_map,
                                  int leaf_max_size, int internal_max_size)
    : iterator_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      curr_page_id_(curr_page_id),
      curr_slot_(curr_slot),
      curr_key_(curr_key),
      curr_val_(curr_val),
      curr_map_(curr_map),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
}
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(const KeyComparator &comparator) : comparator_(std::move(comparator)) {
  iterator_name_ = "is an empty iterator";
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEmpty() -> bool {
  return iterator_name_ == "is an empty iterator";
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { 
  // 这里IsEnd的意思应该是最后一个slot，而不是null
  if (IsEmpty()) {
    return false;
  }
  ReadPageGuard guard = bpm_->FetchPageRead(curr_page_id_);
  auto leaf_page = guard.As<LeafPage>();
  return  leaf_page->GetNextPageId() == INVALID_PAGE_ID && curr_slot_ == leaf_page->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { 
  curr_map_.first = curr_key_;
  curr_map_.second = curr_val_;
  return std::move(curr_map_); 
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & { 
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(std::move(comparator_);
  }
  ReadPageGuard guard = bpm_->FetchPageRead(curr_page_id_);
  auto leaf_page = guard.As<LeafPage>();
  // 更新所有需要更新的数据
  page_id_t next_page_id = curr_page_id_;
  int next_slot = curr_slot_;
  KeyType next_key;
  ValueType next_val;
  MappingType next_map;
  if (curr_slot_ == leaf_page->GetSize()) { // 需要更新新的page
    next_page_id = leaf_page->GetNextPageId();
    next_slot = 0;
  } else {
    next_slot ++;
  }
  guard = bpm_->FetchPageRead(curr_page_id_);
  auto next_leaf_page = guard.As<LeafPage>();
  next_key = next_leaf_page->KeyAt(next_slot);
  next_val = next_leaf_page->ValueAt(next_slot);
  MappingType next_map = {next_key, next_val};
  // 返回新的iterator
  return INDEXITERATOR_TYPE("next_iterator", bpm_, comparator_, next_page_id, next_slot, next_key, next_val, next_map, leaf_max_size_, internal_max_size_);
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
