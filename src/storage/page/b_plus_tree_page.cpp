//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
auto BPlusTreePage::IsLeafPage() const -> bool { return (page_type_ == IndexPageType::LEAF_PAGE); }
void BPlusTreePage::SetPageType(IndexPageType page_type) { page_type_ = page_type; }

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
auto BPlusTreePage::GetSize() const -> int { return size_; }
void BPlusTreePage::SetSize(int size) { size_ = size; }
void BPlusTreePage::IncreaseSize(int amount) { size_ = size_ + amount; }

/*
 * Helper methods to get/set max size (capacity) of the page
 */
auto BPlusTreePage::GetMaxSize() const -> int {
  // 在给的演示中，inter-node和leaf-node都达不到max_size，前者因为有个空槽占了位置
  // 暂时不考虑，因为我看test文件里可以会设置leaf=inter-1，反正也方便改
  // if (IsLeafPage()) {
  //   return max_size_-1;
  // }
  return max_size_;
}
void BPlusTreePage::SetMaxSize(int size) { max_size_ = size; }

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
auto BPlusTreePage::GetMinSize() const -> int {
  // 这里默认是取下整，另外因为inter-node有一个空槽，所以会出问题，得加个1(取上整的意思)
  if (IsLeafPage()) {
    return (max_size_ / 2 > 0) ? max_size_ / 2 : 1;
  }
  return ((max_size_ + 1) / 2 > 1) ? (max_size_ + 1) / 2 : 2;  // max_size不会<=2，那个网站里设为2都会让你重新输的
}  // not ceiling?

}  // namespace bustub
