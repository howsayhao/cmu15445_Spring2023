#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"
#include <typeinfo>

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (root_ == nullptr) {
    return nullptr;
  }
  auto rnode = &root_;
  for (char it_key : key) {
    auto it_find = (*rnode)->children_.find(it_key);
    if (it_find != (*rnode)->children_.end()) {
      rnode = &((*rnode)->children_.at(it_key));
    } else {
      return nullptr;
    }
  }
  if ((*rnode)->is_value_node_) {
    if (dynamic_cast<const TrieNodeWithValue<T> *>((*rnode).get()) == nullptr) {  // 这一步不理解
      return nullptr;
    }
    return dynamic_cast<const TrieNodeWithValue<T> *>((*rnode).get())->value_.get();
  }
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  auto new_root = std::make_unique<TrieNode>(TrieNode(std::map<char, std::shared_ptr<const TrieNode>>()));
  if (root_ != nullptr) {  // 需要保证new_root非空，若root_非空，则原先make_unique的指针会释放内存，没有副作用
    new_root = root_->Clone();
  }
  auto value_ptr = std::make_shared<T>(std::move(value));
  if (key.empty()) { // 根结点可能存值
    auto tmp = std::make_unique<TrieNodeWithValue<T>>(\
              TrieNodeWithValue(new_root->children_, std::move(value_ptr)));
    return Trie(std::move(tmp));
  }
  std::unique_ptr<TrieNode> vec = nullptr;
  std::stack<TrieNode> 
  // std::unique_ptr<TrieNodeWithValue<T>> leaf = nullptr;

  // 先处理掉棘手的第一个字符，因为new_root是个unique_ptr，而后续的children都是shared_ptr
  auto it_key = key.begin();
  if (key.length() == 1) { 
    vec = std::make_unique<TrieNodeWithValue<T>>(TrieNodeWithValue(std::map<char, std::shared_ptr<const TrieNode>>(), std::move(value_ptr)));
    new_root->children_.insert(std::make_pair(*it_key, std::move(vec)));
    return Trie(std::shared_ptr<const TrieNode>(std::move(new_root)));
  } 
  vec = std::make_unique<TrieNode>(TrieNode(std::map<char, std::shared_ptr<const TrieNode>>()));
  auto it_find = new_root->children_.find(*it_key);
  if (it_find != new_root->children_.end()) {
    new_root->children_.at(*it_key) = std::move(vec);
  } else {
    new_root->children_.insert(std::make_pair(*it_key, std::move(vec)));
  }

  std::shared_ptr<TrieNode> parent = new_root->children_.at(*it_key); // 第一个字符的结点
  it_key ++;
  // if (new_root->children_.size() != 1 && parent->children_.size() != 1) {
  //   new_root = std::make_unique<TrieNode>(TrieNode(std::map<char, std::shared_ptr<const TrieNode>>()));
  // }
  vec = std::make_unique<TrieNodeWithValue<T>>(TrieNodeWithValue(std::map<char, std::shared_ptr<const TrieNode>>(), std::move(value_ptr)));
  parent->children_.insert(std::make_pair(*it_key, std::move(vec)));

  // // 遍历原Tire树，直到路径结束或没有结点延续，对直接相关支路结点均Clone，并修改上级结点的map信息
  // // for (; it_key + 1 != key.end(); ++it_key) {  // 假定至少有一个字符，遍历到倒数第二个字符为止
  // //   auto it_find = parent->children_.find(*it_key);
  // //   if (it_find != parent->children_.end()) {
  // //     tmp[count] = parent->children_.at(*it_key)->Clone();
  // //     parent->children_.at(*it_key) = std::shared_ptr<TrieNode>(std::move(tmp[count]));
  // //     parent = &tmp[count ++];
  // //   } else {
  // //     break;
  // //   }
  // // }

  // 根据剩下的路径建立ONLY-TrieNode支路，至仅剩最后一个字符
  // while (it_key + 1 != key.end()) {
  //   vec = std::make_unique<TrieNode>(TrieNode(std::map<char, std::shared_ptr<const TrieNode>>()));
  //   auto it_find = parent->children_.find(*it_key);
  //   if (it_find != parent->children_.end()) {
  //     parent->children_.at(*it_key) = std::move(vec);
  //   } else {
  //     parent->children_.insert(std::make_pair(*it_key, std::move(vec)));
  //   }
  //   parent = parent->children_.at(*it_key);
  //   it_key ++;
  // }

  // 为最后一个字符插入结点，并put上value
  // auto leaf = std::make_unique<TrieNodeWithValue<T>>(TrieNodeWithValue(std::map<char, std::shared_ptr<const TrieNode>>(), std::move(value_ptr)));
  // auto iit_find = parent->children_.find(*it_key);
  // if (iit_find != parent->children_.end()) {  // already exists
  //   leaf->children_ = parent->children_.at(*it_key)->children_;
  //   parent->children_.at(*it_key) = std::move(leaf);
  // }// } else {  // no child for the key-char
  //   // parent->children_.insert(std::make_pair(*it_key, std::shared_ptr<TrieNodeWithValue<T>>(std::move(leaf))));
  // // }

  return Trie(std::shared_ptr<const TrieNode>(std::move(new_root)));
}

auto Trie::Remove(std::string_view key) const -> Trie {
  throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
