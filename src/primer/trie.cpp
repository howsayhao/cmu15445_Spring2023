#include "primer/trie.h"
#include <iostream>
#include <stack>
#include <string_view>
#include <typeinfo>
#include "common/exception.h"

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
    new_root = root_->Clone();  // 因为既然是put了，root_就不可能还是空的
  }
  auto value_ptr = std::make_shared<T>(std::move(value));
  if (key.empty()) {  // 根结点可能存值
    auto tmp = std::make_unique<TrieNodeWithValue<T>>(TrieNodeWithValue(new_root->children_, std::move(value_ptr)));
    return Trie(std::move(tmp));
  }
  std::unique_ptr<TrieNode> slot = nullptr;
  std::stack<std::pair<char, std::unique_ptr<TrieNode>>> stack_vec;
  auto parent = &new_root;

  auto it_key = key.begin();
  // 遍历原Tire树，直到路径结束或没有结点延续，对直接相关支路结点均Clone，并修改上级结点的map信息
  for (; it_key + 1 != key.end(); ++it_key) {  // 假定至少有一个字符，遍历到倒数第二个字符为止
    auto it_find = (*parent)->children_.find(*it_key);
    if (it_find != (*parent)->children_.end()) {
      slot = (*parent)->children_.at(*it_key)->Clone();
      stack_vec.push(std::make_pair(*it_key, std::move(slot)));
      parent = &stack_vec.top().second;  // 所以裸指针很危险，如果还是parent=&slot，会出问题
    } else {
      break;
    }
  }

  // 根据剩下的路径建立ONLY-TrieNode支路，至仅剩最后一个字符
  while (it_key + 1 != key.end()) {
    slot = std::make_unique<TrieNode>(TrieNode(std::map<char, std::shared_ptr<const TrieNode>>()));
    stack_vec.push(std::make_pair(*it_key, std::move(slot)));
    it_key++;
  }

  // 回收，建新树
  slot = std::make_unique<TrieNodeWithValue<T>>(
      TrieNodeWithValue(std::map<char, std::shared_ptr<const TrieNode>>(), std::move(value_ptr)));
  char key_char = *it_key;
  while (!stack_vec.empty()) {
    auto it_find = stack_vec.top().second->children_.find(key_char);
    if (it_find != stack_vec.top().second->children_.end()) {
      if (it_key + 1 ==
          key.end()) {  // 只有新加入的叶子不清楚自己的children_，其他的都Clone()过来了，唯一要加的就只有top()
        slot->children_ = stack_vec.top().second->children_.at(key_char)->children_;
        --it_key;  // 不再进入该if条件
      }
      stack_vec.top().second->children_.at(key_char) = std::move(slot);
    } else {
      it_key = key.begin();  // 防止首次没有进入上面的if，以后又进去了
      stack_vec.top().second->children_.insert(std::make_pair(key_char, std::move(slot)));
    }
    key_char = stack_vec.top().first;
    slot = std::move(stack_vec.top().second);
    stack_vec.pop();
  }
  auto it_find = new_root->children_.find(key_char);
  if (it_find != new_root->children_.end()) {
    new_root->children_.at(key_char) = std::move(slot);
  } else {
    new_root->children_.insert(std::make_pair(key_char, std::move(slot)));
  }

  return Trie(std::shared_ptr<const TrieNode>(std::move(new_root)));
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  if (root_ == nullptr) {
    return Trie(nullptr);
  }
  auto new_root = root_->Clone();
  std::stack<std::pair<char, std::unique_ptr<TrieNode>>> stack_vec;
  stack_vec.push(std::make_pair('a', std::move(new_root)));

  auto it_key = key.begin();
  while (it_key != key.end() && !key.empty()) {
    auto it_find = (stack_vec.top().second)->children_.find(*it_key);
    if (it_find != (stack_vec.top().second)->children_.end()) {
      auto slot = (stack_vec.top().second)->children_.at(*it_key)->Clone();
      stack_vec.push(std::make_pair(*it_key, std::move(slot)));
    } else {
      new_root = root_->Clone();
      return Trie(std::shared_ptr<const TrieNode>(std::move(new_root)));
    }
    it_key++;
  }

  // 定位到后，开始删结点
  if (key.empty()) {  // 删的是根结点，因为对根结点的情况rnode和parent都是new_root，失去了本应赋予的意义，故需特殊处理
    if (root_->children_.empty()) {
      return Trie(nullptr);
    }
    new_root = std::make_unique<TrieNode>(TrieNode(root_->children_));
  } else if ((stack_vec.top().second)->is_value_node_) {  // 不带VALUE的非终端结点不可能删，终端结点都带VALUE
    char char_key = stack_vec.top().first;
    if (stack_vec.top().second->children_.empty()) {
      stack_vec.pop();
      stack_vec.top().second->children_.erase(char_key);
      while (stack_vec.size() > 1) {  // 直到root或第一个带VALUE的非终端结点
        if (stack_vec.top().second->children_.empty() && !(stack_vec.top().second->is_value_node_)) {
          char_key = stack_vec.top().first;
          stack_vec.pop();
          stack_vec.top().second->children_.erase(char_key);
        } else {
          break;
        }
      }
    } else {
      auto slot = std::move(stack_vec.top().second);
      stack_vec.pop();
      stack_vec.top().second->children_.at(char_key) = std::make_unique<TrieNode>(TrieNode(slot->children_));
    }
  }
  // 建新树
  auto slot = std::move(stack_vec.top().second);
  char key_char = stack_vec.top().first;
  stack_vec.pop();
  while (!stack_vec.empty()) {
    stack_vec.top().second->children_.at(key_char) = std::move(slot);
    key_char = stack_vec.top().first;
    slot = std::move(stack_vec.top().second);
    stack_vec.pop();
  }
  new_root = std::move(slot);
  if (!new_root->is_value_node_ && new_root->children_.empty()) {
    return Trie(nullptr);
  }

  return Trie(std::shared_ptr<const TrieNode>(std::move(new_root)));
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

// using namespace bustub;
// int main(void) {
//   // std::mt19937_64 gen(2333);
//   // std::uniform_int_distribution<uint32_t> dis(0, 100);

//   // auto trie = Trie();
//   // for (uint32_t i = 0; i < 10; i++) {
//   //   std::string key = fmt::format("{}", dis(gen));
//   //   auto value = dis(gen);
//   //   trie = trie.Put<uint32_t>(key, value);
//   // }
//   auto trie = Trie();
//   trie = trie.Put<uint32_t>("65", 25);
//   trie = trie.Put<uint32_t>("61", 65);
//   trie = trie.Put<uint32_t>("82", 84);
//   trie = trie.Put<uint32_t>("2", 42);
//   trie = trie.Put<uint32_t>("16", 67);
//   trie = trie.Put<uint32_t>("94", 53);
//   trie = trie.Put<uint32_t>("20", 35);
//   trie = trie.Put<uint32_t>("3", 57);
//   trie = trie.Put<uint32_t>("93", 30);
//   trie = trie.Put<uint32_t>("75", 29);

//   auto tmp = *trie.Get<uint32_t>("75");
//   std::map<char, std::shared_ptr<const TrieNode>> child;
//   std::string str = "此处结束";
//   // Put a breakpoint here.
//   std::cout << tmp << std::endl;
//   std::cout << str << std::endl;
//   // (1) How many children nodes are there on the root?
//   // Replace `CASE_1_YOUR_ANSWER` in `trie_answer.h` with the correct answer.
//   // if (CASE_1_YOUR_ANSWER != Case1CorrectAnswer()) {
//   //   ASSERT_TRUE(false);
//   // }

//   // // (2) How many children nodes are there on the node of prefix `9`?
//   // // Replace `CASE_2_YOUR_ANSWER` in `trie_answer.h` with the correct answer.
//   // if (CASE_2_YOUR_ANSWER != Case2CorrectAnswer()) {
//   //   ASSERT_TRUE(false);
//   // }

//   // // (3) What's the value for `93`?
//   // // Replace `CASE_3_YOUR_ANSWER` in `trie_answer.h` with the correct answer.
//   // if (CASE_3_YOUR_ANSWER != Case3CorrectAnswer()) {
//   //   ASSERT_TRUE(false);
//   // }
// }
