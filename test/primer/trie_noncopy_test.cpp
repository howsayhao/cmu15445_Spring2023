#include <bitset>
#include <functional>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <thread>  // NOLINT

#include "common/exception.h"
#include "gtest/gtest.h"
#include "primer/trie.h"

namespace bustub {

using Integer = std::unique_ptr<uint32_t>;

TEST(TrieTest, NonCopyableTest) {
  auto trie = Trie();
  // trie = trie.Put<Integer>("", std::make_unique<uint32_t>(23));
  // trie = trie.Put<Integer>("", std::make_unique<uint32_t>(233));
  // trie = trie.Put<Integer>("t", std::make_unique<uint32_t>(23));
  trie = trie.Put<Integer>("te", std::make_unique<uint32_t>(2333));
  // ASSERT_EQ(**trie.Get<Integer>(""), 233);
  // std::cout << **trie.Get<Integer>("") << std::endl;
  // ASSERT_EQ(**trie.Get<Integer>("t"), 23);
  // std::cout << **trie.Get<Integer>("t") << std::endl;
  ASSERT_EQ(**trie.Get<Integer>("te"), 2333);
  // std::cout << **trie.Get<Integer>("te") << std::endl;
  // std::cout << **trie.Get<Integer>("") << std::endl;
  // ASSERT_EQ(**trie.Get<Integer>("test"), 2333);
  // trie = trie.Remove("te");
  // trie = trie.Remove("tes");
  // trie = trie.Remove("test");
  // ASSERT_EQ(trie.Get<Integer>("te"), nullptr);
  // ASSERT_EQ(trie.Get<Integer>("tes"), nullptr);
  // ASSERT_EQ(trie.Get<Integer>("test"), nullptr);
}

}  // namespace bustub
