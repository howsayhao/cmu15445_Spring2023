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
  // auto trie = Trie();
  // trie = trie.Put<Integer>("", std::make_unique<uint32_t>(23));
  // // trie = trie.Put<Integer>("", std::make_unique<uint32_t>(233));
  // trie = trie.Put<Integer>("te", std::make_unique<uint32_t>(233));
  // trie = trie.Put<Integer>("tes", std::make_unique<uint32_t>(2333));
  // trie = trie.Put<Integer>("test", std::make_unique<uint32_t>(23333));
  // // ASSERT_EQ(**trie.Get<Integer>("t"), 23);
  // // std::cout << **trie.Get<Integer>("t") << std::endl;
  // // ASSERT_EQ(**trie.Get<Integer>("te"), 2333);
  // // ASSERT_EQ(**trie.Get<Integer>(""), 23);
  // // std::cout << **trie.Get<Integer>("te") << std::endl;
  // // std::cout << **trie.Get<Integer>("") << std::endl;
  // // // std::cout << **trie.Get<Integer>("") << std::endl;
  // // ASSERT_EQ(**trie.Get<Integer>("test"), 23333);
  // trie = trie.Remove("te");
  // trie = trie.Remove("tes");
  // trie = trie.Remove("test");
  // ASSERT_EQ(trie.Get<Integer>("te"), nullptr);
  // ASSERT_EQ(trie.Get<Integer>("tes"), nullptr);
  // ASSERT_EQ(trie.Get<Integer>("test"), nullptr);

  // auto trie = Trie();
  // trie = trie.Put<uint32_t>("111", 111);
  // trie = trie.Put<uint32_t>("11", 11);
  // trie = trie.Put<uint32_t>("1111", 1111);
  // trie = trie.Put<uint32_t>("11", 22);
  // ASSERT_EQ(*trie.Get<uint32_t>("11"), 22);
  // ASSERT_EQ(*trie.Get<uint32_t>("111"), 111);
  // ASSERT_EQ(*trie.Get<uint32_t>("1111"), 1111);
  // // std::cout << *trie.Get<uint32_t>("11") << std::endl;
  // // std::cout << *trie.Get<uint32_t>("111") << std::endl;
  // if (trie.Get<Integer>("test") == nullptr) {
  // std::cout << **trie.Get<Integer>("te") << std::endl;
  // }
  // // std::cout << *trie.Get<uint32_t>("1111") << std::endl;

  auto trie = Trie();
  // Put something
  trie = trie.Put<uint32_t>("test", 2333);
  ASSERT_EQ(*trie.Get<uint32_t>("test"), 2333);
  trie = trie.Put<uint32_t>("te", 23);
  ASSERT_EQ(*trie.Get<uint32_t>("te"), 23);
  trie = trie.Put<uint32_t>("tes", 233);
  ASSERT_EQ(*trie.Get<uint32_t>("tes"), 233);
  // Delete something
  trie = trie.Remove("test");
  trie = trie.Remove("tes");
  trie = trie.Remove("te");

  ASSERT_EQ(trie.Get<uint32_t>("te"), nullptr);
  ASSERT_EQ(trie.Get<uint32_t>("tes"), nullptr);
  ASSERT_EQ(trie.Get<uint32_t>("test"), nullptr);
}

}  // namespace bustub
