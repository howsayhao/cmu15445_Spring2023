#include <sstream>
#include <string>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/exception.h"
// #include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

// #define ZHHAO_P2_INSERT_DEBUG
// #define ZHHAO_P2_REMOVE_DEBUG
// #define ZHHAO_P2_GET_DEBUG
// #define ZHHAO_P2_GET0_DEBUG
// #define ZHHAO_P2_INSERT0_DEBUG
// #define ZHHAO_P2_ITER_DEBUG

#define POSITIVE_CRAB

// 10/19 这一版本准备落实一下向兄弟借以使得各结点的分担平均，这样可以
// #1.更快的释放瓶颈锁；#2.因为测试点的数据还是偏skewed的，虽然因为进程不同步这个
// 情况有所好转，但总归是skewed的，至少你在插入800-999区间时0-199范围的结点还是相对较空的，所以把任务分担给它们一则降低树的高度，二则可以减少写锁封闭的路径长度
// 降低树的高度对我或许并没那么有利，因为这一定程度降低了我的并发程度，但想到它们本来是用来更多地覆盖我的长路径时，又觉得可以考虑了
// 分担的行为会增加额外的开销，至少需要申请获取兄弟结点了，不过这也更快释放了顶端瓶颈锁，也还是值得考虑的；
// 最后的效果有赖实验，或许只对前几个结点进行兄弟借操作更明显一定，尤其是测试的长度本就不算很大，后面的结点如果也要负载均衡操作，可能对当前不有利，对后续的插入也有益处

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);  // 目前bptree已经至少有一个page了
  auto root_header_page =
      guard.AsMut<BPlusTreeHeaderPage>();  // 得到了该page的数据内容，实际上是一个bptree header page的数据结构，
                                           // 暂时还没有内容，需要初始化，其实就只有一个root_page_id
                                           // 为什么要额外构建一个HeaderPage呢，因为root_page可能为空
  root_header_page->root_page_id_ = INVALID_PAGE_ID;  // 我觉得就不应该有HeadPage，有一说一，它怎么存值呢
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  // 我这里有点意识到为什么要专门搞一个header_page了，可以提高并发
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_header_page = guard.template As<BPlusTreeHeaderPage>();
  bool is_empty = root_header_page->root_page_id_ == INVALID_PAGE_ID;
  guard.Drop();
  return is_empty;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // 找到叶子结点，如果没有则返回false
#ifdef ZHHAO_P2_GET_DEBUG
  auto log = std::stringstream();
  log << "---get---" << key << " | thread " << std::this_thread::get_id() << " | leaf size:" << leaf_max_size_
      << " | internal size:" << internal_max_size_ << std::endl;
  LOG_DEBUG("%s", log.str().c_str());
#endif
  ReadPageGuard head_guard = bpm_->FetchPageRead(header_page_id_);
#ifdef ZHHAO_P2_GET_DEBUG
  log = std::stringstream();
  log << "---get header---" << key << " | thread " << std::this_thread::get_id() << std::endl;
  LOG_DEBUG("%s", log.str().c_str());
#endif
  if (head_guard.template As<BPlusTreeHeaderPage>()->root_page_id_ == INVALID_PAGE_ID) {
#ifdef ZHHAO_P2_GET_DEBUG
    log = std::stringstream();
    log << "---get out,empty---" << key << " | thread " << std::this_thread::get_id() << std::endl;
    LOG_DEBUG("%s", log.str().c_str());
#endif
    return false;
  }
  ReadPageGuard guard = bpm_->FetchPageRead(head_guard.As<BPlusTreeHeaderPage>()->root_page_id_);
#ifdef ZHHAO_P2_GET_DEBUG
  log = std::stringstream();
  log << "get PageId(root): " << head_guard.As<BPlusTreeHeaderPage>()->root_page_id_ << " | thread "
      << std::this_thread::get_id() << std::endl;
  LOG_DEBUG("%s", log.str().c_str());
#endif
  head_guard.Drop();
  auto curr_page = guard.template As<BPlusTreePage>();
  while (!curr_page->IsLeafPage()) {
    int slot_num = GetSlotNum(key, curr_page, true);
    if (slot_num == -1) {
      return false;
    }
    // auto internal_page = reinterpret_cast<const InternalPage *>(curr_page);
    guard = bpm_->FetchPageRead(reinterpret_cast<const InternalPage *>(curr_page)->ValueAt(slot_num));
#ifdef ZHHAO_P2_GET_DEBUG
    log = std::stringstream();
    log << "get PageId: " << guard.PageId() << " | thread " << std::this_thread::get_id() << std::endl;
    LOG_DEBUG("%s", log.str().c_str());
#endif
    curr_page = guard.template As<BPlusTreePage>();
  }
  auto *leaf_page = reinterpret_cast<const LeafPage *>(curr_page);

  // 找到叶子结点后判断是否有对应Key;
  int slot_num = GetSlotNum(key, leaf_page, false);
  if (slot_num != -1) {
    result->push_back(leaf_page->ValueAt(slot_num));
    return true;
  }
  return false;
#ifdef ZHHAO_P2_GET_DEBUG
  log = std::stringstream();
  log << "---get can not found---" << key << " | thread " << std::this_thread::get_id() << std::endl;
  LOG_DEBUG("%s", log.str().c_str());
#endif
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetSlotNum(const KeyType &key, const BPlusTreePage *bp_page, bool internaltype) -> int {
  if (!internaltype) {  // 叶子结点
    auto leaf_page = reinterpret_cast<const LeafPage *>(bp_page);
    int start = 0;
    int end = leaf_page->GetSize() - 1;
    while (start <= end) {
      int slot_num = (start + end) / 2;
      int val = comparator_(key, leaf_page->KeyAt(slot_num));
      if (val == 0) {
        return slot_num;
      }
      if (val > 0) {
        start = slot_num + 1;
      } else {
        end = slot_num - 1;
      }
    }
  } else {  // 内部结点
    auto internal_page = reinterpret_cast<const InternalPage *>(bp_page);
    int start = 1;
    int end = internal_page->GetSize() - 1;
    while (start <= end) {
      int slot_num = (start + end) / 2;
      if (comparator_(key, internal_page->KeyAt(slot_num)) < 0) {
        if (slot_num == start) {
          return start - 1;
        }
        if (comparator_(key, internal_page->KeyAt(slot_num - 1)) >= 0) {
          return slot_num - 1;
        }
        end = slot_num - 1;
      } else {
        if (slot_num == end) {
          return end;
        }
        if (comparator_(key, internal_page->KeyAt(slot_num + 1)) < 0) {
          return slot_num;
        }
        start = slot_num + 1;
      }
    }
  }
  return -1;  // 代表没有找到，仅叶子结点会有这种情况
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
#ifdef ZHHAO_P2_INSERT0_DEBUG
  KeyType index_1;
  KeyType index_999;
  index_1.SetFromInteger(1);
  index_999.SetFromInteger(999);
  if (comparator_(key, index_1) == 0 || comparator_(key, index_999) == 0) {
    // if (comparator_(key, index_1) == 0) {
    auto log = std::stringstream();
    log << "---insert---" << key;
    log << " | thread " << std::this_thread::get_id() << std::endl;
    LOG_DEBUG("%s", log.str().c_str());
  }
#endif

  Context ctx;
  ctx.write_set_.clear();
  MappingType tmp;
  MappingType ins = {key, value};
  int insert_slot = -1;

  /* 先进行乐观螃蟹加锁部分 */
#ifdef POSITIVE_CRAB
  ReadPageGuard head_read_guard = bpm_->FetchPageRead(header_page_id_);
#ifdef ZHHAO_P2_INSERT_DEBUG
  {
    auto log = std::stringstream();
    log << "---insert get header for positive crab---" << key << " | thread " << std::this_thread::get_id()
        << std::endl;
    LOG_DEBUG("%s", log.str().c_str());
  }
#endif
  auto head_read_page = head_read_guard.As<BPlusTreeHeaderPage>();
  if (head_read_page->root_page_id_ != INVALID_PAGE_ID) {
    ReadPageGuard read_guard = bpm_->FetchPageRead(head_read_page->root_page_id_);
    WritePageGuard leaf_crab_guard;
    auto curr_read_page = read_guard.template As<InternalPage>();
    // 下一部分获得叶子结点的WriteGuard
    if (curr_read_page->IsLeafPage()) {
      read_guard.Drop();
      leaf_crab_guard = bpm_->FetchPageWrite(head_read_page->root_page_id_);
      head_read_guard.Drop();
    } else {
      head_read_guard.Drop();
      KeyType vice_key;
      vice_key.SetFromInteger(1);
      bool already_found{false};
      while (!already_found) {
        int slot_num = GetSlotNum(key, curr_read_page, true);
        if (comparator_(curr_read_page->KeyAt(0), vice_key) != 0) {  // 内部结点
          read_guard = bpm_->FetchPageRead(curr_read_page->ValueAt(slot_num));
          curr_read_page = read_guard.template As<InternalPage>();
        } else {
          leaf_crab_guard = bpm_->FetchPageWrite(curr_read_page->ValueAt(slot_num));
          read_guard.Drop();
          already_found = true;
        }
      }
    }
    auto leaf_crab_page = leaf_crab_guard.AsMut<LeafPage>();
#ifdef ZHHAO_P2_INSERT_DEBUG
    {
      auto log = std::stringstream();
      log << "insert crab inspect: ";
      log << " | thread " << std::this_thread::get_id() << std::endl;
      for (int i = 0; i < leaf_crab_page->GetSize(); i++) {
        log << leaf_crab_page->KeyAt(i) << ", ";
      }
      log << "   | page id: " << leaf_crab_guard.PageId() << "  | key: " << key << std::endl;
      LOG_DEBUG("%s", log.str().c_str());
    }
#endif
    //     for (int i = 0; i < leaf_crab_page->GetSize(); i++) {  // 先判断duplicate_key
    //       if (comparator_(leaf_crab_page->KeyAt(i), key) == 0) {
    // #ifdef ZHHAO_P2_INSERT_DEBUG
    //         log = std::stringstream();
    //         log << "---insert out,duplicate---" << key << " | thread " << std::this_thread::get_id() << std::endl;
    //         LOG_DEBUG("%s", log.str().c_str());
    // #endif
    //         return false;
    //       }
    //     }
    if (GetSlotNum(key, leaf_crab_page, false) != -1) {
      return false;
    }
    if (leaf_crab_page->GetSize() < leaf_crab_page->GetMaxSize()) {  // 后判断会不会有split操作，如果不会，乐观插入
      for (int i = 0; i < leaf_crab_page->GetSize(); i++) {
        if (comparator_(key, leaf_crab_page->KeyAt(i)) < 0 && insert_slot == -1) {
          insert_slot = i;
        }
        if (insert_slot != -1) {
          tmp = {leaf_crab_page->KeyAt(i), leaf_crab_page->ValueAt(i)};
          leaf_crab_page->SetAt(i, ins.first, ins.second);
          ins = tmp;
        }
      }
      leaf_crab_page->IncreaseSize(1);
      leaf_crab_page->SetAt(leaf_crab_page->GetSize() - 1, ins.first, ins.second);
#ifdef ZHHAO_P2_INSERT_DEBUG
      {
        auto log = std::stringstream();
        log << "---insert out,no split leaf---" << key << " | thread " << std::this_thread::get_id() << std::endl;
        LOG_DEBUG("%s", log.str().c_str());
      }
#endif
      return true;
    }
    leaf_crab_guard.Drop();
  } else {
    head_read_guard.Drop();
  }
#endif

  /* 之后是非改进版的螃蟹锁策略 */
  WritePageGuard head_write_guard = bpm_->FetchPageWrite(header_page_id_);
#ifdef ZHHAO_P2_INSERT_DEBUG
  {
    auto log = std::stringstream();
    log << "---negative start, insert get header for empty judge---" << key << " | thread "
        << std::this_thread::get_id() << std::endl;
    LOG_DEBUG("%s", log.str().c_str());
  }
#endif
  auto root_header_page = head_write_guard.template As<BPlusTreeHeaderPage>();
  if (root_header_page->root_page_id_ == INVALID_PAGE_ID) {
    page_id_t root_page_id;
    BasicPageGuard tmp_pin_guard = bpm_->NewPageGuarded(&root_page_id);
    WritePageGuard root_guard = bpm_->FetchPageWrite(root_page_id);
    tmp_pin_guard.Drop();
    // BasicPageGuard root_guard = bpm_->NewPageGuarded(&root_page_id, AccessType::Scan);
    auto root_header_page =
        head_write_guard.template AsMut<BPlusTreeHeaderPage>();  // 此处及之后都作此修改，以减少磁盘写脏页的不必要次数
    root_header_page->root_page_id_ = root_page_id;
    head_write_guard.Drop();
    auto root_page = root_guard.template AsMut<LeafPage>();
    root_page->Init(leaf_max_size_);
    // root_page->SetMaxSize(leaf_max_size_);
    root_page->IncreaseSize(1);
    root_page->SetAt(0, key, value);
#ifdef ZHHAO_P2_INSERT_DEBUG
    {
      auto log = std::stringstream();
      log << "---insert out, empty---" << key << " | thread " << std::this_thread::get_id() << std::endl;
      LOG_DEBUG("%s", log.str().c_str());
    }
#endif
    // root_guard.WUnLock();
    root_guard.Drop();
    return true;
  }

  WritePageGuard root_guard = bpm_->FetchPageWrite(root_header_page->root_page_id_);
#ifdef ZHHAO_P2_INSERT_DEBUG
  {
    auto log = std::stringstream();
    log << "---insert get root---" << key << " | thread " << std::this_thread::get_id() << std::endl;
    LOG_DEBUG("%s", log.str().c_str());
  }
#endif
  ctx.write_set_.push_back(std::move(head_write_guard));
  // auto curr_page = root_guard.template AsMut<InternalPage>();
  auto curr_page = root_guard.template As<InternalPage>();
  if (curr_page->GetSize() < curr_page->GetMaxSize()) {
    ctx.write_set_.clear();
  }
  ctx.write_set_.push_back(std::move(root_guard));
  while (!curr_page->IsLeafPage()) {
    WritePageGuard guard;
    int slot_num = GetSlotNum(key, curr_page, true);
    guard = bpm_->FetchPageWrite(curr_page->ValueAt(slot_num));
    // curr_page = guard.template AsMut<InternalPage>();
    curr_page = guard.template As<InternalPage>();
    if (curr_page->GetSize() < curr_page->GetMaxSize()) {  // 此时ctx.write_set_中必然至少有一个父亲结点
      ctx.write_set_.clear();
    }
    ctx.write_set_.push_back(std::move(guard));
  }
  auto leaf_guard =
      std::move(ctx.write_set_.back());  // 下面两行注释掉的是有问题的
                                         // 因为leaf_page仅仅是保存了这个page在mem的地址
                                         // 当pop_back之后page_guard被销毁，这个mem的page可能会被回收
                                         // 那么之后如果又new一个新的page很可能就会被分配在这个mem的地址上
                                         // 这样就会有问题，以为是对前者的操作实际是对后者，后面也有这种错误
                                         // 另外吐槽一下，check-lint竟然不允许/**/，是我操作不对吗？
  // auto orign_page_id = ctx.write_set_.back().PageId();
  // auto leaf_page = ctx.write_set_.back().AsMut<LeafPage>();
  auto orign_page_id = leaf_guard.PageId();
  auto leaf_page_not_mut = leaf_guard.template As<LeafPage>();
  ctx.write_set_.pop_back();

#ifdef ZHHAO_P2_INSERT_DEBUG
  {
    auto log = std::stringstream();
    log << "thread " << std::this_thread::get_id() << " | insert"
        << ": " << key << " [page_id: " << leaf_guard.PageId()
        << ", size: " << leaf_guard.As<BPlusTreePage>()->GetSize() << "/"
        << leaf_guard.As<BPlusTreePage>()->GetMaxSize() << "]" << std::endl
        << "parent ids: ";
    for (auto &page_guard : ctx.write_set_) {
      log << page_guard.PageId() << " → ";
    }
    log << leaf_guard.PageId() << std::endl;
    for (int i = 0; i < leaf_page->GetSize(); i++) {
      log << leaf_page->KeyAt(i) << ", ";
    }
    LOG_DEBUG("%s", log.str().c_str());
  }
#endif

  // 还需要再判断，因为可能看的时候是满的，但经过前面的处理后又空了
  if (GetSlotNum(key, leaf_page_not_mut, false) != -1) {
    return false;
  }
  auto leaf_page = leaf_guard.template AsMut<LeafPage>();
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {  // 后判断会不会有split操作
    for (int i = 0; i < leaf_page->GetSize(); i++) {
      if (comparator_(key, leaf_page->KeyAt(i)) < 0 && insert_slot == -1) {
        insert_slot = i;
      }
      if (insert_slot != -1) {
        tmp = {leaf_page->KeyAt(i), leaf_page->ValueAt(i)};
        leaf_page->SetAt(i, ins.first, ins.second);
        ins = tmp;
      }
    }
    leaf_page->IncreaseSize(1);
    leaf_page->SetAt(leaf_page->GetSize() - 1, ins.first, ins.second);
#ifdef ZHHAO_P2_INSERT_DEBUG
    {
      auto log = std::stringstream();
      log << "---insert out,no split leaf2---" << key << " | thread " << std::this_thread::get_id() << std::endl;
      LOG_DEBUG("%s", log.str().c_str());
    }
#endif
    return true;
  }

  // 处理一般性的insert
  // 每次分裂，传给上面的无非就是一个page的id(只有page_id，因为internal不存slot_num)，确定增添位置的key,以及将增加的key，三个东西
  KeyType orign_key;
  KeyType split_key;
  page_id_t split_page_id;
  // 先将叶子结点处理掉，将叶子结点分裂开，提供给上方pid,key1,key2三个东西
  BasicPageGuard tmp_pin_guard = bpm_->NewPageGuarded(&split_page_id);
  auto split_guard = bpm_->FetchPageWrite(split_page_id);
  tmp_pin_guard.Drop();
  // BasicPageGuard split_guard = bpm_->NewPageGuarded(&split_page_id, AccessType::Scan);
  auto split_leaf_page = split_guard.template AsMut<LeafPage>();
  split_leaf_page->Init(leaf_max_size_);
  // split_leaf_page->SetMaxSize(leaf_max_size_);
  split_leaf_page->SetSize((leaf_page->GetMaxSize() + 1) - (leaf_page->GetMaxSize() + 1) / 2);
  for (int i = 0; i < leaf_page->GetMaxSize(); i++) {
    if (comparator_(key, leaf_page->KeyAt(i)) < 0 && insert_slot == -1) {
      insert_slot = i;
    }
    if (insert_slot != -1 || i >= (leaf_page->GetMaxSize() + 1) / 2) {  // 按序插入并必要地移到split_page上
      if (i < (leaf_page->GetMaxSize() + 1) / 2) {
        tmp = {leaf_page->KeyAt(i), leaf_page->ValueAt(i)};
        leaf_page->SetAt(i, ins.first, ins.second);
        ins = tmp;
      } else {
        if (insert_slot != -1) {
          tmp = {leaf_page->KeyAt(i), leaf_page->ValueAt(i)};
          split_leaf_page->SetAt(i - (leaf_page->GetMaxSize() + 1) / 2, ins.first, ins.second);
          ins = tmp;
        } else {
          split_leaf_page->SetAt(i - (leaf_page->GetMaxSize() + 1) / 2, leaf_page->KeyAt(i), leaf_page->ValueAt(i));
        }
      }
    }
  }
  leaf_page->SetSize((leaf_page->GetMaxSize() + 1) / 2);
  split_leaf_page->SetAt(split_leaf_page->GetSize() - 1, ins.first, ins.second);
  orign_key = leaf_page->KeyAt(0);
  split_key = split_leaf_page->KeyAt(0);
  split_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(split_page_id);
  leaf_guard.Drop();
  // split_guard.WUnLock();
  split_guard.Drop();

  // 不断迭代上推，直到只剩最后一个non-split parent，如果没有non-split parent那么全部推出
  // 这一过程的行为和处理叶子的相似但不一样，所以不能合用代码
  page_id_t new_split_page_id;
  bool vice_terminal{true};
  while (ctx.write_set_.size() > 1) {
    BasicPageGuard tmp_pin_guard = bpm_->NewPageGuarded(&new_split_page_id);
    WritePageGuard split_guard =
        bpm_->FetchPageWrite(new_split_page_id);  // 这两个guard设为局部变量，这样就可以自动drop了
    tmp_pin_guard.Drop();
    // BasicPageGuard split_guard = bpm_->NewPageGuarded(&new_split_page_id, AccessType::Scan);
    WritePageGuard parent_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    orign_page_id = parent_guard.PageId();
    auto parent_page = parent_guard.template AsMut<InternalPage>();
    auto split_page = split_guard.template AsMut<InternalPage>();
    split_page->Init(internal_max_size_);
    // split_page->SetMaxSize(internal_max_size_);
    split_page->SetSize((parent_page->GetMaxSize() + 1) - (parent_page->GetMaxSize() / 2 + 1) - 1 + 1);
    int insert_slot = -1;
    KeyType ktmp;
    page_id_t ptmp;
    KeyType kins = split_key;
    page_id_t pins = split_page_id;
    KeyType new_split_key = split_key;
    for (int i = 1; i < parent_page->GetMaxSize(); i++) {
      if (comparator_(split_key, parent_page->KeyAt(i)) < 0 && insert_slot == -1) {  // 找到槽位
        insert_slot = i;
      }
      if (insert_slot != -1 || i >= (parent_page->GetMaxSize() / 2 + 1)) {  // 按序插入并必要地移到split_page上
        if (i < ((parent_page->GetMaxSize()) / 2 + 1)) {
          ktmp = parent_page->KeyAt(i);
          ptmp = parent_page->ValueAt(i);
          parent_page->SetKeyAt(i, kins);
          parent_page->SetValueAt(i, pins);
          kins = ktmp;
          pins = ptmp;
        } else if (i == (parent_page->GetMaxSize() / 2 + 1)) {
          if (insert_slot != -1) {
            new_split_key = kins;
            split_page->SetValueAt(0, pins);
            kins = parent_page->KeyAt(i);
            pins = parent_page->ValueAt(i);
          } else {
            new_split_key = parent_page->KeyAt(i);
            split_page->SetValueAt(0, parent_page->ValueAt(i));
          }
        } else {
          if (insert_slot != -1) {
            split_page->SetKeyAt(i - (parent_page->GetMaxSize() / 2 + 1), kins);
            split_page->SetValueAt(i - (parent_page->GetMaxSize() / 2 + 1), pins);
            kins = parent_page->KeyAt(i);
            pins = parent_page->ValueAt(i);
          } else {
            split_page->SetKeyAt(i - (parent_page->GetMaxSize() / 2 + 1), parent_page->KeyAt(i));
            split_page->SetValueAt(i - (parent_page->GetMaxSize() / 2 + 1), parent_page->ValueAt(i));
          }
        }
      }
    }
    parent_page->SetSize(parent_page->GetMaxSize() / 2 + 1);
    split_page->SetKeyAt(split_page->GetSize() - 1, kins);
    split_page->SetValueAt(split_page->GetSize() - 1, pins);
    // 处理次终端结点增加的情况
    if (vice_terminal) {
      KeyType vice_key;
      vice_key.SetFromInteger(1);
      split_page->SetKeyAt(0, vice_key);
      vice_terminal = false;
    }
    orign_key = parent_page->KeyAt(1);
    split_page_id = new_split_page_id;
    split_key = new_split_key;
    // split_guard.WUnLock();
    parent_guard.Drop();
    split_guard.Drop();
  }

  // 没有non-split parent，意味着需要重建root
  if (ctx.write_set_.front().PageId() == header_page_id_) {
#ifdef ZHHAO_P2_INSERT_DEBUG
    {
      auto log = std::stringstream();
      log << "---need to rebuild root---" << key << " | thread " << std::this_thread::get_id() << std::endl;
      LOG_DEBUG("%s", log.str().c_str());
    }
#endif
    auto root_header_page = ctx.write_set_.front().template AsMut<BPlusTreeHeaderPage>();
    page_id_t root_page_id;
    BasicPageGuard tmp_pin_guard = bpm_->NewPageGuarded(&root_page_id);  //
    //     只是临时的guard，用完马上drop，免得影响后续的unpin以及evict
    // BasicPageGuard guard = bpm_->NewPageGuarded(&root_page_id, AccessType::Scan);
    root_header_page->root_page_id_ = root_page_id;
    WritePageGuard guard = bpm_->FetchPageWrite(root_page_id);  // 这里的fetch必须在父节点drop之前，
    // 因为否则的话该page id已经被获取了，但该guard还未初始化
    // 那么其他进程在悲观锁部分会先一步fetch，并对未初始化的guard进行操作
    tmp_pin_guard.Drop();  // 保护frame slot的任务已经完成了，需要把额外的那份pin给取消掉
    ctx.write_set_.front().Drop();
    auto root_page = guard.template AsMut<InternalPage>();
    root_page->Init(internal_max_size_);
    // root_page->SetMaxSize(internal_max_size_);
    root_page->IncreaseSize(1);
    root_page->SetKeyAt(1, split_key);
    root_page->SetValueAt(1, split_page_id);
    root_page->SetValueAt(0, orign_page_id);
    if (vice_terminal) {
      KeyType vice_key;
      vice_key.SetFromInteger(1);
      root_page->SetKeyAt(0, vice_key);
      vice_terminal = false;
    }
#ifdef ZHHAO_P2_INSERT_DEBUG
    {
      auto log = std::stringstream();
      log << "---insert out,root rebuilt---" << key << " | thread " << std::this_thread::get_id() << std::endl;
      LOG_DEBUG("%s", log.str().c_str());
    }
#endif
    // guard.WUnLock();
    guard.Drop();
    return true;
  }

  // 最后的non-split结点处理
  WritePageGuard parent_guard = std::move(ctx.write_set_.back());
  auto parent_page = parent_guard.template AsMut<InternalPage>();
  ctx.write_set_.pop_back();
  KeyType kins = split_key;
  page_id_t pins = split_page_id;
  insert_slot = GetSlotNum(split_key, parent_page, true) + 1;
  parent_page->IncreaseSize(1);
  for (int i = parent_page->GetSize() - 1; i > insert_slot; i--) {
    parent_page->SetKeyAt(i, parent_page->KeyAt(i - 1));
    parent_page->SetValueAt(i, parent_page->ValueAt(i - 1));
  }
  parent_page->SetKeyAt(insert_slot, kins);
  parent_page->SetValueAt(insert_slot, pins);
  parent_guard.Drop();
#ifdef ZHHAO_P2_INSERT_DEBUG
  {
    auto log = std::stringstream();
    log << "---insert out,top no split---" << key << " | thread " << std::this_thread::get_id() << std::endl;
    LOG_DEBUG("%s", log.str().c_str());
  }
#endif
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // 锁控制，目前为借助page_guard的基础锁策略
#ifdef ZHHAO_P2_REMOVE_DEBUG
  auto log = std::stringstream();
  log << "---remove---" << key << " | thread " << std::this_thread::get_id() << std::endl;
  LOG_DEBUG("%s", log.str().c_str());
#endif
  // std::cout << "remove " << std::endl;
  Context ctx;
  ctx.write_set_.clear();
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.template AsMut<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return;
  }

  // 先找到叶子结点，同时存储路径
  ctx.root_page_id_ = header_page->root_page_id_;
  WritePageGuard guard = bpm_->FetchPageWrite(ctx.root_page_id_);
  auto curr_page = guard.template AsMut<InternalPage>();
  ctx.write_set_.push_back(std::move(header_guard));
  if ((curr_page->IsLeafPage() && curr_page->GetSize() >= 2) || curr_page->GetSize() >= 3) {
    ctx.write_set_.clear();  // 此处不需要主动drop释放，至少我试验过的没问题
  }
  ctx.write_set_.push_back(std::move(guard));
  while (!curr_page->IsLeafPage()) {
    WritePageGuard guard;
    for (int i = 1; i < curr_page->GetSize(); i++) {
      if (comparator_(key, curr_page->KeyAt(i)) < 0) {
        guard = bpm_->FetchPageWrite(curr_page->ValueAt(i - 1));
        break;
      }
      if (i + 1 == curr_page->GetSize()) {
        guard = bpm_->FetchPageWrite(curr_page->ValueAt(i));
      }
    }
    curr_page = guard.template AsMut<InternalPage>();
    if (curr_page->GetSize() > curr_page->GetMinSize()) {
      ctx.write_set_.clear();
    }
    ctx.write_set_.push_back(std::move(guard));
  }
  auto leaf_guard = std::move(ctx.write_set_.back());
  auto leaf_page = leaf_guard.template AsMut<LeafPage>();
  ctx.write_set_.pop_back();
#ifdef ZHHAO_P2_REMOVE_DEBUG
  log = std::stringstream();
  log << "thread " << std::this_thread::get_id() << " | insert"
      << ": " << key << " [page_id: " << leaf_guard.PageId() << ", size: " << leaf_guard.As<BPlusTreePage>()->GetSize()
      << "/" << leaf_guard.As<BPlusTreePage>()->GetMaxSize() << "]" << std::endl
      << "parent ids: ";
  for (auto &page_guard : ctx.write_set_) {
    log << page_guard.PageId() << " → ";
  }
  log << leaf_guard.PageId();
  LOG_DEBUG("%s", log.str().c_str());
#endif

  // 到达叶子结点后，判断key的情况，先处理两个小case
  KeyType key_for_locate = leaf_page->KeyAt(0);  // 该变量仅为处理后续parent_page无key可用的情况
                                                 // 需注意无论该key是否会被删除,都可用于表征其parent的位置
  int delete_slot = -1;
  // 如果树非空，但找不到对应结点key，直接返回；树为空的情况前面已经处理掉了；
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(key, leaf_page->KeyAt(i)) == 0) {
      delete_slot = i;  // 找到要删的key的位置
      break;
    }
  }
  if (delete_slot == -1) {
#ifdef ZHHAO_P2_REMOVE_DEBUG
    auto log = std::stringstream();
    log << "---remove out,target key not found---" << key << " | thread " << std::this_thread::get_id() << std::endl;
    LOG_DEBUG("%s", log.str().c_str());
#endif
    return;
  }
  // 先删key，如果不需要merge（大小符合或只有根结点,因为根结点的min为1），直接返回
  for (int i = delete_slot + 1; i < leaf_page->GetSize(); i++) {
    leaf_page->SetAt(i - 1, leaf_page->KeyAt(i), leaf_page->ValueAt(i));
  }
  leaf_page->IncreaseSize(-1);
  if (leaf_page->GetSize() >= leaf_page->GetMinSize() || leaf_guard.PageId() == ctx.root_page_id_) {
    if (leaf_page->GetSize() == 0) {  // 只可能是根结点，此时树为空，
                                      // 因而为了保持和之前代码的一致性应该删除该page
      leaf_guard.Drop();
      // auto header_page = bpm_->FetchPageWrite(header_page_id_).template AsMut<BPlusTreeHeaderPage>();
      ctx.write_set_.front().template AsMut<BPlusTreeHeaderPage>()->root_page_id_ = INVALID_PAGE_ID;
      ctx.write_set_.pop_front();
    }
#ifdef ZHHAO_P2_REMOVE_DEBUG
    auto log = std::stringstream();
    log << "---remove out,no merge or borrow for leaf, or just drop for root---" << key << " | thread "
        << std::this_thread::get_id() << std::endl;
    LOG_DEBUG("%s", log.str().c_str());
#endif
    return;
  }

  // 迭代删除的策略如下，总体为先借后merge，如果借就能解决问题那么甚至不需要再迭代，调整后直接输出即可
  // 对叶子：
  // 借：优先向右兄弟借，借不来再向左兄弟借；若向右兄弟借则需要注意更新parent对应右兄弟的key，反之同理；
  // 合并：优先右兄弟，没有右兄弟才与左兄弟合并；合并之后删去对应的parent的key即可；
  // 对内部结点：
  // 借：也是优先向右兄弟借；不同的是借来的给parent，同时拿parent过来以维持搜索树的特征；
  // 合并：也是优先右兄弟；若与右兄弟合并则需要删去最右边的key换上parent的对应key与兄弟合并，反之同理；
  // 因为没有严格的更新，因而删除之后并不能保证每一个internal_key都有唯一的leaf_key对应；
  // 所以需要先将叶子结点的merge删除问题解决掉，提供给上层一个已经初步删除后的parent即可(<min)
  auto parent_guard =
      std::move(ctx.write_set_.back());  // leaf的merge删除用到需要parent
                                         // 这里需要std::move，因为page_guard的直接复制被删掉了，需要右值引用
  ctx.write_set_.pop_back();
  auto parent_page = parent_guard.template AsMut<InternalPage>();
  int orign_slot_in_parent = -1;
  for (int i = 1; i < parent_page->GetSize(); i++) {
    if (comparator_(key, parent_page->KeyAt(i)) < 0) {
      orign_slot_in_parent = i - 1;
      break;
    }
  }
  WritePageGuard rsibling_guard;
  WritePageGuard lsibling_guard;
  if (orign_slot_in_parent == 0) {
    // 没有左兄弟
    rsibling_guard = bpm_->FetchPageWrite(parent_page->ValueAt(orign_slot_in_parent + 1));
    auto rsibling_page = rsibling_guard.template AsMut<LeafPage>();
    if (rsibling_page->GetSize() > rsibling_page->GetMinSize()) {
      // 向右兄弟借
      leaf_page->IncreaseSize(1);
      leaf_page->SetAt(leaf_page->GetSize() - 1, rsibling_page->KeyAt(0), rsibling_page->ValueAt(0));
      parent_page->SetKeyAt(orign_slot_in_parent + 1, rsibling_page->KeyAt(1));
      for (int i = 1; i < rsibling_page->GetSize(); i++) {
        rsibling_page->SetAt(i - 1, rsibling_page->KeyAt(i), rsibling_page->ValueAt(i));
      }
      rsibling_page->IncreaseSize(-1);
      return;
    }
    // 与右兄弟合并
    leaf_page->SetNextPageId(rsibling_page->GetNextPageId());
    int i = leaf_page->GetSize();
    leaf_page->IncreaseSize(rsibling_page->GetSize());
    for (int j = 0; j < rsibling_page->GetSize(); i++, j++) {
      leaf_page->SetAt(i, rsibling_page->KeyAt(j), rsibling_page->ValueAt(j));
    }
    rsibling_guard.Drop();
    for (int i = orign_slot_in_parent + 2; i < parent_page->GetSize(); i++) {
      parent_page->SetKeyAt(i - 1, parent_page->KeyAt(i));
      parent_page->SetValueAt(i - 1, parent_page->ValueAt(i));
    }
    parent_page->IncreaseSize(-1);
  } else if (orign_slot_in_parent == -1) {
    // 没有右兄弟
    orign_slot_in_parent = parent_page->GetSize() - 1;
    lsibling_guard = bpm_->FetchPageWrite(parent_page->ValueAt(orign_slot_in_parent - 1));
    auto lsibling_page = lsibling_guard.template AsMut<LeafPage>();
    if (lsibling_page->GetSize() > lsibling_page->GetMinSize()) {
      // 向左兄弟借
      leaf_page->IncreaseSize(1);
      for (int i = leaf_page->GetSize() - 1; i >= 1; i--) {
        leaf_page->SetAt(i, leaf_page->KeyAt(i - 1), leaf_page->ValueAt(i - 1));
      }
      leaf_page->SetAt(0, lsibling_page->KeyAt(lsibling_page->GetSize() - 1),
                       lsibling_page->ValueAt(lsibling_page->GetSize() - 1));
      parent_page->SetKeyAt(orign_slot_in_parent, lsibling_page->KeyAt(lsibling_page->GetSize() - 1));
      lsibling_page->IncreaseSize(-1);
      return;
    }
    // 与左兄弟合并
    lsibling_page->SetNextPageId(leaf_page->GetNextPageId());
    int i = lsibling_page->GetSize();
    lsibling_page->IncreaseSize(leaf_page->GetSize());
    for (int j = 0; j < leaf_page->GetSize(); i++, j++) {
      lsibling_page->SetAt(i, leaf_page->KeyAt(j), leaf_page->ValueAt(j));
    }
    leaf_guard.Drop();
    for (int i = orign_slot_in_parent + 1; i < parent_page->GetSize(); i++) {
      parent_page->SetKeyAt(i - 1, parent_page->KeyAt(i));
      parent_page->SetValueAt(i - 1, parent_page->ValueAt(i));
    }
    parent_page->IncreaseSize(-1);
  } else {
    // 左右兄弟都有，需要判别
    lsibling_guard = bpm_->FetchPageWrite(parent_page->ValueAt(orign_slot_in_parent - 1));
    rsibling_guard = bpm_->FetchPageWrite(parent_page->ValueAt(orign_slot_in_parent + 1));
    auto lsibling_page = lsibling_guard.template AsMut<LeafPage>();
    auto rsibling_page = rsibling_guard.template AsMut<LeafPage>();
    if (rsibling_page->GetSize() > rsibling_page->GetMinSize()) {
      // 向右兄弟借
      leaf_page->IncreaseSize(1);
      leaf_page->SetAt(leaf_page->GetSize() - 1, rsibling_page->KeyAt(0), rsibling_page->ValueAt(0));
      parent_page->SetKeyAt(orign_slot_in_parent + 1, rsibling_page->KeyAt(1));
      for (int i = 1; i < rsibling_page->GetSize(); i++) {
        rsibling_page->SetAt(i - 1, rsibling_page->KeyAt(i), rsibling_page->ValueAt(i));
      }
      rsibling_page->IncreaseSize(-1);
      return;
    }
    if (lsibling_page->GetSize() > lsibling_page->GetMinSize()) {
      // 向左兄弟借
      leaf_page->IncreaseSize(1);
      for (int i = leaf_page->GetSize() - 1; i >= 1; i--) {
        leaf_page->SetAt(i, leaf_page->KeyAt(i - 1), leaf_page->ValueAt(i - 1));
      }
      leaf_page->SetAt(0, lsibling_page->KeyAt(lsibling_page->GetSize() - 1),
                       lsibling_page->ValueAt(lsibling_page->GetSize() - 1));
      parent_page->SetKeyAt(orign_slot_in_parent, lsibling_page->KeyAt(lsibling_page->GetSize() - 1));
      lsibling_page->IncreaseSize(-1);
      return;
    }
    // 与右兄弟合并
    leaf_page->SetNextPageId(rsibling_page->GetNextPageId());
    int i = leaf_page->GetSize();
    leaf_page->IncreaseSize(rsibling_page->GetSize());
    for (int j = 0; j < rsibling_page->GetSize(); i++, j++) {
      leaf_page->SetAt(i, rsibling_page->KeyAt(j), rsibling_page->ValueAt(j));
    }
    rsibling_guard.Drop();  // 这个意思只是说buffer_pool里的这个page不再表达原来的物理page了,
                            // 但真正的物理page依然有内容，是下面的索引脱钩才使得该有内容的page无效化了
    for (int i = orign_slot_in_parent + 2; i < parent_page->GetSize(); i++) {
      parent_page->SetKeyAt(i - 1, parent_page->KeyAt(i));
      parent_page->SetValueAt(i - 1, parent_page->ValueAt(i));
    }
    parent_page->IncreaseSize(-1);
  }
  ctx.write_set_.push_back(std::move(parent_guard));  // 将修改后的parent结点返回；
  // 需要进行迭代merge删除，并保留最顶层的parent结点
  while (ctx.write_set_.size() > 1) {  // 到这一步其实已经保证至少有两个结点在手了，
                                       // 现在的目的是将头尾结点之间的所有结点都迭代推出处理掉
    WritePageGuard curr_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    WritePageGuard parent_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    // 下面的代码基本和前面的差不多
    auto curr_page = curr_guard.template AsMut<InternalPage>();
    auto parent_page = parent_guard.template AsMut<InternalPage>();
    if (parent_guard.PageId() == header_page_id_) {
      auto header_page = reinterpret_cast<BPlusTreeHeaderPage *>(parent_page);
      header_page->root_page_id_ = curr_page->ValueAt(0);
      curr_guard.Drop();
      return;
    }
    int orign_slot_in_parent = -1;
    if (curr_page->GetSize() != 1) {
      key_for_locate = curr_page->KeyAt(1);
    }
    for (int i = 1; i < parent_page->GetSize(); i++) {
      if (comparator_(key_for_locate, parent_page->KeyAt(i)) < 0) {
        // 此处有问题，curr_page可能已经没有可用的key了，此时无法定位在其parent的位置
        // 需要对该临界情况做讨论，并处理此时的定位问题，故对此处做了修改
        orign_slot_in_parent = i - 1;
        break;
      }
    }
    key_for_locate = parent_page->KeyAt(1);  // 更新新的key_for_locate，此时parent还未减小；
    WritePageGuard rsibling_guard;
    WritePageGuard lsibling_guard;
    if (orign_slot_in_parent == 0) {
      // 没有左兄弟
      rsibling_guard = bpm_->FetchPageWrite(parent_page->ValueAt(orign_slot_in_parent + 1));
      auto rsibling_page = rsibling_guard.template AsMut<InternalPage>();
      if (rsibling_page->GetSize() > rsibling_page->GetMinSize()) {
        // 向右兄弟借
        curr_page->IncreaseSize(1);
        curr_page->SetKeyAt(curr_page->GetSize() - 1, parent_page->KeyAt(orign_slot_in_parent + 1));
        curr_page->SetValueAt(curr_page->GetSize() - 1, rsibling_page->ValueAt(0));
        parent_page->SetKeyAt(orign_slot_in_parent + 1, rsibling_page->KeyAt(1));
        for (int i = 1; i < rsibling_page->GetSize(); i++) {
          if (i != 1) {
            rsibling_page->SetKeyAt(i - 1, rsibling_page->KeyAt(i));
          }
          rsibling_page->SetValueAt(i - 1, rsibling_page->ValueAt(i));
        }
        rsibling_page->IncreaseSize(-1);
        return;
      }
      // 与右兄弟合并
      int i = curr_page->GetSize();
      curr_page->IncreaseSize(rsibling_page->GetSize());
      curr_page->SetKeyAt(i, parent_page->KeyAt(orign_slot_in_parent + 1));
      curr_page->SetValueAt(i, rsibling_page->ValueAt(0));
      for (int j = 1; j < rsibling_page->GetSize(); i++, j++) {
        curr_page->SetKeyAt(i + 1, rsibling_page->KeyAt(j));
        curr_page->SetValueAt(i + 1, rsibling_page->ValueAt(j));
      }
      rsibling_guard.Drop();
      for (int i = orign_slot_in_parent + 2; i < parent_page->GetSize(); i++) {
        parent_page->SetKeyAt(i - 1, parent_page->KeyAt(i));
        parent_page->SetValueAt(i - 1, parent_page->ValueAt(i));
      }
      parent_page->IncreaseSize(-1);
    } else if (orign_slot_in_parent == -1) {
      // 没有右兄弟
      orign_slot_in_parent = parent_page->GetSize() - 1;
      lsibling_guard = bpm_->FetchPageWrite(parent_page->ValueAt(orign_slot_in_parent - 1));
      auto lsibling_page = lsibling_guard.template AsMut<InternalPage>();
      if (lsibling_page->GetSize() > lsibling_page->GetMinSize()) {
        // 向左兄弟借
        curr_page->IncreaseSize(1);
        for (int i = curr_page->GetSize() - 1; i >= 1; i--) {
          if (i != 1) {
            curr_page->SetKeyAt(i, curr_page->KeyAt(i - 1));
          }
          curr_page->SetValueAt(i, curr_page->ValueAt(i - 1));
        }
        curr_page->SetKeyAt(1, parent_page->KeyAt(orign_slot_in_parent));
        curr_page->SetValueAt(0, lsibling_page->ValueAt(lsibling_page->GetSize() - 1));
        parent_page->SetKeyAt(orign_slot_in_parent, lsibling_page->KeyAt(lsibling_page->GetSize() - 1));
        lsibling_page->IncreaseSize(-1);
        return;
      }
      // 与左兄弟合并
      int i = lsibling_page->GetSize();
      lsibling_page->IncreaseSize(curr_page->GetSize());
      lsibling_page->SetKeyAt(i, parent_page->KeyAt(orign_slot_in_parent));
      lsibling_page->SetValueAt(i, curr_page->ValueAt(0));
      for (int j = 1; j < curr_page->GetSize(); i++, j++) {
        lsibling_page->SetKeyAt(i + 1, curr_page->KeyAt(j));
        lsibling_page->SetValueAt(i + 1, curr_page->ValueAt(j));
      }
      curr_guard.Drop();
      for (int i = orign_slot_in_parent + 1; i < parent_page->GetSize(); i++) {
        parent_page->SetKeyAt(i - 1, parent_page->KeyAt(i));
        parent_page->SetValueAt(i - 1, parent_page->ValueAt(i));
      }
      parent_page->IncreaseSize(-1);
    } else {
      // 左右兄弟都有，需要判别
      lsibling_guard = bpm_->FetchPageWrite(parent_page->ValueAt(orign_slot_in_parent - 1));
      rsibling_guard = bpm_->FetchPageWrite(parent_page->ValueAt(orign_slot_in_parent + 1));
      auto lsibling_page = lsibling_guard.template AsMut<InternalPage>();
      auto rsibling_page = rsibling_guard.template AsMut<InternalPage>();
      if (rsibling_page->GetSize() > rsibling_page->GetMinSize()) {
        // 向右兄弟借
        curr_page->IncreaseSize(1);
        curr_page->SetKeyAt(curr_page->GetSize() - 1, parent_page->KeyAt(orign_slot_in_parent + 1));
        curr_page->SetValueAt(curr_page->GetSize() - 1, rsibling_page->ValueAt(0));
        parent_page->SetKeyAt(orign_slot_in_parent + 1, rsibling_page->KeyAt(1));
        for (int i = 1; i < rsibling_page->GetSize(); i++) {
          if (i != 1) {
            rsibling_page->SetKeyAt(i - 1, rsibling_page->KeyAt(i));
          }
          rsibling_page->SetValueAt(i - 1, rsibling_page->ValueAt(i));
        }
        rsibling_page->IncreaseSize(-1);
        return;
      }
      if (lsibling_page->GetSize() > lsibling_page->GetMinSize()) {
        // 向左兄弟借
        curr_page->IncreaseSize(1);
        for (int i = curr_page->GetSize() - 1; i >= 1; i--) {
          if (i != 1) {
            curr_page->SetKeyAt(i, curr_page->KeyAt(i - 1));
          }
          curr_page->SetValueAt(i, curr_page->ValueAt(i - 1));
        }
        curr_page->SetKeyAt(1, parent_page->KeyAt(orign_slot_in_parent));
        curr_page->SetValueAt(0, lsibling_page->ValueAt(lsibling_page->GetSize() - 1));
        parent_page->SetKeyAt(orign_slot_in_parent, lsibling_page->KeyAt(lsibling_page->GetSize() - 1));
        lsibling_page->IncreaseSize(-1);
        return;
      }
      // 与右兄弟合并
      int i = curr_page->GetSize();
      curr_page->IncreaseSize(rsibling_page->GetSize());
      curr_page->SetKeyAt(i, parent_page->KeyAt(orign_slot_in_parent + 1));
      curr_page->SetValueAt(i, rsibling_page->ValueAt(0));
      for (int j = 1; j < rsibling_page->GetSize(); i++, j++) {
        curr_page->SetKeyAt(i + 1, rsibling_page->KeyAt(j));
        curr_page->SetValueAt(i + 1, rsibling_page->ValueAt(j));
      }
      rsibling_guard.Drop();
      for (int i = orign_slot_in_parent + 2; i < parent_page->GetSize(); i++) {
        parent_page->SetKeyAt(i - 1, parent_page->KeyAt(i));
        parent_page->SetValueAt(i - 1, parent_page->ValueAt(i));
      }
      parent_page->IncreaseSize(-1);
    }
    ctx.write_set_.push_back(std::move(parent_guard));
  }
  ctx.write_set_.front().Drop();
  // 如果不需要，那么最顶层的parent结点只需要做基本的删除即可，不需要merge
  // 实际上不需要任何操作了；
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
// 二分查找
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinaryFind(const LeafPage *leaf_page, const KeyType &key) -> int {
  int l = 0;
  int r = leaf_page->GetSize() - 1;
  while (l < r) {
    int mid = (l + r + 1) >> 1;
    if (comparator_(leaf_page->KeyAt(mid), key) != 1) {
      l = mid;
    } else {
      r = mid - 1;
    }
  }

  if (r >= 0 && comparator_(leaf_page->KeyAt(r), key) == 1) {
    r = -1;
  }

  return r;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinaryFind(const InternalPage *internal_page, const KeyType &key) -> int {
  int l = 1;
  int r = internal_page->GetSize() - 1;
  while (l < r) {
    int mid = (l + r + 1) >> 1;
    if (comparator_(internal_page->KeyAt(mid), key) != 1) {
      l = mid;
    } else {
      r = mid - 1;
    }
  }

  if (r == -1 || comparator_(internal_page->KeyAt(r), key) == 1) {
    r = 0;
  }

  return r;
}

/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  ReadPageGuard head_guard = bpm_->FetchPageRead(header_page_id_);
  if (head_guard.template As<BPlusTreeHeaderPage>()->root_page_id_ == INVALID_PAGE_ID) {
    return End();
  }
  ReadPageGuard guard = bpm_->FetchPageRead(head_guard.As<BPlusTreeHeaderPage>()->root_page_id_);
  head_guard.Drop();

  auto curr_page = guard.template As<BPlusTreePage>();
  while (!curr_page->IsLeafPage()) {
    int slot_num = 0;
    guard = bpm_->FetchPageRead(reinterpret_cast<const InternalPage *>(curr_page)->ValueAt(slot_num));
    curr_page = guard.template As<BPlusTreePage>();
  }
  // auto *leaf_page = reinterpret_cast<const LeafPage *>(curr_page);

  // 找到叶子结点后判断是否有对应Key;
  int slot_num = 0;
  if (slot_num != -1) {
    // result->push_back(leaf_page->ValueAt(slot_num));
    return INDEXITERATOR_TYPE(bpm_, guard.PageId(), 0);
  }
  return End();
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  ReadPageGuard head_guard = bpm_->FetchPageRead(header_page_id_);

  if (head_guard.template As<BPlusTreeHeaderPage>()->root_page_id_ == INVALID_PAGE_ID) {
    return End();
  }
  ReadPageGuard guard = bpm_->FetchPageRead(head_guard.As<BPlusTreeHeaderPage>()->root_page_id_);
  head_guard.Drop();
  auto curr_page = guard.template As<BPlusTreePage>();
  while (!curr_page->IsLeafPage()) {
    // int slot_num = GetSlotNum(key, curr_page, true);
    auto internal = reinterpret_cast<const InternalPage *>(curr_page);
    int slot_num = BinaryFind(internal, key);
    if (slot_num == -1) {
      BUSTUB_ENSURE(1 == 2, "iterator begin(key) not find leaf page");
      return End();
    }
    guard = bpm_->FetchPageRead(reinterpret_cast<const InternalPage *>(curr_page)->ValueAt(slot_num));
    curr_page = guard.template As<BPlusTreePage>();
  }
  auto *leaf_page = reinterpret_cast<const LeafPage *>(curr_page);

  // 找到叶子结点后判断是否有对应Key;
  // int slot_num = GetSlotNum(key, leaf_page, false);
  int slot_num = BinaryFind(leaf_page, key);
  if (slot_num != -1) {
    return INDEXITERATOR_TYPE(bpm_, guard.PageId(), slot_num);
  }
  BUSTUB_ENSURE(1 == 2, "iterator begin(key) not find key at leaf page");
  return End();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(bpm_, -1, -1); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_header_page = guard.template As<BPlusTreeHeaderPage>();
  page_id_t root_page_id = root_header_page->root_page_id_;
  guard.Drop();
  return root_page_id;
}

// /*****************************************************************************
//  * INDEX ITERATOR
//  *****************************************************************************/
// /*
//  * Input parameter is void, find the leftmost leaf page first, then construct
//  * index iterator
//  * @return : index iterator
//  */
// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
//   // 找到最左边叶子结点的page_id
//   // rwlatch_.RLock();
// #ifdef ZHHAO_P2_ITER_DEBUG
//   auto log = std::stringstream();
//   log << "---begin()--- | thread " << std::this_thread::get_id() << std::endl;
//   LOG_DEBUG("%s", log.str().c_str());
// #endif
//   // std::cout << "begin" << std::endl;
//   if (IsEmpty()) {
//     INDEXITERATOR_TYPE iterator(comparator_);
//     // rwlatch_.RUnlock();
//     return iterator;
//   }
//   ReadPageGuard guard = bpm_->FetchPageRead(GetRootPageId());
//   auto curr_page = guard.As<InternalPage>();
//   while (!curr_page->IsLeafPage()) {
//     guard = bpm_->FetchPageRead(curr_page->ValueAt(0));
//     curr_page = guard.As<InternalPage>();
//   }
//   auto leaf_page = guard.As<LeafPage>();
//   // 建立迭代器并返回
//   page_id_t curr_page_id = guard.PageId();
//   int curr_slot = 0;
//   KeyType curr_key = leaf_page->KeyAt(0);
//   ValueType curr_val = leaf_page->ValueAt(0);
//   MappingType curr_map = {curr_key, curr_val};
//   INDEXITERATOR_TYPE iterator("first_iterator", bpm_, comparator_, curr_page_id, curr_slot, curr_key, curr_val,
//                               curr_map, leaf_max_size_, internal_max_size_);
//   // rwlatch_.RUnlock();
//   return iterator;
// }

// /*
//  * Input parameter is low key, find the leaf page that contains the input key
//  * first, then construct index iterator
//  * @return : index iterator
//  */
// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
//   // rwlatch_.RLock();
// #ifdef ZHHAO_P2_ITER_DEBUG
//   auto log = std::stringstream();
//   log << "---begin(" << key << ")---"
//       << " | thread " << std::this_thread::get_id() << std::endl;
//   LOG_DEBUG("%s", log.str().c_str());
// #endif
//   auto iterator = Begin();
//   for (; iterator != End(); ++iterator) {
//     if (comparator_((*iterator).first, key) == 0) {
//       break;
//     }
//   }
//   // rwlatch_.RUnlock();
//   return iterator;
// }

// /*
//  * Input parameter is void, construct an index iterator representing the end
//  * of the key/value pair in the leaf node
//  * @return : index iterator
//  */
// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
//   // rwlatch_.RLock();
// #ifdef ZHHAO_P2_ITER_DEBUG
//   auto log = std::stringstream();
//   log << "---end()--- | thread " << std::this_thread::get_id() << std::endl;
//   LOG_DEBUG("%s", log.str().c_str());
// #endif
//   auto iterator = Begin();
//   iterator.SetEnd();
//   // rwlatch_.RUnlock();
//   return iterator;
// }

// /**
//  * @return Page id of the root of this tree
//  */
// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
//   ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
//   auto root_header_page = guard.template As<BPlusTreeHeaderPage>();
//   page_id_t root_page_id = root_header_page->root_page_id_;
//   guard.Drop();
//   return root_page_id;
// }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input) {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction) {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      // if (i > 0) {
      out << inner->KeyAt(i) << "  " << inner->ValueAt(i);
      // } else {
      // out << inner->ValueAt(0);
      // }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
