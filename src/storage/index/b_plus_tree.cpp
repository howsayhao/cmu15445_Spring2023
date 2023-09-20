#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

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
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_header_page = guard.AsMut<BPlusTreeHeaderPage>();
  return root_header_page->root_page_id_ == INVALID_PAGE_ID;
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
  if (IsEmpty()) {
    return false;
  }
  ReadPageGuard guard = bpm_->FetchPageRead(GetRootPageId());
  auto curr_page = guard.As<InternalPage>();
  while (!curr_page->IsLeafPage()) {
    for (int i = 1; i < curr_page->GetSize(); i++) {  // 注意，这里的getsize是已经包括了空槽的了，所以<
      if (comparator_(key, curr_page->KeyAt(i)) == -1) {
        guard = bpm_->FetchPageRead(curr_page->ValueAt(i - 1));
        break;
      }
      if (i + 1 == curr_page->GetSize()) {  // 取最后一个child node
        guard = bpm_->FetchPageRead(curr_page->ValueAt(i));
      }
    }
    curr_page = guard.As<InternalPage>();
  }
  auto leaf_page = guard.As<LeafPage>();

  // 找到叶子结点后判断是否有对应Key;
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(key, leaf_page->KeyAt(i)) == 0) {
      (*result).push_back(leaf_page->ValueAt(i));
      return true;
    }
  }
  return false;
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
  // Declaration of context instance.
  Context ctx;
  // 处理root page为空的情况
  if (IsEmpty()) {
    WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
    auto root_header_page = guard.AsMut<BPlusTreeHeaderPage>();
    page_id_t root_page_id;
    bpm_->NewPageGuarded(&root_page_id);
    root_header_page->root_page_id_ = root_page_id;
    guard = bpm_->FetchPageWrite(root_page_id);
    auto root_page = guard.AsMut<LeafPage>();
    root_page->Init();
    root_page->SetAt(0, key, value);
    root_page->IncreaseSize(1);
    return true;
  }
  // 找到叶子结点并存储必要路径
  ctx.root_page_id_ = GetRootPageId();
  WritePageGuard guard = bpm_->FetchPageWrite(GetRootPageId());
  auto curr_page = guard.As<InternalPage>();
  ctx.write_set_.push_back(std::move(guard));
  while (!curr_page->IsLeafPage()) {
    WritePageGuard guard;
    for (int i = 1; i < curr_page->GetSize(); i++) {  // 注意，这里的getsize是已经包括了空槽的了，所以<
      if (comparator_(key, curr_page->KeyAt(i)) == -1) {
        guard = bpm_->FetchPageWrite(curr_page->ValueAt(i - 1));
        break;
      }
      if (i + 1 == curr_page->GetSize()) {  // 取最后一个child node
        guard = bpm_->FetchPageWrite(curr_page->ValueAt(i));
      }
    }
    curr_page = guard.As<InternalPage>();
    if (curr_page->GetSize() < curr_page->GetMaxSize()) {
      ctx.write_set_.clear();
    }
    ctx.write_set_.push_back(std::move(guard));
  }
  // 进行insert操作，首先处理两个小case
  auto orign_page_id = ctx.write_set_.back().PageId();
  auto leaf_page = ctx.write_set_.back().AsMut<LeafPage>();
  ctx.write_set_.pop_back();
  for (int i = 0; i < leaf_page->GetSize(); i++) {  // 先判断duplicate_key
    if (comparator_(leaf_page->KeyAt(i), key) == 0) {
      return false;
    }
  }
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {  // 不会有split操作
    leaf_page->SetAt(leaf_page->GetSize(), key, value);
    leaf_page->IncreaseSize(1);
    return true;
  }
  // 处理一般性的insert
  // 每次分裂，传给上面的无非就是一个page的id(只有page_id，因为internal不存slot_num)，确定增添位置的key,以及将增加的key，三个东西
  KeyType orign_key;
  KeyType split_key;
  page_id_t split_page_id;
  // 先将叶子结点处理掉
  bpm_->NewPageGuarded(&split_page_id);
  auto split_guard = bpm_->FetchPageWrite(split_page_id);
  auto split_leaf_page = split_guard.AsMut<LeafPage>();
  split_leaf_page->Init();
  split_leaf_page->SetSize((leaf_page->GetMaxSize() + 1) - (leaf_page->GetMaxSize() + 1) / 2);
  MappingType tmp;
  MappingType ins = {key, value};
  int insert_slot = -1;
  for (int i = 0; i < leaf_page->GetMaxSize(); i++) {
    if (comparator_(key, leaf_page->KeyAt(i)) >= 0 && insert_slot == -1) {  // 找到槽位
      insert_slot = i;
    }
    if (insert_slot != -1) {  // 按序插入并必要地移到split_page上
      if (i < (leaf_page->GetMaxSize() + 1) / 2) {
        tmp = {leaf_page->KeyAt(i), leaf_page->ValueAt(i)};
        leaf_page->SetAt(i, ins.first, ins.second);
        ins = tmp;
      } else {
        tmp = {leaf_page->KeyAt(i), leaf_page->ValueAt(i)};
        split_leaf_page->SetAt(i - (leaf_page->GetMaxSize() + 1) / 2, ins.first, ins.second);
        ins = tmp;
      }
    }
  }
  leaf_page->SetSize((leaf_page->GetMaxSize() + 1) / 2);
  split_leaf_page->SetAt(split_leaf_page->GetSize() - 1, ins.first, ins.second);
  orign_key = leaf_page->KeyAt(0);
  split_key = split_leaf_page->KeyAt(0);
  split_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(split_page_id);

  // 不断迭代上推，直到只剩最后一个parent，它的行为和其他的不一样
  WritePageGuard parent_guard;
  page_id_t new_split_page_id;
  while (ctx.write_set_.size() > 1 || ctx.write_set_.back().PageId() == GetRootPageId()) {
    parent_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    auto parent_page = parent_guard.AsMut<InternalPage>();
    bpm_->NewPageGuarded(&new_split_page_id);
    auto split_guard = bpm_->FetchPageWrite(new_split_page_id);
    auto split_page = split_guard.AsMut<InternalPage>();
    split_page->Init();
    split_page->SetSize((parent_page->GetMaxSize()) - (parent_page->GetMaxSize()) / 2);

    KeyType ktmp;
    page_id_t ptmp;
    KeyType kins = split_key;
    page_id_t pins = split_page_id;
    int insert_slot = -1;
    for (int i = 1; i < parent_page->GetMaxSize(); i++) {
      if (comparator_(split_key, parent_page->KeyAt(i)) >= 0 && insert_slot == -1) {  // 找到槽位
        insert_slot = i;
      }
      if (insert_slot != -1) {  // 按序插入并必要地移到split_page上
        if (i < (parent_page->GetMaxSize()) / 2) {
          ktmp = parent_page->KeyAt(i);
          ptmp = parent_page->ValueAt(i);
          parent_page->SetKeyAt(i, kins);
          parent_page->SetValueAt(i, pins);
          kins = ktmp;
          pins = ptmp;
        } else if (i == (parent_page->GetMaxSize()) / 2) {
          split_key = kins;
          split_page->SetValueAt(0, pins);
          kins = parent_page->KeyAt(i);
          pins = parent_page->ValueAt(i);
        } else {
          parent_page->SetKeyAt(i - (parent_page->GetMaxSize()) / 2, kins);
          parent_page->SetValueAt(i - (parent_page->GetMaxSize()) / 2, pins);
          kins = parent_page->KeyAt(i);
          pins = parent_page->ValueAt(i);
        }
      }
    }
    parent_page->SetSize((parent_page->GetMaxSize()) / 2);
    split_page->SetKeyAt(split_page->GetSize() - 1, kins);
    split_page->SetValueAt(split_page->GetSize() - 1, pins);
    orign_key = parent_page->KeyAt(1);
    split_page_id = new_split_page_id;
    orign_page_id = parent_guard.PageId();
  }

  // 获得后判断此时根结点是否分裂
  if (ctx.write_set_.empty()) {
    WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
    auto root_header_page = guard.AsMut<BPlusTreeHeaderPage>();
    page_id_t root_page_id;
    bpm_->NewPageGuarded(&root_page_id);
    root_header_page->root_page_id_ = root_page_id;
    guard = bpm_->FetchPageWrite(root_page_id);
    auto root_page = guard.AsMut<InternalPage>();
    root_page->Init();
    root_page->SetKeyAt(1, split_key);
    root_page->SetValueAt(1, split_page_id);
    root_page->SetValueAt(0, orign_page_id);
    root_page->IncreaseSize(1);
    return true;
  }

  // 最后的non-split结点处理
  parent_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto parent_page = parent_guard.AsMut<InternalPage>();
  KeyType ktmp;
  page_id_t ptmp;
  KeyType kins = split_key;
  page_id_t pins = split_page_id;
  insert_slot = -1;
  for (int i = 1; i < parent_page->GetMaxSize(); i++) {
    if (comparator_(split_key, parent_page->KeyAt(i)) >= 0 && insert_slot == -1) {  // 找到槽位
      insert_slot = i;
    }
    if (insert_slot != -1) {  // 按序插入并必要地移到split_page上
      ktmp = parent_page->KeyAt(i);
      ptmp = parent_page->ValueAt(i);
      parent_page->SetKeyAt(i, kins);
      parent_page->SetValueAt(i, pins);
      kins = ktmp;
      pins = ptmp;
    }
  }
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
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_header_page = guard.AsMut<BPlusTreeHeaderPage>();
  return root_header_page->root_page_id_;
}

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
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
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
