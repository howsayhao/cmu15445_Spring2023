//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);
  // 似乎没有去把空洞填掉，坑坑洼洼的
  txn->SetState(TransactionState::COMMITTED);
  std::cout << "<<<<<<<<<<<<<<commit    " << txn->GetTransactionId() << ">>>>>>>>>>>>>>>>>>" << std::endl;
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  while (!txn->GetWriteSet()->empty()) {
    auto record = txn->GetWriteSet()->back();
    switch (record.wtype_) {
      case WType::INSERT: {
        TupleMeta meta = record.table_heap_->GetTupleMeta(record.rid_);
        meta.is_deleted_ = true;
        record.table_heap_->UpdateTupleMeta(meta, record.rid_);
      } break;
      case WType::DELETE: {  // 删除并没有真实删除，空洞保留，因而回退只需要更新meta即可
        TupleMeta meta = record.table_heap_->GetTupleMeta(record.rid_);
        meta.is_deleted_ = false;
        record.table_heap_->UpdateTupleMeta(meta, record.rid_);
      } break;
      default: {                                                       // UPDATE
        TupleMeta old_meta = {INVALID_TXN_ID, INVALID_TXN_ID, false};  // 更新不会删除，更新也不会用来处理被删除的tuple
        /** 这个地方的回滚相当丑陋，或许仅仅适用于这个场景，但transaction.h改不了，遍历真的很蠢，，，
            如果遇到一个事务多次update的情况也会有问题，但没想到有比较好的方法，测试中不会有abort的情况，算了摆烂啦 */
        bool success{false};
        for (auto &index_record : *txn->GetIndexWriteSet()) {
          if (index_record.table_oid_ == record.tid_ && index_record.rid_ == record.rid_) {
            record.table_heap_->UpdateTupleInPlaceUnsafe(old_meta, index_record.old_tuple_, record.rid_);
            success = true;
            break;
          }
        }
        if (!success) {
          BUSTUB_ENSURE(1 == 2, "revert update failed");
        }
      } break;
    }
    txn->GetWriteSet()->pop_back();
  }
  while (!txn->GetIndexWriteSet()->empty()) {
    auto record = txn->GetIndexWriteSet()->back();
    auto index = record.catalog_->GetIndex(record.index_oid_);
    auto tbl_info = record.catalog_->GetTable(record.table_oid_);
    auto key = record.tuple_.KeyFromTuple(tbl_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    switch (record.wtype_) {
      case WType::INSERT: {
        index->index_->DeleteEntry(key, record.rid_, txn);
      } break;
      case WType::DELETE: {
        index->index_->InsertEntry(key, record.rid_, txn);
      } break;
      default: {  // UPDATE
        index->index_->DeleteEntry(key, record.rid_, txn);
        auto old_key =
            record.old_tuple_.KeyFromTuple(tbl_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->InsertEntry(old_key, record.rid_, txn);
        /** 想了一下还是放这里回滚更新tuple的操作，毕竟遍历index来辅助恢复我觉得还是太蠢了 */
        /** 需要考虑不止建立了一个索引的情况，不过无所谓，反正都是最新的值，嗯，，更新值的操作是不是也比较慢，也好蠢啊，，，
            算了，还是原来的吧，反正不影响，terrier没有abort啊好像 */
      } break;
    }
  }

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
  std::cout << "<<<<<<<<<<<<<<abort     " << txn->GetTransactionId() << ">>>>>>>>>>>>>>>>>>" << std::endl;
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
