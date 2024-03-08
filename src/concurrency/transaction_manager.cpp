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
#include "common/macros.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
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
      case WType::DELETE: { // 删除并没有真实删除，空洞保留，因而回退只需要更新meta即可
        TupleMeta meta = record.table_heap_->GetTupleMeta(record.rid_);
        meta.is_deleted_ = false;
        record.table_heap_->UpdateTupleMeta(meta, record.rid_);
      }
      default: break;
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
      }
      default: break;
    }
  }

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
