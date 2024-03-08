//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <memory>
#include "catalog/schema.h"
#include "concurrency/lock_manager.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      tbl_info_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())),
      tbl_it_(std::make_unique<TableIterator>(tbl_info_->table_->MakeEagerIterator())) {}

void SeqScanExecutor::Init() {
  LockManager::LockMode lock_mode = LockManager::LockMode::INTENTION_SHARED;
  switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
    case IsolationLevel::READ_COMMITTED: {
      // 事务禁止降锁，但LockTable处throw是针对于异常情况的，这里防止即可不必throw，都达到禁止降锁目的、不冲突
      if (!(exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(tbl_info_->oid_) ||
            exec_ctx_->GetTransaction()->IsTableExclusiveLocked(tbl_info_->oid_) ||
            exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(tbl_info_->oid_) ||
            exec_ctx_->GetTransaction()->IsTableSharedLocked(tbl_info_->oid_))) {
        exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), lock_mode, tbl_info_->oid_);
      }
    } break;
    case IsolationLevel::READ_UNCOMMITTED:
      break;
  }
  tbl_it_ = std::make_unique<TableIterator>(tbl_info_->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  /** Get the current position of the table iterator. */
  while (!tbl_it_->IsEnd()) {
    *rid = tbl_it_->GetRID();
    /** Lock the tuple as needed for the isolation level. */
    LockManager::LockMode lock_mode = LockManager::LockMode::SHARED;
    switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ:
      case IsolationLevel::READ_COMMITTED: {
        if (!exec_ctx_->GetTransaction()->IsRowExclusiveLocked(tbl_info_->oid_, *rid)) {
          exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), lock_mode, tbl_info_->oid_, *rid);
        }
      } break;
      case IsolationLevel::READ_UNCOMMITTED:
        break;
    }
    /** If the current operation is delete, should take X locks on the table and tuple
     *  which will be set to true for DELETE and UPDATE), you should assume all tuples scanned will be deleted */
    if (exec_ctx_->IsDelete()) {
      LockManager::LockMode lock_mode = LockManager::LockMode::EXCLUSIVE;
      try {
        // if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), lock_mode, tbl_info_->oid_)) {
        // throw ExecutionException("seqscan next, is_delete, locktable");
        // }
        // if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), lock_mode, tbl_info_->oid_, *rid)) {
        // throw ExecutionException("seqscan next, is_delete, lockrow");
        // }
        exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                               tbl_info_->oid_);
        exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), lock_mode, tbl_info_->oid_, *rid);
      } catch (TransactionAbortException &e) {
        // std::cout << e.GetInfo() << "<<<<<<<<<<<<<<<<<<<IsDelete<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>" << std::endl;
        throw ExecutionException(e.GetInfo());
        // exec_ctx_->GetLockManager()->ThrowAbort(exec_ctx_->GetTransaction(),
        // AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
      }
      // exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), lock_mode, tbl_info_->oid_);
      // exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), lock_mode, tbl_info_->oid_, *rid);
    }
    /** Fetch the tuple. Check tuple meta, and if you have implemented filter pushdown to scan, check the predicate. */
    auto [meta, new_tuple] = tbl_it_->GetTuple();
    ++(*tbl_it_);
    if (!meta.is_deleted_) {
      if (plan_->filter_predicate_ != nullptr) {  // 处理优化器将filter下推到seq_scan的情况
        auto value = plan_->filter_predicate_->Evaluate(&new_tuple, GetOutputSchema());
        if (value.IsNull() || !value.GetAs<bool>()) {
          /** If the tuple should not be read by this transaction, force unlock the row. */
          // exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), tbl_info_->oid_, *rid, true);
          try {
            exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), tbl_info_->oid_, *rid, true);
            // if (!exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), tbl_info_->oid_, *rid, true)) {
            //   throw ExecutionException("seq filter-not");
            // }
          } catch (TransactionAbortException &e) {
            // std::cout << e.GetInfo() << "<<<<<<<<<<<<<<<<<<<<not filter<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>" << std::endl;
            throw ExecutionException(e.GetInfo());
          }
          continue;
        }
      }
      /** Otherwise, unlock the row as needed for the isolation level. */
      if (!exec_ctx_->IsDelete()) {
        switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
          case IsolationLevel::REPEATABLE_READ:
            break;
          case IsolationLevel::READ_COMMITTED: {
            exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), tbl_info_->oid_, *rid, false);
          } break;
          case IsolationLevel::READ_UNCOMMITTED:
            break;
        }
      }
      *tuple = new_tuple;
      return true;
    }
    /** If the tuple should not be read by this transaction, force unlock the row. */
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), tbl_info_->oid_, *rid, true);
    }
  }

  return false;
}

}  // namespace bustub
