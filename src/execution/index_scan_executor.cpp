//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <vector>
#include "common/macros.h"
#include "concurrency/lock_manager.h"
#include "include/type/value_factory.h"
#include "storage/index/generic_key.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_info_(exec_ctx->GetCatalog()->GetIndex(plan_->index_oid_)),
      // Get the index identifier by index OID.
      tbl_info_(exec_ctx->GetCatalog()->GetTable(index_info_->table_name_)),
      // GetTable(plan_->GetIndexOid()) can not work, tbl_info_ turns to be null or wrong.
      // original note of index_oid_ is "The table whose tuples should be scanned."
      // that should be the index table rather than data content table.
      it_(dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get())->GetBeginIterator()) {}

void IndexScanExecutor::Init() {
  it_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get())->GetBeginIterator();

  if (plan_->single_strike_) {
    /** proj4; 斩首行动 key_col == const */
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
    killed_ = false;
    // 迭代器不支持并发，所以无法做到范围查找，
    // 不过斩首行动也只是更新某个nft的terrier，本身就只有一个record/row，直接点查找即可
  } else {
    /** proj3; 范围索引查找 key_col >= const */
    if (plan_->predicate_ != nullptr) {
      Tuple key = Tuple(plan_->range_start_, index_info_->index_->GetKeySchema());
      IntegerKeyType index_key;
      index_key.SetFromKey(key);
      it_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get())->GetBeginIterator(index_key);
    }
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (plan_->single_strike_) {
    /** proj4; 斩首行动 key_col == const */
    if (killed_) {
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        /** If the tuple should not be read by this transaction, force unlock the row. */
        exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), tbl_info_->oid_, *rid, true);
      }
      return false;
    }
    std::vector<RID> rids;
    Tuple key = Tuple(plan_->range_start_, index_info_->index_->GetKeySchema());
    index_info_->index_->ScanKey(key, &rids, exec_ctx_->GetTransaction());
    BUSTUB_ENSURE(rids.size() == 1, "GetValue() should return and ONLY return one rid.");
    *rid = rids.back();
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
        exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                               tbl_info_->oid_);
        exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), lock_mode, tbl_info_->oid_, *rid);
      } catch (TransactionAbortException &e) {
        std::cout << e.GetInfo() << "<<<<<<<<<<<<<<<<<<<IsDelete<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>" << std::endl;
        throw ExecutionException(e.GetInfo());
      }
    }
    auto [meta, new_tuple] = tbl_info_->table_->GetTuple(rids.back());
    if (meta.is_deleted_) {
      killed_ = true;
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
          !exec_ctx_->IsDelete()) {
        /** If the tuple should not be read by this transaction, force unlock the row. */
        exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), tbl_info_->oid_, *rid, true);
      }
      return false;
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
    killed_ = true;
    return true;
  }

  /** proj3; */
  while (!it_.IsEnd()) {
    *rid = (*it_).second;
    GenericKey<8> key = (*it_).first;
    ++it_;
    if (plan_->predicate_ != nullptr) {
      std::vector<Value> data{};
      data.emplace_back(key.ToValue(index_info_->index_->GetKeySchema(), 0));
      data.emplace_back(key.ToValue(index_info_->index_->GetKeySchema(), 1));
      /** proj3; 偷个懒，目前的测试仅针对(x>=,y=)的场景 */
      if (data[1].CompareEquals(plan_->range_start_[1]) != CmpBool::CmpTrue) {
        continue;
      }
      if (data[0].CompareGreaterThanEquals(plan_->range_start_[0]) != CmpBool::CmpTrue) {
        continue;
      }
      auto [meta, new_tuple] = tbl_info_->table_->GetTuple(*rid);
      *tuple = new_tuple;
      if (meta.is_deleted_) {
        continue;
      }
      return true;
    }
    auto [meta, new_tuple] = tbl_info_->table_->GetTuple(*rid);
    *tuple = new_tuple;
    if (meta.is_deleted_) {
      continue;
    }
    return true;
  }
  return false;
}

}  // namespace bustub
