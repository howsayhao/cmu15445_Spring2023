//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "execution/executors/index_scan_executor.h"
#include "execution/executors/insert_executor.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      tbl_info_(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)),
      tbl_index_(exec_ctx->GetCatalog()->GetTableIndexes(tbl_info_->name_)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  LockManager::LockMode lock_mode = LockManager::LockMode::INTENTION_EXCLUSIVE;
  // exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), lock_mode, tbl_info_->oid_);
  try {
    // if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), lock_mode, tbl_info_->oid_)) {
    // throw ExecutionException("insert init");
    // }
    exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), lock_mode, tbl_info_->oid_);
  } catch (TransactionAbortException &e) {
    // std::cout << e.GetInfo() << "<<<<<<<<<<<<<<<<<<<<<<<Insert table IX<<<>>>>>>>>>>>>>>>>>>>>>>>>>" << std::endl;
    throw ExecutionException(e.GetInfo());
  }
  done_ = false;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }

  TupleMeta meta = {INVALID_TXN_ID, INVALID_TXN_ID, false};
  int32_t row_nums{0};
  while (child_executor_->Next(tuple, rid)) {
    // insert (tuple from child) to table
    auto new_tuple_rid = tbl_info_->table_->InsertTuple(meta, *tuple, exec_ctx_->GetLockManager(),
                                                        exec_ctx_->GetTransaction(), tbl_info_->oid_);
    if (!new_tuple_rid.has_value()) {
      continue;
    }
    *rid = new_tuple_rid.value();
    auto tbl_write_record = TableWriteRecord(tbl_info_->oid_, *rid, tbl_info_->table_.get());
    tbl_write_record.wtype_ = WType::INSERT;
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(tbl_write_record);
    // insert (index of this tuple) to (b_plus_index_tree of table)
    for (auto &index : tbl_index_) {
      auto key = tuple->KeyFromTuple(tbl_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key, new_tuple_rid.value(), exec_ctx_->GetTransaction());
      exec_ctx_->GetTransaction()->AppendIndexWriteRecord(
          IndexWriteRecord(*rid, tbl_info_->oid_, WType::INSERT, *tuple, index->index_oid_, exec_ctx_->GetCatalog()));
    }
    // iteration
    ++row_nums;
  }

  done_ = true;
  *tuple = Tuple({Value(INTEGER, row_nums)}, &GetOutputSchema());
  return true;
}

}  // namespace bustub
