//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      tbl_info_(exec_ctx->GetCatalog()->GetTable(plan_->table_oid_)),
      tbl_index_(exec_ctx->GetCatalog()->GetTableIndexes(tbl_info_->name_)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  done_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }

  int32_t delete_nums{0};
  TupleMeta meta{INVALID_TXN_ID, INVALID_TXN_ID, false};
  while (child_executor_->Next(tuple, rid)) {
    meta.is_deleted_ = true;  // for function count, ignore
    tbl_info_->table_->UpdateTupleMeta(meta, *rid);
    for (auto index : tbl_index_) {
      auto key = tuple->KeyFromTuple(tbl_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }
    ++delete_nums;
  }

  *tuple = Tuple({Value(INTEGER, delete_nums)}, &GetOutputSchema());
  done_ = true;
  return true;
}

}  // namespace bustub
