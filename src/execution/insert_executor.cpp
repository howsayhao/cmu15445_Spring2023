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
  done_ = false;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }

  TupleMeta meta = {INVALID_TXN_ID, INVALID_TXN_ID, false};
  int32_t row_nums{0};
  while (child_executor_->Next(tuple, rid)) {
    // try: insert (tuple from child) to table
    auto new_tuple_rid = tbl_info_->table_->InsertTuple(meta, *tuple);
    if (!new_tuple_rid.has_value()) {
      continue;
    }
    *rid = new_tuple_rid.value();
    // try: insert (index of this tuple) to (b_plus_index_tree of table)
    for (auto &index : tbl_index_) {
      auto key = tuple->KeyFromTuple(tbl_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key, new_tuple_rid.value(), exec_ctx_->GetTransaction());
    }
    // iteration
    ++row_nums;
  }

  done_ = true;
  *tuple = Tuple({Value(INTEGER, row_nums)}, &GetOutputSchema());
  return true;
}

}  // namespace bustub
