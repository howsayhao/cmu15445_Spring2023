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

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_info_(exec_ctx->GetCatalog()->GetIndex(plan_->GetIndexOid())),
      // Get the index identifier by index OID.
      tbl_info_(exec_ctx->GetCatalog()->GetTable(index_info_->table_name_)),
      // GetTable(plan_->GetIndexOid()) can not work, tbl_info_ turns to be null or wrong.
      // original note of index_oid_ is "The table whose tuples should be scanned."
      // that should be the index table rather than data content table.
      it_(dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get())->GetBeginIterator()) {}

void IndexScanExecutor::Init() {
  it_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get())->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!it_.IsEnd()) {
    *rid = (*it_).second;
    auto [meta, new_tuple] = tbl_info_->table_->GetTuple(*rid);
    ++it_;
    if (!meta.is_deleted_) {
      *tuple = new_tuple;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
