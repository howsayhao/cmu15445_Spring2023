//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "common/config.h"
#include "execution/executors/update_executor.h"
#include "type/type.h"
#include "type/type_id.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      tbl_info_(exec_ctx->GetCatalog()->GetTable(plan_->table_oid_)),
      child_executor_(std::move(child_executor)),
      tbl_index_(exec_ctx->GetCatalog()->GetTableIndexes(tbl_info_->name_)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  done_ = false;
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  /** proj4; 原地修改 */
  if (done_) {
    return false;
  }
  int32_t update_nums{0};
  while (child_executor_->Next(tuple, rid)) {
    /** 得到新的tuple值 */
    std::vector<Value> values{};
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {  // child_node返回的rows需要处理，例如对指定coloum修改值
      values.push_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    Tuple update_tuple = Tuple(values, &child_executor_->GetOutputSchema());
    /** 原地修改tuple值, 此时tuple和table都已经在children中获取了X和IX锁，且没有被释放 */
    TupleMeta meta{INVALID_TXN_ID, INVALID_TXN_ID, false};
    tbl_info_->table_->UpdateTupleInPlaceUnsafe(meta, update_tuple, *rid);
    /** 记录事务，需要处理回滚吗->需要的，可以直接存旧值 */
    auto tbl_write_record = TableWriteRecord(tbl_info_->oid_, *rid, tbl_info_->table_.get());
    tbl_write_record.wtype_ = WType::UPDATE;
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(tbl_write_record);
    /** 修改索引，记录事务 */
    for (auto index : tbl_index_) {
      auto old_key = tuple->KeyFromTuple(tbl_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      auto update_key = update_tuple.KeyFromTuple(tbl_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(old_key, *rid, exec_ctx_->GetTransaction());
      index->index_->InsertEntry(update_key, *rid, exec_ctx_->GetTransaction());
      exec_ctx_->GetTransaction()->AppendIndexWriteRecord(
          IndexWriteRecord(*rid, tbl_info_->oid_, WType::UPDATE, *tuple, index->index_oid_, exec_ctx_->GetCatalog()));
    }

    ++update_nums;
  }
  done_ = true;
  *tuple = Tuple({Value(INTEGER, update_nums)}, &GetOutputSchema());
  return true;

  // /** proj3; 先删后增 */
  // if (done_) {
  //   return false;
  // }
  // int32_t update_nums{0};
  // TupleMeta meta{INVALID_TXN_ID, INVALID_TXN_ID, false};
  // int64_t once_loc;
  // bool once_update{false};
  // while (child_executor_->Next(tuple, rid)) {
  //   //
  //   底层实现不会填补被删除的tuple的空缺，这使得底层顺序查询可能会循环修改新增的update_tuple，为减小开销采用两个标记位的维护
  //   if (once_update && once_loc == rid->Get()) {  // 基于底层新增tuple都是从当前末端顺序自增的观察，否则不可信
  //     break;  // 因为rid->Get()的实现结合了page_id和slot_num，所以不用担心table不止一页导致类似溢出归0后id冲突的问题
  //   }         // 根据proj3中对delete的描述，被删除的空缺会在commit时执行，确实也比较合理。
  //   // delete
  //   meta.is_deleted_ = true;
  //   tbl_info_->table_->UpdateTupleMeta(meta, *rid);
  //   for (auto index : tbl_index_) {
  //     auto key = tuple->KeyFromTuple(tbl_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
  //     index->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
  //   }
  //   // prepare
  //   std::vector<Value> values{};
  //   values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
  //   for (const auto &expr : plan_->target_expressions_) {  // child_node返回的rows需要处理，例如对指定coloum修改值
  //     values.push_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
  //   }
  //   Tuple update_tuple = Tuple(values, &child_executor_->GetOutputSchema());  // 此处应保留和pushback时的schema一致
  //   // insert
  //   meta.is_deleted_ = false;  // tbl_info_->table_->UpdateTupleMeta(meta, *rid); 不需要，InsertTuple实现了；
  //   auto new_tuple_rid = tbl_info_->table_->InsertTuple(meta, update_tuple);
  //   if (!new_tuple_rid.has_value()) {
  //     continue;
  //   }
  //   for (auto index : tbl_index_) {
  //     auto key = update_tuple.KeyFromTuple(tbl_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
  //     index->index_->InsertEntry(key, new_tuple_rid.value(), exec_ctx_->GetTransaction());
  //   }
  //   // increment
  //   ++update_nums;
  //   if (!once_update) {
  //     once_loc = new_tuple_rid->Get();
  //     once_update = true;
  //   }
  // }
  // done_ = true;
  // *tuple = Tuple({Value(INTEGER, update_nums)}, &GetOutputSchema());
  // return true;
}

}  // namespace bustub
