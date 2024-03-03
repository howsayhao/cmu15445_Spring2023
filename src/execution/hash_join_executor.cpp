//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <cstdint>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_executor_(std::move(left_child)),
      right_child_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_executor_->Init();
  right_child_executor_->Init();
  jht_.clear();
  tuples_.clear();
  Tuple tuple{};
  RID rid{};
  // 构建哈希右表
  while (right_child_executor_->Next(&tuple, &rid)) {
    JoinHashKey key{};
    for (auto &expr : plan_->RightJoinKeyExpressions()) {
      key.jh_keys_.emplace_back(expr->Evaluate(&tuple, right_child_executor_->GetOutputSchema()));
    }
    JoinHashValue values{};
    values.jh_values_.reserve(GetOutputSchema().GetColumnCount());
    for (uint32_t col = 0; col < right_child_executor_->GetOutputSchema().GetColumnCount(); col++) {
      values.jh_values_.emplace_back(tuple.GetValue(&right_child_executor_->GetOutputSchema(), col));
    }
    jht_.insert({key, values});
  }
  // hash-join不必过分在意下面新增的空间开销，其意义在于seq-scan->hash-scan，明显减少一个幂次级的查询开销
  while (left_child_executor_->Next(&tuple, &rid)) {
    JoinHashKey key{};
    for (auto &expr : plan_->LeftJoinKeyExpressions()) {
      key.jh_keys_.emplace_back(expr->Evaluate(&tuple, left_child_executor_->GetOutputSchema()));
    }
    if (jht_.count(key) == 0 && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values{};
      values.reserve(GetOutputSchema().GetColumnCount());
      for (uint32_t col = 0; col < left_child_executor_->GetOutputSchema().GetColumnCount(); col++) {
        values.emplace_back(tuple.GetValue(&left_child_executor_->GetOutputSchema(), col));
      }
      for (uint32_t col = 0; col < right_child_executor_->GetOutputSchema().GetColumnCount(); col++) {
        values.emplace_back(
            ValueFactory::GetNullValueByType(right_child_executor_->GetOutputSchema().GetColumn(col).GetType()));
      }
      tuples_.emplace_back(Tuple{values, &GetOutputSchema()});
    } else if (jht_.count(key) != 0) {
      auto range = jht_.equal_range(key);
      for (auto it = range.first; it != range.second; ++it) {
        std::vector<Value> values{};
        values.reserve(GetOutputSchema().GetColumnCount());
        for (uint32_t col = 0; col < left_child_executor_->GetOutputSchema().GetColumnCount(); col++) {
          values.emplace_back(tuple.GetValue(&left_child_executor_->GetOutputSchema(), col));
        }
        for (auto const &value : it->second.jh_values_) {
          values.emplace_back(value);
        }
        tuples_.emplace_back(Tuple{values, &GetOutputSchema()});
      }
    }
  }
  it_ = tuples_.begin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (it_ != tuples_.end()) {
    *rid = it_->GetRid();
    *tuple = *it_;
    ++it_;
    return true;
  }

  return false;
}

}  // namespace bustub
