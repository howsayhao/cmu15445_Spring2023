//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstdint>
// #include <thread>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "execution/expressions/constant_value_expression.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      predicate_(plan_->Predicate()) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  match_ = false;
  already_init_ = false;
  done_this_turn_ = true;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (done_this_turn_) {
      if (!left_executor_->Next(tuple, rid)) {
        return false;
      }
      done_this_turn_ = false;
      left_tuple_ = *tuple;
    }
    if (!already_init_) {
      right_executor_->Init();
      already_init_ = true;
    }
    while (right_executor_->Next(tuple, rid)) {
      Tuple right_tuple = *tuple;
      auto value = predicate_->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                            right_executor_->GetOutputSchema());
      if ((!value.IsNull() && value.GetAs<bool>())) {
        std::vector<Value> values{};
        values.reserve(GetOutputSchema().GetColumnCount());
        for (uint32_t col = 0; col < left_executor_->GetOutputSchema().GetColumnCount(); col++) {
          values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), col));
        }
        for (uint32_t col = 0; col < right_executor_->GetOutputSchema().GetColumnCount(); col++) {
          values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), col));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        match_ = true;
        return true;
      }
    }
    if (!match_ && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values{};
      values.reserve(GetOutputSchema().GetColumnCount());
      for (uint32_t col = 0; col < left_executor_->GetOutputSchema().GetColumnCount(); col++) {
        values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), col));
      }
      for (uint32_t col = 0; col < right_executor_->GetOutputSchema().GetColumnCount(); col++) {
        values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(col).GetType()));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      already_init_ = false;
      match_ = false;
      done_this_turn_ = true;
      return true;
    }
    already_init_ = false;
    match_ = false;
    done_this_turn_ = true;
  }
  // return false;
}

}  // namespace bustub
