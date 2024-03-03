//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(SimpleAggregationHashTable(plan_->GetAggregates(), plan_->GetAggregateTypes())),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_.Clear();
  Tuple tuple{};
  RID rid{};
  while (child_executor_->Next(&tuple, &rid)) {  // get all value from child and insert into hashtable
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = aht_.Begin();
  cope_with_empty_ = false;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // empty table, especially count_star
  if (aht_.Begin() == aht_.End() && !cope_with_empty_ &&
      plan_->group_bys_.empty()) {  // no group, no output: if exist group-by, return null if empty table
    *tuple = Tuple(aht_.GenerateInitialAggregateValue().aggregates_, &GetOutputSchema());
    cope_with_empty_ = true;
    return true;
  }

  while (aht_iterator_ != aht_.End()) {
    std::vector<Value> values{};
    for (auto &group_key : aht_iterator_.Key().group_bys_) {
      values.push_back(group_key);
    }
    for (auto &aggregate : aht_iterator_.Val().aggregates_) {
      values.push_back(aggregate);
    }
    Tuple agg_tuple = Tuple(values, &GetOutputSchema());
    *tuple = agg_tuple;
    ++aht_iterator_;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
