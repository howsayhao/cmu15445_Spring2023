#include "execution/executors/sort_executor.h"
#include <algorithm>
#include <vector>
#include "execution/expressions/abstract_expression.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple{};
  RID rid{};
  tuples_.clear();
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
  }
  // for (auto it = plan_->GetOrderBy().rbegin(); it != plan_->GetOrderBy().rend(); ++it) {
  //     switch (it->first) {
  //       case OrderByType::DESC: {
  //         std::sort(tuples_.begin(), tuples_.end(), [expr=it->second, child=child_executor_.get()](const Tuple tpl1,
  //         const Tuple tpl2){
  //             return expr->Evaluate(&tpl1, child->GetOutputSchema()).CompareGreaterThan(
  //                 expr->Evaluate(&tpl2, child->GetOutputSchema())) == CmpBool::CmpTrue; });
  //       } break;
  //     //   case OrderByType::INVALID: break; // omit
  //       default: { // default or other value, go asc
  //         std::sort(tuples_.begin(), tuples_.end(), [expr=it->second, child=child_executor_.get()](const Tuple tpl1,
  //         const Tuple tpl2){
  //             return expr->Evaluate(&tpl1, child->GetOutputSchema()).CompareLessThan(
  //                 expr->Evaluate(&tpl2, child->GetOutputSchema())) == CmpBool::CmpTrue; });
  //       } break;
  //     }
  // }
  std::sort(tuples_.begin(), tuples_.end(), [this](const Tuple &left_tuple, const Tuple &right_tuple) {
    for (auto &[type, expr] : this->plan_->GetOrderBy()) {
      auto left_value = expr->Evaluate(&left_tuple, this->child_executor_->GetOutputSchema());
      auto right_value = expr->Evaluate(&right_tuple, this->child_executor_->GetOutputSchema());
      if (left_value.CompareLessThan(right_value) == CmpBool::CmpTrue) {
        return type != OrderByType::DESC;
      }
      if (left_value.CompareGreaterThan(right_value) == CmpBool::CmpTrue) {
        return type == OrderByType::DESC;
      }
    }
    return true;
  });
  it_ = tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (it_ != tuples_.end()) {
    *tuple = *it_;
    ++it_;
    return true;
  }
  return false;
}

}  // namespace bustub
