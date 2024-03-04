#include "execution/executors/topn_executor.h"
#include <iterator>
#include "common/rid.h"
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  tuples_.clear();
  auto compare = [this](const Tuple tpl1, const Tuple tpl2) -> bool {
    for (auto &[type, expr] : this->plan_->GetOrderBy()) {
      auto left_value = expr->Evaluate(&tpl1, this->child_executor_->GetOutputSchema());
      auto right_value = expr->Evaluate(&tpl2, this->child_executor_->GetOutputSchema());
      if (left_value.CompareLessThan(right_value) == CmpBool::CmpTrue) {
        return type != OrderByType::DESC;
      }
      if (left_value.CompareGreaterThan(right_value) == CmpBool::CmpTrue) {
        return type == OrderByType::DESC;
      }
    }
    return true;
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(compare)> pq(compare);
  Tuple tuple{};
  RID rid{};
  while (child_executor_->Next(&tuple, &rid)) {
    pq.emplace(tuple);
    if (pq.size() > plan_->GetN()) {
      pq.pop();
    }
  }
  while (!pq.empty()) {
    tuples_.emplace_back(pq.top());
    pq.pop();
  }
  it_ = tuples_.rbegin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (it_ != tuples_.rend()) {
    *tuple = *it_;
    ++it_;
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return std::distance(it_, tuples_.rend()); }

}  // namespace bustub
