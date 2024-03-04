#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Limit && optimized_plan->GetChildren()[0]->GetType() == PlanType::Sort) {
    auto limit_node = dynamic_cast<LimitPlanNode *>(optimized_plan.get());
    auto sort_node = dynamic_cast<const SortPlanNode *>(optimized_plan->GetChildren()[0].get());
    return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, sort_node->GetChildren()[0],
                                          sort_node->GetOrderBy(), limit_node->GetLimit());
  }
  return optimized_plan;
}

}  // namespace bustub
