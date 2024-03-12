#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/arithmetic_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/nested_index_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "optimizer/optimizer.h"
#include "type/numeric_type.h"

// Note for 2023 Spring: You can add all optimizer rule implementations and apply the rules as you want in this file.
// Note that for some test cases, we force using starter rules, so that the configuration here won't take effects.
// Starter rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

namespace bustub {

auto Optimizer::CollectAllPredicates(std::vector<AbstractExpressionRef> &left_predicates,
                                     std::vector<AbstractExpressionRef> &right_predicates,
                                     std::vector<AbstractExpressionRef> &join_predicates,
                                     const AbstractExpressionRef &exprRef) -> bool {
  // std::cout << "<<<<<<<>>>>>>>>" << exprRef->ToString() << std::endl;
  if (IsPredicateTrue(exprRef)) {
    return true;
  }
  if (const auto *andexpr = dynamic_cast<const LogicExpression *>(exprRef.get());
      andexpr != nullptr && andexpr->logic_type_ == LogicType::And) {
    if (CollectAllPredicates(left_predicates, right_predicates, join_predicates, andexpr->children_[1]) &&
        CollectAllPredicates(left_predicates, right_predicates, join_predicates, andexpr->children_[0])) {
      return true;
    }
  } else {
    auto expr = dynamic_cast<const ComparisonExpression *>(exprRef.get());
    if (expr == nullptr) {
      return false;
    }
    auto left = dynamic_cast<const ColumnValueExpression *>(exprRef->children_[0].get());
    auto right = dynamic_cast<const ColumnValueExpression *>(exprRef->children_[1].get());
    if (left == nullptr && right == nullptr) {  // 默认常数折叠会处理这种情况，如果还有说明有问题
      return false;
    }
    if (left == nullptr || right == nullptr) {
      uint32_t tuple_idx = (left == nullptr) ? right->GetTupleIdx() : left->GetTupleIdx();
      if (tuple_idx == 0) {
        left_predicates.push_back(exprRef);
      } else {
        if (left == nullptr) {
          exprRef->children_[1] =
              std::make_shared<ColumnValueExpression>(0, right->GetColIdx(), right->GetReturnType());
        } else {
          exprRef->children_[0] = std::make_shared<ColumnValueExpression>(0, left->GetColIdx(), left->GetReturnType());
        }
        right_predicates.push_back(exprRef);
      }
      return true;
    }
    if (left->GetTupleIdx() == 0 && right->GetTupleIdx() == 0) {
      left_predicates.push_back(exprRef);
      return true;
    }
    if (left->GetTupleIdx() == 1 && right->GetTupleIdx() == 1) {
      right_predicates.push_back(exprRef);
      return true;
    }
    join_predicates.push_back(exprRef);
    return true;
  }

  return false;
}

auto Optimizer::PushToItsChild(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(child);
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    auto nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    auto left_child_plan = nlj_plan.GetLeftPlan();
    auto right_child_plan = nlj_plan.GetRightPlan();
    if ((left_child_plan->GetType() == PlanType::NestedLoopJoin || left_child_plan->GetType() == PlanType::SeqScan ||
         left_child_plan->GetType() == PlanType::MockScan) &&
        (right_child_plan->GetType() == PlanType::NestedLoopJoin || right_child_plan->GetType() == PlanType::SeqScan ||
         right_child_plan->GetType() == PlanType::MockScan)) {
      std::vector<AbstractExpressionRef> join_predicates;
      std::vector<AbstractExpressionRef> left_filter_predicates;
      std::vector<AbstractExpressionRef> right_filter_predicates;
      /** collect all predicate_expression from nlj_plan */
      // if (const auto *andexpr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get());
      // andexpr != nullptr && andexpr->logic_type_ == LogicType::And) {
      // if (!CollectAllPredicates(left_filter_predicates, right_filter_predicates, join_predicates,
      // nlj_plan.Predicate()->children_[1], nlj_plan.GetLeftPlan()->OutputSchema().GetColumnCount(),
      // nlj_plan.GetRightPlan()->OutputSchema().GetColumnCount())) {
      // 只支持对所有predicate之间都是AND关系的转换
      // return optimized_plan;
      // }
      // } else {  //
      // 应该是所有的predicate开始都是默认一个true，那么这种情况本来就是要直接不下推的，直接return即可，虽然结果都一样
      if (!CollectAllPredicates(left_filter_predicates, right_filter_predicates, join_predicates, nlj_plan.Predicate())) {
        // 只支持对所有predicate之间都是AND关系的转换
        return optimized_plan;
      }
      if (left_filter_predicates.empty() && right_filter_predicates.empty()) {
        return optimized_plan;
      }
      // }
      AbstractExpressionRef left_filter_predicate;
      AbstractExpressionRef right_filter_predicate;
      while (!left_filter_predicates.empty()) {
        if (left_filter_predicate == nullptr) {
          left_filter_predicate = left_filter_predicates.back();
        } else {
          left_filter_predicate =
              std::make_shared<LogicExpression>(left_filter_predicate, left_filter_predicates.back(), LogicType::And);
        }
        left_filter_predicates.pop_back();
      }
      if (left_filter_predicate != nullptr) {
        nlj_plan.children_[0] = std::make_shared<FilterPlanNode>(nlj_plan.children_[0]->output_schema_,
                                                                 left_filter_predicate, nlj_plan.children_[0]);
        nlj_plan.children_[0] = OptimizeMergeFilterNLJ(nlj_plan.children_[0]);
      }
      while (!right_filter_predicates.empty()) {
        if (right_filter_predicate == nullptr) {
          right_filter_predicate = right_filter_predicates.back();
        } else {
          right_filter_predicate =
              std::make_shared<LogicExpression>(right_filter_predicate, right_filter_predicates.back(), LogicType::And);
        }
        right_filter_predicates.pop_back();
      }
      if (right_filter_predicate != nullptr) {
        nlj_plan.children_[1] = std::make_shared<FilterPlanNode>(nlj_plan.children_[1]->output_schema_,
                                                                 right_filter_predicate, nlj_plan.children_[1]);
        nlj_plan.children_[1] = OptimizeMergeFilterNLJ(nlj_plan.children_[1]);
      }
      /** build a new one */
      AbstractExpressionRef join_predicate;
      while (!join_predicates.empty()) {
        if (join_predicate == nullptr) {
          join_predicate = join_predicates.back();
        } else {
          join_predicate = std::make_shared<LogicExpression>(join_predicate, join_predicates.back(), LogicType::And);
        }
        join_predicates.pop_back();
      }
      if (join_predicate == nullptr) {
        join_predicate = std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(true));
      }
      return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, PushToItsChild(nlj_plan.children_[0]),
                                                      PushToItsChild(nlj_plan.children_[1]), join_predicate,
                                                      nlj_plan.GetJoinType());
    }
  }

  return optimized_plan;
}

auto Optimizer::OptimizePredicatePushDown(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizePredicatePushDown(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    if (IsPredicateTrue(nlj_plan.Predicate())) {
      return optimized_plan;
    }
    return PushToItsChild(plan);
  }

  return optimized_plan;
}

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeMergeProjection(p);     // omit no-need projection
  p = OptimizeMergeFilterNLJ(p);      // filter+nlj -> nlj'
  p = OptimizePredicatePushDown(p);   // push down predicates  // hao
  p = OptimizeNLJAsHashJoin(p);       // nlj -> hash_join
  p = OptimizeOrderByAsIndexScan(p);  // order-by -> index_scan
  p = OptimizeSortLimitAsTopN(p);     // sort+limit -> topn
  // no NILASINDEXSCAN
  p = OptimizeMergeFilterScan(p);  // filter+seq_scan -> seq_scan(filter) // hao
  return p;
}

}  // namespace bustub
