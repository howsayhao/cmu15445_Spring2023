#include <algorithm>
#include <memory>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::RecursiveConvertAND(std::vector<AbstractExpressionRef> &expr_tuple_left,
                                    std::vector<AbstractExpressionRef> &expr_tuple_right,
                                    const AbstractExpressionRef &exprRef) -> bool {
  // std::cout << "<<<<<<<>>>>>>>>" << exprRef->ToString() << std::endl;
  if (const auto *andexpr = dynamic_cast<const LogicExpression *>(exprRef.get());
      andexpr != nullptr && andexpr->logic_type_ == LogicType::And) {
    if (RecursiveConvertAND(expr_tuple_left, expr_tuple_right, andexpr->children_[0]) &&
        RecursiveConvertAND(expr_tuple_left, expr_tuple_right, andexpr->children_[1])) {
      return true;
    }
  } else {
    auto expr = dynamic_cast<const ComparisonExpression *>(exprRef.get());
    if (IsPredicateTrue(exprRef)) {
      return true;
    }
    if ((expr == nullptr || expr->comp_type_ != ComparisonType::Equal)) {
      return false;
    }
    if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
        left_expr != nullptr) {
      if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
          right_expr != nullptr) {
        if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
          expr_tuple_left.emplace_back(
              std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType()));
          expr_tuple_right.emplace_back(
              std::make_shared<ColumnValueExpression>(1, right_expr->GetColIdx(), right_expr->GetReturnType()));
          return true;
        }
        if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
          expr_tuple_left.emplace_back(
              std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType()));
          expr_tuple_right.emplace_back(
              std::make_shared<ColumnValueExpression>(1, left_expr->GetColIdx(), left_expr->GetReturnType()));
          return true;
        }
      }
    }
  }
  return false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
  // BUSTUB_ENSURE(plan->GetType() == PlanType::NestedLoopJoin, "this function ONLY supports optimize nlj.");
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");

    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get());
        expr != nullptr && expr->comp_type_ == ComparisonType::Equal) {
      if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
          left_expr != nullptr) {
        if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
            right_expr != nullptr) {
          if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
            std::vector<AbstractExpressionRef> left_expr_tuple_0{};
            left_expr_tuple_0.emplace_back(
                std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType()));
            std::vector<AbstractExpressionRef> right_expr_tuple_1{};
            right_expr_tuple_1.emplace_back(
                std::make_shared<ColumnValueExpression>(1, right_expr->GetColIdx(), right_expr->GetReturnType()));
            return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                      nlj_plan.GetRightPlan(), std::move(left_expr_tuple_0),
                                                      std::move(right_expr_tuple_1), nlj_plan.GetJoinType());
          }
          if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
            std::vector<AbstractExpressionRef> left_expr_tuple_1{};
            left_expr_tuple_1.emplace_back(
                std::make_shared<ColumnValueExpression>(1, left_expr->GetColIdx(), left_expr->GetReturnType()));
            std::vector<AbstractExpressionRef> right_expr_tuple_0{};
            right_expr_tuple_0.emplace_back(
                std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType()));
            return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                      nlj_plan.GetRightPlan(), std::move(right_expr_tuple_0),
                                                      std::move(left_expr_tuple_1), nlj_plan.GetJoinType());
          }
        }
      }
    }
    // 完成多个AND的转换
    if (const auto *expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get());
        expr != nullptr && expr->logic_type_ == LogicType::And) {
      std::vector<AbstractExpressionRef> expr_tuple_0{};
      std::vector<AbstractExpressionRef> expr_tuple_1{};
      if (RecursiveConvertAND(expr_tuple_0, expr_tuple_1, expr->children_[0]) &&
          RecursiveConvertAND(expr_tuple_0, expr_tuple_1, expr->children_[1])) {
        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), std::move(expr_tuple_0),
                                                  std::move(expr_tuple_1), nlj_plan.GetJoinType());
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
