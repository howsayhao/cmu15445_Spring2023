#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include "catalog/column.h"
#include "common/macros.h"
#include "execution/executors/index_scan_executor.h"
#include "execution/executors/seq_scan_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/arithmetic_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/nested_index_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/values_plan.h"
#include "optimizer/optimizer.h"
#include "type/numeric_type.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"

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
      // 应该是所有的predicate开始都是默认一个true，那么这种情况本来就是要直接不下推的，直接return即可，虽然结果都一样
      if (!CollectAllPredicates(left_filter_predicates, right_filter_predicates, join_predicates,
                                nlj_plan.Predicate())) {
        // 只支持对所有predicate之间都是AND关系的转换
        return optimized_plan;
      }
      if (left_filter_predicates.empty() && right_filter_predicates.empty()) {
        return optimized_plan;
      }
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

auto Optimizer::IsConstValue(Value &value, const AbstractExpressionRef &exprRef) -> bool {
  if (const auto *arithmetic_expr = dynamic_cast<const ArithmeticExpression *>(exprRef.get());
      arithmetic_expr != nullptr) {
    Value val0;
    Value val1;
    if (IsConstValue(val0, exprRef->children_[0]) && IsConstValue(val1, exprRef->children_[1])) {
      if (val0.IsNull() || val1.IsNull()) {
        return false;
      }
      switch (arithmetic_expr->compute_type_) {
        case ArithmeticType::Plus:
          value = ValueFactory::GetIntegerValue(val0.GetAs<int32_t>() + val1.GetAs<int32_t>());
          break;
        case ArithmeticType::Minus:
          value = ValueFactory::GetIntegerValue(val0.GetAs<int32_t>() - val1.GetAs<int32_t>());
          break;
        default:
          UNREACHABLE("Unsupported arithmetic type.");
      }
      return true;
    }
  } else {
    if (const auto *const_value = dynamic_cast<const ConstantValueExpression *>(exprRef.get());
        const_value != nullptr) {
      value = const_value->val_;
      return true;
    }
  }
  return false;
}

auto Optimizer::FoldPredicate(std::vector<AbstractExpressionRef> &expr, const AbstractExpressionRef &exprRef) -> bool {
  if (const auto *andexpr = dynamic_cast<const LogicExpression *>(exprRef.get());
      andexpr != nullptr && andexpr->logic_type_ == LogicType::And) {
    if (FoldPredicate(expr, exprRef->children_[0]) && FoldPredicate(expr, exprRef->children_[1])) {
      return true;
    }
  } else {
    if (IsPredicateTrue(exprRef)) {
      return true;
    }
    if (const auto *cmpexpr = dynamic_cast<const ComparisonExpression *>(exprRef.get()); cmpexpr != nullptr) {
      Value left_const;
      Value right_const;
      auto left_expr = cmpexpr->children_[0];
      auto right_expr = cmpexpr->children_[1];
      CmpBool predicate;
      if (IsConstValue(left_const, left_expr) && IsConstValue(right_const, right_expr)) {
        switch (cmpexpr->comp_type_) {
          case ComparisonType::Equal:
            predicate = left_const.CompareEquals(right_const);
            break;
          case ComparisonType::NotEqual:
            predicate = left_const.CompareNotEquals(right_const);
            break;
          case ComparisonType::LessThan:
            predicate = left_const.CompareLessThan(right_const);
            break;
          case ComparisonType::LessThanOrEqual:
            predicate = left_const.CompareLessThanEquals(right_const);
            break;
          case ComparisonType::GreaterThan:
            predicate = left_const.CompareGreaterThan(right_const);
            break;
          case ComparisonType::GreaterThanOrEqual:
            predicate = left_const.CompareGreaterThanEquals(right_const);
            break;
          default:
            BUSTUB_ASSERT(false, "Unsupported comparison type.");
        }
        if (predicate == CmpBool::CmpFalse) {
          expr.push_back(std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(false)));
        }
        return true;
      }
      expr.push_back(exprRef);
      return true;
    }
  }
  return false;
}

auto Optimizer::IsPredicateFalse(const AbstractExpressionRef &expr) -> bool {
  if (const auto *const_expr = dynamic_cast<const ConstantValueExpression *>(expr.get()); const_expr != nullptr) {
    return !const_expr->val_.CastAs(TypeId::BOOLEAN).GetAs<bool>();
  }
  return false;
}

auto Optimizer::OptimizeConstantFolder(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeConstantFolder(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // 只处理全部由AND连接的，否则返回false
  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    std::vector<AbstractExpressionRef> expr{};
    if (!FoldPredicate(expr, filter_plan.GetPredicate())) {
      return optimized_plan;
    }
    AbstractExpressionRef predicate;
    while (!expr.empty()) {
      if (IsPredicateFalse(expr.back())) {
        predicate = std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(false));
        break;
      }
      if (predicate == nullptr) {
        predicate = expr.back();
      } else {
        predicate = std::make_shared<LogicExpression>(predicate, expr.back(), LogicType::And);
      }
      expr.pop_back();
    }
    if (predicate == nullptr) {
      predicate = std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(true));
    }
    if (IsPredicateTrue(predicate)) {
      return filter_plan.children_[0]->CloneWithChildren(filter_plan.children_[0]->GetChildren());
    }
    return std::make_shared<FilterPlanNode>(filter_plan.output_schema_, predicate, filter_plan.children_[0]);
  }
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    std::vector<AbstractExpressionRef> expr{};
    if (!FoldPredicate(expr, nlj_plan.Predicate())) {
      return optimized_plan;
    }
    AbstractExpressionRef predicate;
    while (!expr.empty()) {
      if (IsPredicateFalse(expr.back())) {
        predicate = std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(false));
        break;
      }
      if (predicate == nullptr) {
        predicate = expr.back();
      } else {
        predicate = std::make_shared<LogicExpression>(predicate, expr.back(), LogicType::And);
      }
      expr.pop_back();
    }
    if (predicate == nullptr) {
      predicate = std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(true));
    }
    return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                    nlj_plan.GetRightPlan(), predicate, nlj_plan.GetJoinType());
  }

  return optimized_plan;
}

auto Optimizer::SideConcerned(bool &side_0, bool &side_1, const AbstractExpressionRef &exprRef) -> bool {
  if (IsPredicateTrue(exprRef)) {
    return true;
  }
  if (side_0 && side_1) {
    return true;
  }
  if (const auto *andexpr = dynamic_cast<const LogicExpression *>(exprRef.get());
      andexpr != nullptr && andexpr->logic_type_ == LogicType::And) {
    if (SideConcerned(side_0, side_1, exprRef->children_[0]) && SideConcerned(side_0, side_1, exprRef->children_[1])) {
      return true;
    }
  } else {
    auto expr = dynamic_cast<const ComparisonExpression *>(exprRef.get());
    if (expr == nullptr) {
      return false;
    }
    if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
        left_expr != nullptr) {
      if (left_expr->GetTupleIdx() == 0) {
        side_0 = true;
      } else {
        side_1 = true;
      }
    }
    if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
        right_expr != nullptr) {
      if (right_expr->GetTupleIdx() == 0) {
        side_0 = true;
      } else {
        side_1 = true;
      }
    }
    return true;
  }

  return false;
}

auto Optimizer::OptimizeNullFolder(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNullFolder(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    const auto &left_child_plan = *optimized_plan->children_[0];
    const auto &right_child_plan = *optimized_plan->children_[1];
    bool child0_concern;
    bool child1_concern;
    // 只考虑全部由AND连接的空值折叠
    if (!SideConcerned(child0_concern, child1_concern, nlj_plan.Predicate())) {
      return optimized_plan;
    }
    if (left_child_plan.GetType() == PlanType::Filter) {
      const auto &left_filter_plan = dynamic_cast<const FilterPlanNode &>(left_child_plan);
      if (IsPredicateFalse(left_filter_plan.GetPredicate()) && child0_concern) {
        return std::make_shared<NestedLoopJoinPlanNode>(
            nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
            std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(false)), nlj_plan.GetJoinType());
      }
    }
    if (right_child_plan.GetType() == PlanType::Filter) {
      const auto &right_filter_plan = dynamic_cast<const FilterPlanNode &>(right_child_plan);
      if (IsPredicateFalse(right_filter_plan.GetPredicate()) && child1_concern) {
        return std::make_shared<NestedLoopJoinPlanNode>(
            nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
            std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(false)), nlj_plan.GetJoinType());
      }
    }
  }

  return optimized_plan;
}

auto Optimizer::OptimizeEliminateAggregates(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  /** 需要实施列裁剪的往往都是projection的儿子节点往后，或者说如果第一个节点不是projection那么我就不做列裁剪了
      考虑到projection本身做的就是列裁剪的工作，似乎多造几个projection节点往下推就好了，
      当然这改变了儿子节点的outputschema，需要将路径上节点都修改。 */
  /** 但这些我目前都不考虑做，我目前要解决掉的是针对agg重复计算了过多的相同列值，意即，
      我要砍的是agg的重复列，而这个的裁剪逻辑是和一般列裁剪不一样的,
      目前也不准备处理agg的处理不是columnvalue的类型 */
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeEliminateAggregates(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Projection) {
    const auto &proj_plan = dynamic_cast<const ProjectionPlanNode &>(*optimized_plan);
    const auto &child_plan = *optimized_plan->children_[0];
    if (child_plan.GetType() == PlanType::Aggregation) {
      const auto &agg_plan = dynamic_cast<const AggregationPlanNode &>(child_plan);
      /** 目前也不准备处理agg的处理不是columnvalue的类型 */
      for (const auto &agg_expr : agg_plan.aggregates_) {
        if (const auto *my_expr = dynamic_cast<const ColumnValueExpression *>(agg_expr.get()); my_expr != nullptr) {
          if (my_expr->GetReturnType() != TypeId::INTEGER) {
            return optimized_plan;
          }
          continue;
        }
        return optimized_plan;
      }
      if (agg_plan.group_bys_.size() != 1) {
        return optimized_plan;
      }
      if (const auto *group = dynamic_cast<const ColumnValueExpression *>(agg_plan.group_bys_.back().get());
          group == nullptr) {
        return plan;
      }
      /** 将agg的各列归类 */
      std::unordered_map<ColumnKey, ColumnValue> ht;
      for (uint32_t col = 1; col <= agg_plan.agg_types_.size(); col++) {
        if (const auto *col_expr = dynamic_cast<const ColumnValueExpression *>(agg_plan.aggregates_[col - 1].get());
            col_expr != nullptr) {
          auto key = ColumnKey(agg_plan.agg_types_[col - 1], col_expr->GetColIdx());
          if (ht.count(key) == 0) {
            ht.insert({key, ColumnValue(col)});
          } else {
            ht[key].col_idx_.push_back(col);
            if (col < ht[key].min_col_idx_) {
              ht[key].min_col_idx_ = col;
            }
          }
        }
      }
      std::vector<std::pair<ColumnKey, ColumnValue>> columns(ht.begin(), ht.end());
      std::sort(columns.begin(), columns.end(),
                [](std::pair<ColumnKey, ColumnValue> &t1, std::pair<ColumnKey, ColumnValue> &t2) {
                  return t1.second.min_col_idx_ < t2.second.min_col_idx_;
                });
      /** 更改agg的列和输出格式 */
      uint32_t idx = 0;
      std::vector<AbstractExpressionRef> cut_aggregates;
      std::vector<AggregationType> cut_types;
      std::vector<Column> cut_schema;
      cut_schema.emplace_back(agg_plan.OutputSchema().GetColumns()[0]);
      for (const auto &column : columns) {
        cut_types.emplace_back(column.first.agg_type_);
        cut_aggregates.emplace_back(std::make_shared<ColumnValueExpression>(0, column.first.col_idx_, TypeId::INTEGER));
        cut_schema.push_back(agg_plan.OutputSchema().GetColumns()[column.second.min_col_idx_]);
        ht[column.first].min_col_idx_ = ++idx;
      }
      /** 更改projection的处理逻辑，合并同类 */
      std::vector<AbstractExpressionRef> cut_exprs;
      if (!MergeProjectionExpr(ht, cut_exprs, proj_plan.expressions_)) {
        return optimized_plan;
      }
      /** 返回新plan结点 */
      return std::make_shared<ProjectionPlanNode>(
          proj_plan.output_schema_, cut_exprs,
          std::make_shared<AggregationPlanNode>(std::make_shared<Schema>(cut_schema), agg_plan.GetChildPlan(),
                                                agg_plan.GetGroupBys(), cut_aggregates, cut_types));
    }
  }

  return optimized_plan;
}

auto Optimizer::MergeProjectionExpr(std::unordered_map<ColumnKey, ColumnValue> &ht,
                                    std::vector<AbstractExpressionRef> &expr,
                                    const std::vector<AbstractExpressionRef> &exprRef) -> bool {
  for (auto &column_expr : exprRef) {
    if (const auto *col_expr = dynamic_cast<const ColumnValueExpression *>(column_expr.get()); col_expr != nullptr) {
      auto idx = col_expr->GetColIdx();
      if (idx == 0) {
        expr.emplace_back(std::make_shared<ColumnValueExpression>(0, 0, TypeId::INTEGER));
        continue;
      }
      for (auto i : ht) {
        if (i.second.IsMatch(idx)) {
          expr.emplace_back(std::make_shared<ColumnValueExpression>(0, i.second.min_col_idx_, TypeId::INTEGER));
          break;
        }
      }
    } else if (const auto *ari_expr = dynamic_cast<const ArithmeticExpression *>(column_expr.get());
               ari_expr != nullptr) {
      std::vector<AbstractExpressionRef> arithmetic;
      arithmetic.emplace_back(ari_expr->children_[0]);
      arithmetic.emplace_back(ari_expr->children_[1]);
      std::vector<AbstractExpressionRef> exprs;
      if (!MergeProjectionExpr(ht, exprs, arithmetic)) {
        return false;
      }
      BUSTUB_ENSURE(exprs.size() == 2, "TWO CHILD for Arithmetic");
      expr.emplace_back(std::make_shared<ArithmeticExpression>(exprs.front(), exprs.back(), ari_expr->compute_type_));
    } else {
      return false;
    }
  }
  return true;
}

auto Optimizer::OptimizeColumnCut(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(child);
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  /** 所谓列裁剪主要是比对当前PlanNode的输出项与输入项，删去与输出项不相干的无关输入项
      目前仅准备完成projection结点和aggregate结点的列裁剪
      删去无关输入项的同时也需要改变子节点的输出格式，之后往下传递继续裁剪 */
  if (optimized_plan->GetType() == PlanType::Projection) {
    const auto &projection_plan = dynamic_cast<const ProjectionPlanNode &>(*optimized_plan);
    auto output_schema = projection_plan.output_schema_;
    auto expressions = projection_plan.expressions_;
    auto input_schema = projection_plan.children_[0]->output_schema_;
    bool necessary[input_schema->GetColumnCount()];
    for (uint32_t col = 0; col < input_schema->GetColumnCount(); col++) {
      necessary[col] = false;
    }
    BUSTUB_ENSURE(expressions.size() == output_schema->GetColumnCount(), "should be same.");
    for (uint32_t col = 0; col < projection_plan.GetExpressions().size(); col++) {
      if (!NoteNecessary(necessary, expressions[col])) {
        return optimized_plan;
      }
    }
    /** 没必要转换就不转换了 */
    for (uint32_t col = 0; col < input_schema->GetColumnCount(); col++) {
      if (col + 1 == input_schema->GetColumnCount() && necessary[col]) {
        return optimized_plan;
      }
      if (!necessary[col]) {
        break;
      }
    }
    /** 仅适用于儿子结点是这两种的 */
    if (!(projection_plan.GetChildPlan()->GetType() == PlanType::Projection ||
          projection_plan.GetChildPlan()->GetType() == PlanType::Aggregation)) {
      return optimized_plan;
    }
    std::vector<AbstractExpressionRef> cut_expression;
    /** 修改结点表达式，因为下层传来的column_id变化了
        如果想要转换的表达式比较特殊，那就不转换了 */
    if (!ReorderNodeExpr(necessary, input_schema->GetColumnCount(), cut_expression, projection_plan.expressions_)) {
      return optimized_plan;
    }
    if (projection_plan.GetChildPlan()->GetType() == PlanType::Aggregation) {
      const auto &aggregate_plan = dynamic_cast<const AggregationPlanNode &>(*projection_plan.GetChildPlan().get());
      if (aggregate_plan.group_bys_.size() != 1) {
        return optimized_plan;
      }
      if (const auto *group = dynamic_cast<const ColumnValueExpression *>(aggregate_plan.group_bys_.back().get());
          group == nullptr) {
        return optimized_plan;
      }
    }
    /** 向下传递裁剪，最后返回结果 */
    return std::make_shared<ProjectionPlanNode>(
        projection_plan.output_schema_, cut_expression,
        ReorderChildSchema(necessary, input_schema->GetColumnCount(), projection_plan.children_[0]));
  }
  return optimized_plan;
}

auto Optimizer::NoteNecessary(bool necessary[], const AbstractExpressionRef &exprRef) -> bool {
  if (const auto *col_expr = dynamic_cast<const ColumnValueExpression *>(exprRef.get()); col_expr != nullptr) {
    necessary[col_expr->GetColIdx()] = true;
    return true;
  }
  if (const auto *ari_expr = dynamic_cast<const ArithmeticExpression *>(exprRef.get()); ari_expr != nullptr) {
    if (NoteNecessary(necessary, ari_expr->children_[0]) && NoteNecessary(necessary, ari_expr->children_[1])) {
      return true;
    }
  }
  return false;
}

auto Optimizer::RecursiveOrderNodeExpr(uint32_t off_set, uint32_t col, std::vector<AbstractExpressionRef> &plan)
    -> bool {
  std::vector<AbstractExpressionRef> expr;
  for (auto &column_expr : plan) {
    if (const auto col_expr = dynamic_cast<const ColumnValueExpression *>(column_expr.get()); col_expr != nullptr) {
      if (col_expr->GetColIdx() == col) {
        expr.emplace_back(std::make_shared<ColumnValueExpression>(0, col - off_set, col_expr->GetReturnType()));
        continue;
      }
    } else if (const auto *ari_expr = dynamic_cast<const ArithmeticExpression *>(column_expr.get());
               ari_expr != nullptr) {
      std::vector<AbstractExpressionRef> arithmetic;
      arithmetic.emplace_back(ari_expr->children_[0]);
      arithmetic.emplace_back(ari_expr->children_[1]);
      if (!RecursiveOrderNodeExpr(off_set, col, arithmetic)) {
        return false;
      }
      expr.emplace_back(
          std::make_shared<ArithmeticExpression>(arithmetic.front(), arithmetic.back(), ari_expr->compute_type_));
      continue;
    } else {
      return false;
    }
    expr.emplace_back(column_expr);
  }
  plan = expr;
  return true;
}

auto Optimizer::ReorderNodeExpr(const bool necessary[], uint32_t size, std::vector<AbstractExpressionRef> &expr,
                                const std::vector<AbstractExpressionRef> &exprRef) -> bool {
  uint32_t off_set = 0;
  expr = exprRef;
  for (uint32_t col = 0; col < size; col++) {
    if (!necessary[col]) {
      off_set++;
    } else {
      if (off_set != 0) {
        if (!RecursiveOrderNodeExpr(off_set, col, expr)) {
          return false;
        }
      }
    }
  }
  return true;
}

auto Optimizer::ReorderChildSchema(const bool necessary[], uint32_t size, const AbstractPlanNodeRef &plan)
    -> AbstractPlanNodeRef {
  if (plan->GetType() == PlanType::Projection) {
    const auto &projection_plan = dynamic_cast<const ProjectionPlanNode &>(*plan);
    auto output_schema = plan->output_schema_;
    std::vector<AbstractExpressionRef> expressions;
    std::vector<Column> schema;
    for (uint32_t col = 0; col < output_schema->GetColumnCount(); col++) {
      if (necessary[col]) {
        expressions.emplace_back(projection_plan.expressions_[col]);
        schema.emplace_back(projection_plan.OutputSchema().GetColumns()[col]);
      }
    }
    return OptimizeColumnCut(std::make_shared<ProjectionPlanNode>(std::make_shared<Schema>(schema), expressions,
                                                                  projection_plan.children_[0]));
  }
  if (plan->GetType() == PlanType::Aggregation) {
    const auto &aggregate_plan = dynamic_cast<const AggregationPlanNode &>(*plan);
    auto output_schema = plan->output_schema_;
    std::vector<AggregationType> agg_types;
    std::vector<AbstractExpressionRef> aggregates;
    std::vector<Column> schema;
    if (aggregate_plan.group_bys_.size() != 1) {
      return plan;
    }
    if (const auto *group = dynamic_cast<const ColumnValueExpression *>(aggregate_plan.group_bys_.back().get());
        group == nullptr) {
      return plan;
    }
    for (uint32_t col = 0; col < output_schema->GetColumnCount(); col++) {
      if (necessary[col]) {
        if (col != 0) {
          agg_types.emplace_back(aggregate_plan.agg_types_[col - 1]);
          aggregates.emplace_back(aggregate_plan.aggregates_[col - 1]);
        }
        schema.emplace_back(aggregate_plan.OutputSchema().GetColumns()[col]);
      }
    }
    return OptimizeColumnCut(
        std::make_shared<AggregationPlanNode>(std::make_shared<Schema>(schema), aggregate_plan.children_[0],
                                              aggregate_plan.GetGroupBys(), aggregates, agg_types));
  }
  return plan;
}

auto Optimizer::OptimizeFalseFilterAsNullValue(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeFalseFilterAsNullValue(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    if (IsPredicateFalse(filter_plan.GetPredicate())) {
      std::vector<std::vector<AbstractExpressionRef>> values{};
      std::vector<AbstractExpressionRef> single_value{};
      std::vector<Column> schema{};
      for (uint32_t col = 0; col < filter_plan.output_schema_->GetColumnCount(); col++) {
        single_value.emplace_back(std::make_shared<ConstantValueExpression>(
            ValueFactory::GetNullValueByType(filter_plan.output_schema_->GetColumn(col).GetType())));
        schema.emplace_back(filter_plan.output_schema_->GetColumn(col));
      }
      values.emplace_back(single_value);
      return std::make_shared<ValuesPlanNode>(std::make_shared<Schema>(schema), values);
    }
  }
  return optimized_plan;
}

auto Optimizer::OptimizeIndexRange(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeIndexRange(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seqscan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if (seqscan_plan.filter_predicate_ != nullptr) {
      /** q1建立了一个2-column的索引
          因而我目前只准备处理(y>=/=/> const, x>=/=/> const)的情况，尽量简化，快速通过 */
      if (const auto *logical_expr = dynamic_cast<const LogicExpression *>(seqscan_plan.filter_predicate_.get());
          logical_expr != nullptr && logical_expr->logic_type_ == LogicType::And) {
        auto left_expr = logical_expr->children_[0];
        auto right_expr = logical_expr->children_[1];
        if (const auto *left_cmp_expr = dynamic_cast<const ComparisonExpression *>(left_expr.get());
            left_cmp_expr != nullptr && (left_cmp_expr->comp_type_ == ComparisonType::Equal ||
                                         left_cmp_expr->comp_type_ == ComparisonType::GreaterThan ||
                                         left_cmp_expr->comp_type_ == ComparisonType::GreaterThanOrEqual)) {
          if (const auto *right_cmp_expr = dynamic_cast<const ComparisonExpression *>(right_expr.get());
              right_cmp_expr != nullptr && (right_cmp_expr->comp_type_ == ComparisonType::Equal ||
                                            right_cmp_expr->comp_type_ == ComparisonType::GreaterThan ||
                                            right_cmp_expr->comp_type_ == ComparisonType::GreaterThanOrEqual)) {
            const auto *left_column = dynamic_cast<const ColumnValueExpression *>(left_cmp_expr->children_[0].get());
            const auto *left_const = dynamic_cast<const ConstantValueExpression *>(left_cmp_expr->children_[1].get());
            const auto *right_column = dynamic_cast<const ColumnValueExpression *>(right_cmp_expr->children_[0].get());
            const auto *right_const = dynamic_cast<const ConstantValueExpression *>(right_cmp_expr->children_[1].get());
            if (left_column != nullptr && left_const != nullptr && right_column != nullptr && right_const != nullptr) {
              const auto *table_info = catalog_.GetTable(seqscan_plan.GetTableOid());
              const auto indices = catalog_.GetTableIndexes(table_info->name_);
              std::vector<uint32_t> column_ids;
              column_ids.emplace_back(left_column->GetColIdx());
              column_ids.emplace_back(right_column->GetColIdx());
              for (const auto *index : indices) {
                const auto &columns = index->key_schema_.GetColumns();
                if (columns.size() == 2) {
                  if (columns[0].GetName() == table_info->schema_.GetColumn(column_ids[0]).GetName() &&
                      columns[1].GetName() == table_info->schema_.GetColumn(column_ids[1]).GetName()) {
                    std::vector<Value> values{left_const->val_, right_const->val_};
                    return std::make_shared<IndexScanPlanNode>(seqscan_plan.output_schema_, index->index_oid_,
                                                               seqscan_plan.filter_predicate_, values);
                  }
                  if (columns[0].GetName() == table_info->schema_.GetColumn(column_ids[1]).GetName() &&
                      columns[1].GetName() == table_info->schema_.GetColumn(column_ids[0]).GetName()) {
                    std::vector<Value> values{right_const->val_, left_const->val_};
                    return std::make_shared<IndexScanPlanNode>(seqscan_plan.output_schema_, index->index_oid_,
                                                               seqscan_plan.filter_predicate_, values);
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  return optimized_plan;
}

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeMergeProjection(p);         // omit no-need projection
  p = OptimizeConstantFolder(p);          // 常数折叠  // hao
  p = OptimizeNullFolder(p);              // 空值折叠  // hao
  p = OptimizeEliminateAggregates(p);     // aggregate重复列裁剪    // hao
  p = OptimizeColumnCut(p);               // 列裁剪  // hao
  p = OptimizeMergeProjection(p);         // omit no-need projection
  p = OptimizeMergeFilterNLJ(p);          // filter+nlj -> nlj'
  p = OptimizePredicatePushDown(p);       // 谓语下推  // hao
  p = OptimizeFalseFilterAsNullValue(p);  // 源截流  // hao
  p = OptimizeNLJAsHashJoin(p);           // nlj -> hash_join
  p = OptimizeOrderByAsIndexScan(p);      // order-by -> index_scan
  p = OptimizeSortLimitAsTopN(p);         // sort+limit -> topn
  // no NILASINDEXSCAN
  p = OptimizeMergeFilterScan(p);  // filter+seq_scan -> seq_scan(filter) // hao
  p = OptimizeIndexRange(p);       // 范围索引 seq_scan(filter) -> index_scan
  // p = OptimizeSelectIndexScan(p);
  return p;
}

}  // namespace bustub
