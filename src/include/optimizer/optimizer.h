#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/aggregation_plan.h"

namespace bustub {

struct ColumnKey {
  /** The group-by values */
  AggregationType agg_type_{};
  uint32_t col_idx_{};  // origin col_idx
  ColumnKey(AggregationType type, uint32_t idx) : agg_type_(type), col_idx_(idx) {}
  auto operator==(const ColumnKey &other) const -> bool {
    if (agg_type_ == other.agg_type_) {
      if (col_idx_ == other.col_idx_) {
        return true;
      }
    }
    return false;
  }
};

struct ColumnValue {
  std::vector<uint32_t> col_idx_{};
  uint32_t min_col_idx_{0};

  ColumnValue() = default;
  explicit ColumnValue(uint32_t idx) {
    col_idx_.push_back(idx);
    min_col_idx_ = idx;
  }
  auto GetMinColIdx() -> uint32_t {
    if (col_idx_.empty()) {
      return -1;
    }
    return min_col_idx_;
  }
  auto IsMatch(uint32_t col) -> bool { return std::find(col_idx_.begin(), col_idx_.end(), col) != col_idx_.end(); }
};

/**
 * The optimizer takes an `AbstractPlanNode` and outputs an optimized `AbstractPlanNode`.
 */
class Optimizer {
 public:
  explicit Optimizer(const Catalog &catalog, bool force_starter_rule)
      : catalog_(catalog), force_starter_rule_(force_starter_rule) {}

  auto Optimize(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

 private:
  /**
   * @brief merge projections that do identical project.
   * Identical projection might be produced when there's `SELECT *`, aggregation, or when we need to rename the columns
   * in the planner. We merge these projections so as to make execution faster.
   */
  auto OptimizeMergeProjection(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief merge filter condition into nested loop join.
   * In planner, we plan cross join + filter with cross product (done with nested loop join) and a filter plan node. We
   * can merge the filter condition into nested loop join to achieve better efficiency.
   */
  auto OptimizeMergeFilterNLJ(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief optimize nested loop join into hash join.
   * In the starter code, we will check NLJs with exactly one equal condition. You can further support optimizing joins
   * with multiple eq conditions.
   */
  auto OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief optimize nested loop join into index join.
   */
  auto OptimizeNLJAsIndexJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief eliminate always true filter
   */
  auto OptimizeEliminateTrueFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief merge filter into filter_predicate of seq scan plan node
   */
  auto OptimizeMergeFilterScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief rewrite expression to be used in nested loop joins. e.g., if we have `SELECT * FROM a, b WHERE a.x = b.y`,
   * we will have `#0.x = #0.y` in the filter plan node. We will need to figure out where does `0.x` and `0.y` belong
   * in NLJ (left table or right table?), and rewrite it as `#0.x = #1.y`.
   *
   * @param expr the filter expression
   * @param left_column_cnt number of columns in the left size of the NLJ
   * @param right_column_cnt number of columns in the left size of the NLJ
   */
  auto RewriteExpressionForJoin(const AbstractExpressionRef &expr, size_t left_column_cnt, size_t right_column_cnt)
      -> AbstractExpressionRef;

  /** @brief check if the predicate is true::boolean */
  auto IsPredicateTrue(const AbstractExpressionRef &expr) -> bool;

  /**
   * @brief optimize order by as index scan if there's an index on a table
   */
  auto OptimizeOrderByAsIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /** @brief check if the index can be matched */
  auto MatchIndex(const std::string &table_name, uint32_t index_key_idx)
      -> std::optional<std::tuple<index_oid_t, std::string>>;

  /**
   * @brief optimize sort + limit as top N
   */
  auto OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief get the estimated cardinality for a table based on the table name. Useful when join reordering. BusTub
   * doesn't support statistics for now, so it's the only way for you to get the table size :(
   *
   * @param table_name
   * @return std::optional<size_t>
   */
  auto EstimatedCardinality(const std::string &table_name) -> std::optional<size_t>;

  // leaderboard  //TODO
  auto RecursiveConvertAND(std::vector<AbstractExpressionRef> &expr_tuple_left,
                           std::vector<AbstractExpressionRef> &expr_tuple_right, const AbstractExpressionRef &exprRef)
      -> bool;
  auto OptimizePredicatePushDown(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;
  auto PushToItsChild(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;
  auto CollectAllPredicates(std::vector<AbstractExpressionRef> &left_predicates,
                            std::vector<AbstractExpressionRef> &right_predicates,
                            std::vector<AbstractExpressionRef> &join_predicates, const AbstractExpressionRef &exprRef)
      -> bool;
  auto OptimizeConstantFolder(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;
  auto FoldPredicate(std::vector<AbstractExpressionRef> &expr, const AbstractExpressionRef &exprRef) -> bool;
  auto IsConstValue(Value &value, const AbstractExpressionRef &exprRef) -> bool;
  auto IsPredicateFalse(const AbstractExpressionRef &expr) -> bool;
  auto OptimizeNullFolder(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;
  auto SideConcerned(bool &side_0, bool &side_1, const AbstractExpressionRef &exprRef) -> bool;
  auto OptimizeEliminateAggregates(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;
  auto MergeProjectionExpr(std::unordered_map<ColumnKey, ColumnValue> &ht, std::vector<AbstractExpressionRef> &expr,
                           const std::vector<AbstractExpressionRef> &exprRef) -> bool;
  auto OptimizeColumnCut(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;
  auto NoteNecessary(bool necessary[], const AbstractExpressionRef &exprRef) -> bool;
  auto ReorderChildSchema(const bool necessary[], uint32_t size, const AbstractPlanNodeRef &plan)
      -> AbstractPlanNodeRef;
  auto ReorderNodeExpr(const bool necessary[], uint32_t size, std::vector<AbstractExpressionRef> &expr,
                       const std::vector<AbstractExpressionRef> &exprRef) -> bool;
  auto RecursiveOrderNodeExpr(uint32_t off_set, uint32_t col, std::vector<AbstractExpressionRef> &plan) -> bool;
  auto OptimizeFalseFilterAsNullValue(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;
  auto OptimizeIndexRange(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;
  auto OptimizeSelectIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;
  auto MatchTwoKeysIndex(const std::string &table_name, const std::vector<uint32_t> &index_key_idxs)
      -> std::optional<std::tuple<index_oid_t, std::string>>;
  /** Catalog will be used during the planning process. USERS SHOULD ENSURE IT OUTLIVES
   * OPTIMIZER, otherwise it's a dangling reference.
   */
  const Catalog &catalog_;

  const bool force_starter_rule_;
};

}  // namespace bustub

namespace std {
template <>
struct std::hash<bustub::ColumnKey> {
  auto operator()(const bustub::ColumnKey &agg_key) const -> std::size_t {
    return std::hash<bustub::AggregationType>()(agg_key.agg_type_) ^ std::hash<int>()(agg_key.col_idx_);
  }
};

}  // namespace std
