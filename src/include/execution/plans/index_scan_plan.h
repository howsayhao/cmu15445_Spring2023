//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_plan.h
//
// Identification: src/include/execution/plans/index_scan_plan.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"
#include "type/value.h"

namespace bustub {
/**
 * IndexScanPlanNode identifies a table that should be scanned with an optional predicate.
 */
class IndexScanPlanNode : public AbstractPlanNode {
 public:
  /**
   * Creates a new index scan plan node.
   * @param output The output format of this scan plan node
   * @param table_oid The identifier of table to be scanned
   */
  IndexScanPlanNode(SchemaRef output, index_oid_t index_oid)
      : AbstractPlanNode(std::move(output), {}), index_oid_(index_oid) {}
  IndexScanPlanNode(SchemaRef output, index_oid_t index_oid, AbstractExpressionRef predicate, std::vector<Value> keys)
      : AbstractPlanNode(std::move(output), {}),
        index_oid_(index_oid),
        predicate_(std::move(predicate)),
        range_start_(std::move(keys)) {}

  auto GetType() const -> PlanType override { return PlanType::IndexScan; }

  /** @return the identifier of the table that should be scanned */
  auto GetIndexOid() const -> index_oid_t { return index_oid_; }

  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(IndexScanPlanNode);

  /** The table whose tuples should be scanned. */
  index_oid_t index_oid_;

  // Add anything you want here for index lookup
  AbstractExpressionRef predicate_{};
  std::vector<Value> range_start_{};

 protected:
  auto PlanNodeToString() const -> std::string override {
    if (predicate_) {
      return fmt::format("IndexScan {{ index_oid={}, filter={} }}", index_oid_, predicate_);
    }
    return fmt::format("IndexScan {{ index_oid={} }}", index_oid_);
  }
};

}  // namespace bustub
