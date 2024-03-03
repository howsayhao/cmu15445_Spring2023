#include "execution/executors/projection_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

ProjectionExecutor::ProjectionExecutor(ExecutorContext *exec_ctx, const ProjectionPlanNode *plan,
                                       std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void ProjectionExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();
}

auto ProjectionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};

  // Get the next tuple
  const auto status = child_executor_->Next(&child_tuple, rid);

  if (!status) {
    return false;
  }

  // Compute expressions
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());  // plan.output_schema_.columns_.size()
  for (const auto &expr : plan_->GetExpressions()) {   // plan.expressions_
    values.push_back(expr->Evaluate(
        &child_tuple, child_executor_->GetOutputSchema()));  // child_executor的输出模式 -> projection的输入模式
  }

  *tuple = Tuple{values, &GetOutputSchema()};  // emit

  return true;
}
}  // namespace bustub
