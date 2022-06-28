//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      out_executor_(std::move(left_executor)),
      in_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() { out_executor_->Init(); }

// rid not used
bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  // return tuple in cache first
  if (!join_tuple_cache_.empty()) {
    *tuple = join_tuple_cache_.front();
    join_tuple_cache_.pop();
    return true;
  }

  // get new join tuple
  Tuple out_tuple;
  RID out_rid;
  // TODO(kamille): may be NULL?
  if (!out_executor_->Next(&out_tuple, &out_rid)) {
    return false;
  }

  // find joinable inner
  in_executor_->Init();
  bool inner_null = true;
  Tuple in_tuple;
  RID in_rid;
  while (in_executor_->Next(&in_tuple, &in_rid)) {
    if (inner_null) {
      inner_null = false;
    }

    if (plan_->Predicate()
            ->EvaluateJoin(&out_tuple, out_executor_->GetOutputSchema(), &in_tuple, in_executor_->GetOutputSchema())
            .GetAs<bool>()) {
      join_tuple_cache_.push(GetJoinTuple(out_tuple, in_tuple));
    }
  }
  if (inner_null) {
    return false;
  }

  // not found and recur
  if (join_tuple_cache_.empty()) {
    return Next(tuple, rid);
  }

  // found and process
  *tuple = join_tuple_cache_.front();
  join_tuple_cache_.pop();
  return true;
}

Tuple NestedLoopJoinExecutor::GetJoinTuple(const Tuple &out_tuple, const Tuple &in_tuple) {
  // loop out schema
  auto &join_cols = GetOutputSchema()->GetColumns();
  std::vector<Value> join_vals;
  join_vals.reserve(GetOutputSchema()->GetColumnCount());
  for (auto &join_col : join_cols) {
    auto col_expr = dynamic_cast<const ColumnValueExpression *>(join_col.GetExpr());
    BUSTUB_ASSERT(col_expr != nullptr, "col_expr cast failed");
    if (0 == col_expr->GetTupleIdx()) {
      join_vals.push_back(out_tuple.GetValue(out_executor_->GetOutputSchema(), col_expr->GetColIdx()));
    } else {
      join_vals.push_back(in_tuple.GetValue(in_executor_->GetOutputSchema(), col_expr->GetColIdx()));
    }
  }

  return Tuple(join_vals, GetOutputSchema());
}

}  // namespace bustub
