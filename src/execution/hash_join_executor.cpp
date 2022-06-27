//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      out_executor_(std::move(left_child)),
      in_executor_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  // child executor init
  out_executor_->Init();
  in_executor_->Init();

  // build hashtable by in_executor
  Tuple tuple;
  RID rid;
  while (in_executor_->Next(&tuple, &rid)) {
    const Schema *in_schema = in_executor_->GetOutputSchema();
    JoinKey join_key{plan_->RightJoinKeyExpression()->Evaluate(&tuple, in_schema)};
    in_tuples_[join_key] = tuple;
  }
}

Tuple HashJoinExecutor::GetJoinTuple(const Tuple &out_tuple, const Tuple &in_tuple) {
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

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple out_tuple;
  RID out_rid;

  while (!out_executor_->Next(&out_tuple, &out_rid)) {
    return false;
  }

  // get join key
  const Schema *out_schema = out_executor_->GetOutputSchema();
  JoinKey join_key{plan_->LeftJoinKeyExpression()->Evaluate(&out_tuple, out_schema)};
  BUSTUB_ASSERT(0 != in_tuples_.count(join_key), "invalid join");
  *tuple = GetJoinTuple(out_tuple, in_tuples_[join_key]);

  return true;
}

}  // namespace bustub
