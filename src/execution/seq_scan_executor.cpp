//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(nullptr), cur_table_it_(nullptr, RID(), nullptr) {}

void SeqScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  cur_table_it_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (true) {
    // end
    if (cur_table_it_ == table_info_->table_->End()) {
      return false;
    }

    // get tuple and predicate
    auto pred = plan_->GetPredicate();
    if (nullptr == pred || pred->Evaluate(&(*cur_table_it_), &table_info_->schema_).GetAs<bool>()) {
      break;
    }

    // next
    ++cur_table_it_;
  }

  const Schema *out_schema = GetOutputSchema();
  const auto &out_cols = out_schema->GetColumns();
  std::vector<Value> out_values;
  out_values.reserve(out_cols.size());
  for (const auto &out_col : out_cols) {
    // get src's value
    out_values.push_back(out_col.GetExpr()->Evaluate(&(*cur_table_it_), &table_info_->schema_));
  }

  // make the out tu
  (*tuple) = Tuple(out_values, out_schema);
  (*rid) = cur_table_it_->GetRid();
  // find it
  ++cur_table_it_;
  return true;
}

}  // namespace bustub
