//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.End()) {}

void AggregationExecutor::Init() {
  // init child
  child_->Init();

  // loop child(seq) and get all tuples
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    // create aggre_key and aggre_value
    // insert them to hashtable
    // `groups` may be null, but nothing different, empty one can be hashed, too
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  // get next
  while (true) {
    if (aht_iterator_ == aht_.End()) {
      return false;
    }

    // only get the agg value suited to having
    const auto having = plan_->GetHaving();
    if (having == nullptr ||
        having->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_).GetAs<bool>()) {
      break;
    }

    ++aht_iterator_;
  }

  // format and return
  auto &o_cols = GetOutputSchema()->GetColumns();
  std::vector<Value> o_vals;
  o_vals.reserve(GetOutputSchema()->GetColumnCount());
  for (auto &o_col : o_cols) {
    o_vals.push_back(
        o_col.GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_));
  }
  *tuple = Tuple(o_vals, GetOutputSchema());

  ++aht_iterator_;
  return true;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
