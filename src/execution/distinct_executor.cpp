//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctKey TupleToKey(const Tuple *tuple, const Schema *schema) {
  std::vector<Value> vals;
  const auto &cols = schema->GetColumns();
  vals.reserve(cols.size());
  for (uint32_t i = 0; i < cols.size(); i++) {
    vals.push_back(tuple->GetValue(schema, i));
  }

  return DistinctKey{std::move(vals)};
}

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() { child_executor_->Init(); }

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  while (true) {
    // child end
    if (!child_executor_->Next(tuple, rid)) {
      return false;
    }

    // check distinct
    //   + true, continue
    //   + false, insert and return
    DistinctKey dis_key = TupleToKey(tuple, child_executor_->GetOutputSchema());
    if (0 == dis_set_.count(dis_key)) {
      dis_set_.insert(dis_key);
      break;
    }
  }

  return true;
}

}  // namespace bustub
