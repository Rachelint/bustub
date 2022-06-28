//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/insert_executor.h"
#include <cassert>
#include <memory>

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(nullptr), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

void InsertExecutor::InsertOneTuple(Tuple *tuple) {
  RID rid;
  assert(table_info_->table_->InsertTuple(*tuple, &rid, exec_ctx_->GetTransaction()));

  // insert idx into indexs
  for (auto index_info : index_infos_) {
    index_info->index_->InsertEntry(
        tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), rid,
        exec_ctx_->GetTransaction());
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // whether check the schema?
  if (plan_->IsRawInsert()) {
    // if raw values and get child plan, it will panic
    // assert(plan_->GetChildPlan() == nullptr);
    for (auto &one_row : plan_->RawValues()) {
      // insert data into table
      Tuple tmp_tuple(one_row, &(table_info_->schema_));
      InsertOneTuple(&tmp_tuple);
    }
    // insert once, should return false to stop
    return false;
  }

  // get one tuple from child once
  Tuple child_tuple;
  RID child_rid;
  // not found or end
  if (!child_executor_->Next(&child_tuple, &child_rid)) {
    return false;
  }
  InsertOneTuple(&child_tuple);
  return true;
}

}  // namespace bustub
