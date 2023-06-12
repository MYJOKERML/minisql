//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor))
{
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), const_cast<TableInfo *&>(table_info_));
}

void InsertExecutor::Init()
{
  child_executor_->Init();
  exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(),table_indexes_);
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  if (is_end) {
    return false;
  }
  Row to_insert_tuple{};
  RowId emit_rid;
  int32_t insert_count = 0;

  while (child_executor_->Next(&to_insert_tuple, &emit_rid)) //child executor 为 value executor
  {
    bool inserted = table_info_->GetTableHeap()->InsertTuple(to_insert_tuple, exec_ctx_->GetTransaction());
    Row keys{};
    if (inserted) {
      for(auto Index_in_Table:table_indexes_){
        to_insert_tuple.GetKeyFromRow(table_info_->GetSchema(), Index_in_Table->GetIndexKeySchema(),keys);
        Index_in_Table->GetIndex()->InsertEntry(keys,*rid, exec_ctx_->GetTransaction());
      }
      insert_count++;
    }
  }
  std::vector<Field> Fields{};
  Fields.push_back(Field(kTypeInt,insert_count));
  *row = Row{Fields};
  is_end = true;
  return true;
}