//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"

/**
* TODO: Student Implement
 */

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor))
{
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), const_cast<TableInfo *&>(table_info_));
}

void DeleteExecutor::Init()
{
  child_executor_->Init();
  exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(),table_indexes_);
}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid)
{
  if (is_end) {
    return false;
  }
  Row to_delete_tuple{};
  RowId emit_rid;

  while (child_executor_->Next(&to_delete_tuple, &emit_rid)) //child executor 为 value executor
  {
    bool deleted = table_info_->GetTableHeap()->MarkDelete(emit_rid, exec_ctx_->GetTransaction());
    Row keys{};
    if (deleted) {
      for(auto Index_in_Table:table_indexes_){
        to_delete_tuple.GetKeyFromRow(table_info_->GetSchema(), Index_in_Table->GetIndexKeySchema(),keys);
        Index_in_Table->GetIndex()->RemoveEntry(keys, *rid, exec_ctx_->GetTransaction());
      }
    }
  }
  is_end = true;
  return true;
}