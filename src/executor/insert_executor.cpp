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

  while (child_executor_->Next(&to_insert_tuple, &emit_rid)) //child executor 为 value executor
  {
    Row keys{};
    vector<RowId> result;
    bool inserted = true;
    for(auto Index_in_Table:table_indexes_)
    {
        to_insert_tuple.GetKeyFromRow(table_info_->GetSchema(), Index_in_Table->GetIndexKeySchema(), keys);
        Index_in_Table->GetIndex()->ScanKey(keys, result, exec_ctx_->GetTransaction());
        if(!result.empty())
        {
          inserted = false;
          LOG(WARNING) << "Insert failed, duplicate key";
          break;
        }
    }
    if(!inserted)
        break;
    inserted = table_info_->GetTableHeap()->InsertTuple(to_insert_tuple, exec_ctx_->GetTransaction());
    if (inserted) {
      for(auto Index_in_Table:table_indexes_){
        to_insert_tuple.GetKeyFromRow(table_info_->GetSchema(), Index_in_Table->GetIndexKeySchema(),keys);
        Index_in_Table->GetIndex()->InsertEntry(keys, to_insert_tuple.GetRowId() ,exec_ctx_->GetTransaction());
      }
    }
  }
  *row = Row{};
  is_end = true;
  return true;
}