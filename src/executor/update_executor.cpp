//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor))
{
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), const_cast<TableInfo *&>(table_info_));
}

/**
* TODO: Student Implement
 */
void UpdateExecutor::Init()
{
  child_executor_->Init();
  exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(),table_indexes_);
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid)
{
  if (is_end) {
    return false;
  }
  Row old_row {};
  RowId emit_rid;


  while (child_executor_->Next(&old_row, &emit_rid)) //child executor 为 value executor
  {
    Row update_row = GenerateUpdatedTuple(old_row);
    bool inserted = table_info_->GetTableHeap()->UpdateTuple(update_row, emit_rid,exec_ctx_->GetTransaction());
    Row keys{};
    if (inserted) {
      for(auto Index_in_Table:table_indexes_){
        old_row.GetKeyFromRow(table_info_->GetSchema(), Index_in_Table->GetIndexKeySchema(),keys);
        Index_in_Table->GetIndex()->RemoveEntry(keys,*rid, exec_ctx_->GetTransaction());
      }
      for(auto Index_in_Table:table_indexes_){
        update_row.GetKeyFromRow(table_info_->GetSchema(), Index_in_Table->GetIndexKeySchema(),keys);
        Index_in_Table->GetIndex()->InsertEntry(keys,*rid, exec_ctx_->GetTransaction());
      }
    }
  }
  is_end = true;
  return true;
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row)
{
  std::vector<Field> values{};
  values.reserve(child_executor_->GetOutputSchema()->GetColumnCount());
  for (uint32_t i=0; i < child_executor_->GetOutputSchema()->GetColumnCount(); i++)
  {
    auto predict_node_map = plan_->GetUpdateAttr();
    auto predict_node = predict_node_map[i];
    values.push_back(predict_node->Evaluate(&src_row));
  }

  auto to_update_tuple = Row{values};
  return to_update_tuple;
}