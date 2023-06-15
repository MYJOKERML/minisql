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
    Row keys2{};
    LOG(WARNING) << inserted << endl;
    if (inserted) {
      for(auto &Index_in_Table:table_indexes_){
        old_row.GetKeyFromRow(table_info_->GetSchema(), Index_in_Table->GetIndexKeySchema(),keys);
        Index_in_Table->GetIndex()->RemoveEntry(keys, emit_rid, exec_ctx_->GetTransaction());
        update_row.GetKeyFromRow(table_info_->GetSchema(), Index_in_Table->GetIndexKeySchema(),keys2);
        Index_in_Table->GetIndex()->InsertEntry(keys2, emit_rid, exec_ctx_->GetTransaction());
      }
    }
  }
  is_end = true;
  return true;
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema *schema = table_info_->GetSchema();
  uint32_t col_count = schema->GetColumnCount();
  std::vector<Field> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if(update_attrs.find(idx) != update_attrs.end()) {
      values.push_back(update_attrs.at(idx)->Evaluate(&src_row));
    }else {
      values.push_back(*src_row.GetField(idx));
    }
  }
  return Row{values};
}