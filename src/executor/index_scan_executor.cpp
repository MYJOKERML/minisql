#include "executor/executors/index_scan_executor.h"
/**
* TODO: Student Implement
*/
IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init()
{
  auto Predicate_ = plan_->GetPredicate();    //获取谓词
  index_info = plan_->indexes_;
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
  auto Predicate_ = plan_->GetPredicate();    //获取谓词
  if(cursor_<index_info.size())   //遍历表
  {
    Field f = Predicate_->Evaluate(index_info[cursor_]->;
    if(f.CompareEquals(Field(kTypeInt, 1)))   //如果谓词不为空，那么判断是否满足谓词
    {
      *row = *table_iterator;
      *rid = table_iterator->GetRowId();
    }
    table_iterator++;   //遍历下一行
    return true;
  }
  return false;   //遍历结束
}
