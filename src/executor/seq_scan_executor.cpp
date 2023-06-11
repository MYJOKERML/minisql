//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"

/**
* TODO: Student Implement
*/
SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan),
      table_iterator(nullptr, RowId(0, 0)){}

void SeqScanExecutor::Init()
{
    auto Predicate_ = plan_->GetPredicate();    //获取谓词
    std::string table_name_(plan_->GetTableName());   //获取表名
    exec_ctx_->GetCatalog()->GetTable(table_name_, table_info);   //获取表信息
    table_iterator = table_info->GetTableHeap()->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Row *row, RowId *rid)
{
    auto Predicate_ = plan_->GetPredicate();    //获取谓词
    if(table_iterator != table_info->GetTableHeap()->End())   //遍历表
    {
      Field f = Predicate_->Evaluate(table_iterator.operator->());
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
