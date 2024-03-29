//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"
#include "planner/expressions/logic_expression.h"

/**
 * TODO: Student Implement
 */
SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan),
      table_iterator(nullptr, RowId(0, 0)){}

void SeqScanExecutor::Init()
{
  std::string table_name_(plan_->GetTableName());   //获取表名
  exec_ctx_->GetCatalog()->GetTable(table_name_, table_info);   //获取表信息
  table_iterator = table_info->GetTableHeap()->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Row *row, RowId *rid)
{
  auto Predicate_ = plan_->GetPredicate();    //获取谓词
  while(table_iterator != table_info->GetTableHeap()->End())   //遍历表
  {
    //LOG(WARNING) << "SeqScanExecutor::Next"<<std::endl;
    if(Predicate_ == nullptr)   //如果谓词为空，那么直接返回
    {
      *row = *table_iterator;
      *rid = table_iterator->GetRowId();
      table_iterator++;
      return true;
    }
    if(Predicate_->GetType() == ExpressionType::LogicExpression)   //如果谓词为逻辑表达式，那么判断是否满足谓词
    {
      auto logic_expression = dynamic_cast<LogicExpression *>(Predicate_.get());
      bool is_ret = true;
      for(auto &k: logic_expression->GetChildren())
      {
        Field f = k->Evaluate(table_iterator.operator->());
        if(!f.CompareEquals(Field(kTypeInt, 1)))
        {
          is_ret = false;
          break;
        }
      }
      if(is_ret)
      {
        std::vector<Field> Fields;
        Row* tuple = new Row(*table_iterator);
        Schema *original_schema_ = table_info->GetSchema();
        for (auto column: original_schema_->GetColumns()) {
          for (auto target: plan_->OutputSchema()->GetColumns()) {
            if (!target->GetName().compare(column->GetName())) {
              Fields.push_back(*tuple->GetField(column->GetTableInd()));
            }
          }
        }
        *row = Row(Fields);
        *rid = table_iterator->GetRowId();
        table_iterator++;
        return true;
      }
    }
    else    //如果谓词为其他表达式，那么判断是否满足谓词
    {
      Field f = Predicate_->Evaluate(table_iterator.operator->());
      if(f.CompareEquals(Field(kTypeInt, 1)) == kTrue)   //如果谓词不为空，那么判断是否满足谓词
      {
        std::vector<Field> Fields;
        Row* tuple = new Row(*table_iterator);
        Schema *original_schema_ = table_info->GetSchema();
        for (auto column: original_schema_->GetColumns()) {
          for (auto target: plan_->OutputSchema()->GetColumns()) {
            if (!target->GetName().compare(column->GetName())) {
              Fields.push_back(*tuple->GetField(column->GetTableInd()));
            }
          }
        }
        *row = Row(Fields);
        *rid = table_iterator->GetRowId();
        table_iterator++;
        return true;
      }
    }
    table_iterator++;   //遍历下一行
  }
  //LOG(WARNING) << "SeqScanExecutor::Next"<<std::endl;
  return false;   //遍历结束
}