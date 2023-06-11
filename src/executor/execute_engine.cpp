#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "parser/syntax_tree_printer.h"
#include "planner/planner.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"

extern "C" {
int yyparse(void);
FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** After you finish the code for the CatalogManager section,
   *  you can uncomment the commented code.
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   **/
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if(!current_db_.empty())
    context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}
/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context)
{
  #ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
  #endif
    if(ast->child_ == nullptr)
      return DB_FAILED;
    std::string db_name(ast->child_->val_);   // 数据库名字
    if(dbs_[db_name] != nullptr)    // 数据库已经存在
    {
      ExecuteInformation(DB_ALREADY_EXIST);
      return DB_ALREADY_EXIST;
    }
    auto new_database = new DBStorageEngine(db_name);  // 创建数据库
    dbs_[db_name] = new_database;  // 将数据库加入到数据库map中

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context)
{
  #ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDropDatabase" << std::endl;
  #endif
    if(ast->child_ == nullptr)    // 没有孩子节点
        return DB_FAILED;
    std::string del_db_name(ast->child_->val_);   // 待删除的数据库名字
    if(dbs_[del_db_name] == nullptr)    // 数据库不存在
    {
        ExecuteInformation(DB_NOT_EXIST);   // 打印信息
        return DB_NOT_EXIST;
    }
    if(current_db_ == del_db_name)    // 待删除的数据库是当前数据库
        current_db_ = "";
    DBStorageEngine* target_db = dbs_[del_db_name];   // 找到待删除的数据库
    target_db->~DBStorageEngine();    // 删除数据库
    dbs_.erase(del_db_name);    // 从数据库map中删除
    return DB_SUCCESS;    // 返回成功
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context)
{
  #ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteShowDatabases" << std::endl;
  #endif

    for(std::pair<std::string, DBStorageEngine*> kv : dbs_)     // 遍历数据库map
    {
          std::cout << kv.first << std::endl;
    }

    std::cout << "There is(are) " << dbs_.size() << " database(s) in total." << endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context)
{
  #ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteUseDatabase" << std::endl;
  #endif
    if(ast->child_ == nullptr)
          return DB_FAILED;
    std::string db_name(ast->child_->val_);   // 待使用的数据库名字
    if(dbs_[db_name] == nullptr)
    {
        ExecuteInformation(DB_NOT_EXIST);   // 打印信息
        return DB_NOT_EXIST;
    }
    current_db_ = db_name;    // 设置当前数据库
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context)
{
  #ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteShowTables" << std::endl;
  #endif
    DBStorageEngine* current_db_engine = dbs_[current_db_];   // 当前数据库
    if(current_db_engine == nullptr)
    {
        ExecuteInformation(DB_NOT_EXIST);   // 打印信息
        return DB_NOT_EXIST;
    }

    std::vector<TableInfo*> vec_table_info;   // 存储表信息
    current_db_engine->catalog_mgr_->GetTables(vec_table_info);  // 获取表信息
    for(auto & i : vec_table_info)
    {
          std::cout << i->GetTableName() << std::endl;
    }
    std::cout << "table(s) of number: " << vec_table_info.size() << endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context)
{
  #ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteCreateTable" << std::endl;
  #endif

    DBStorageEngine* current_db_engine = dbs_[current_db_];  // 当前数据库
    if(current_db_engine == nullptr)
    {
        ExecuteInformation(DB_NOT_EXIST);   // 打印信息
        return DB_NOT_EXIST;
    }
    string new_table_name(ast->child_->val_);   // 新表名字
    TableInfo* new_table_info;
    dberr_t find_table = current_db_engine->catalog_mgr_->GetTable(new_table_name, new_table_info);
    if(find_table == DB_SUCCESS)
    {
        ExecuteInformation(DB_TABLE_ALREADY_EXIST);   // 打印信息
        return DB_TABLE_ALREADY_EXIST;
    }

    TableInfo* tmp_table_info;
    pSyntaxNode kNCD_List = ast->child_->next_;   // create table中的信息
    pSyntaxNode kNCD_node = kNCD_List->child_;  // 得到第一个column
    vector<Column*> tmp_column_vec;             // 存储column的vector
    vector<string> column_names;                // 存储列名字的vector
    unordered_map<string, bool> if_unique;      // 存储列是否unique的map
    unordered_map<string, bool> if_primary_key; // 存储列是否primary key的map
    unordered_map<string, string> type_of_column;  // 存储列的类型的map
    unordered_map<string, int> char_size;       // 存储每个类型为char的属性对应的char的大小的map
    vector<string> uni_keys;                  // 存储unique的列的名字的vector
    vector<string> pri_keys;                  // 存储primary key的列的名字的vector

    while (kNCD_node != nullptr && kNCD_node->type_ == kNodeColumnDefinition)   // 遍历每个column
    {
        string kNCD_ifunique;
        if (kNCD_node->val_ == nullptr)
          kNCD_ifunique = "";
        else
          kNCD_ifunique = kNCD_node->val_;    // 得到unique的信息

        string kNCD_columname(kNCD_node->child_->val_);   // 得到列名字
        string kNCD_typename(kNCD_node->child_->next_->val_);   // 得到列的类型

        column_names.push_back(kNCD_columname);   // 将列名字加入vector
        type_of_column[kNCD_columname] = kNCD_typename;   // 将列的类型加入map
        if_unique[kNCD_columname] = false;    // 初始化为false
        if_primary_key[kNCD_columname] = false;   // 初始化为false
        if (kNCD_ifunique == "unique")    // 如果是unique,则将if_unique设置为true
        {
          if_unique[kNCD_columname] = true;
          uni_keys.push_back(kNCD_columname);
        }
        else
          if_unique[kNCD_columname] = false;
        if (kNCD_typename == "char")    // 如果是char,则得到char的大小
        {
            pSyntaxNode kNCD_char_sizenode = kNCD_node->child_->next_->child_;
            string str_char_size(kNCD_char_sizenode->val_);
            char_size[kNCD_columname] = stoi(str_char_size);    // 将char的大小加入map
            if (char_size[kNCD_columname] <= 0)
            {
              std::cout << "char size <= 0 !" << endl;
              return DB_FAILED;
            }
        }
        kNCD_node = kNCD_node->next_;   // 下一个column
    }

    if (kNCD_node != nullptr)   // 如果还有下一个节点,则说明有primary key
    {
        pSyntaxNode primary_keys_node = kNCD_node->child_;    // 得到primary key的节点
        while (primary_keys_node)   // 遍历primary key的节点
        {
          string primary_key_name(primary_keys_node->val_);   // 得到primary key的列的名字
          if_primary_key[primary_key_name] = true;    // 将if_primary_key设置为true
          pri_keys.push_back(primary_key_name);   // 将primary key的列的名字加入vector
 /*<------------------------------此处待修改------------------------------------------------>*/
          uni_keys.push_back(primary_key_name);
          primary_keys_node = primary_keys_node->next_;   // 下一个primary key的节点
        }
    }

    int column_index_counter = 0;
    for (string column_name_stp : column_names)   // 遍历每个列的名字
    {
        Column *new_column;
        if (type_of_column[column_name_stp] == "int")
        {
/*<------------------------------此处待修改------------------------------------------------>*/
          if (if_unique[column_name_stp] || if_primary_key[column_name_stp])
          {
            new_column = new Column(column_name_stp, TypeId::kTypeInt, column_index_counter,
                                    false, true);
          }
          else
          {
            new_column = new Column(column_name_stp, TypeId::kTypeInt, column_index_counter,
                                    false, false);
          }
        }
        else if (type_of_column[column_name_stp] == "float")
        {
/*<------------------------------此处待修改------------------------------------------------>*/
          if (if_unique[column_name_stp] || if_primary_key[column_name_stp])
          {
            new_column = new Column(column_name_stp, TypeId::kTypeFloat, column_index_counter,
                                    false, true);
          }
          else
          {
            new_column = new Column(column_name_stp, TypeId::kTypeFloat, column_index_counter,
                                    false, false);
          }
        }
        else if (type_of_column[column_name_stp] == "char")
        {
/*<------------------------------此处待修改------------------------------------------------>*/
          if (if_unique[column_name_stp] || if_primary_key[column_name_stp])
          {
            new_column = new Column(column_name_stp, TypeId::kTypeChar, char_size[column_name_stp], column_index_counter,
                                    false, true);
          }
          else
          {
            new_column = new Column(column_name_stp, TypeId::kTypeChar, char_size[column_name_stp], column_index_counter,
                                    false, false);
          }
        }
        else
        {
          std::cout << "unknown typename" << column_name_stp << endl;
          return DB_FAILED;
        }
        column_index_counter++;
        tmp_column_vec.push_back(new_column);
    }

    auto new_schema = new Schema(tmp_column_vec);   // 创建schema，用于创建table
    dberr_t if_create_success;
    if_create_success = current_db_engine->catalog_mgr_->CreateTable(new_table_name, new_schema, nullptr, tmp_table_info);    // 创建table
    if (if_create_success != DB_SUCCESS)
        return if_create_success;
    CatalogManager *current_CMgr = dbs_[current_db_]->catalog_mgr_;
    for (string column_name_stp : column_names)
    {
        if (if_primary_key[column_name_stp])
        {
          tmp_table_info->GetTableMeta()->primary_key_name = pri_keys;
          tmp_table_info->GetTableMeta()->unique_key_name = uni_keys;
          string stp_index_name = column_name_stp + "_index";
          vector<string> index_columns_stp = {column_name_stp};
          IndexInfo *stp_index_info;
/*<------------------------------此处待修改------------------------------------------------>*/
          dberr_t if_create_index_success = current_CMgr->CreateIndex(new_table_name, stp_index_name, index_columns_stp,
                                                                      nullptr, stp_index_info, "");
          if (if_create_index_success != DB_SUCCESS)
            return if_create_index_success;
        }
    }
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context)
{
  #ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDropTable" << std::endl;
  #endif
    if(ast->child_ == nullptr)
    {
        ExecuteInformation(DB_NOT_EXIST);
        return DB_NOT_EXIST;
    }
    string drop_table_name(ast->child_->val_);

    return dbs_[current_db_]->catalog_mgr_->DropTable(drop_table_name);
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context)
{
  #ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteShowIndexes" << std::endl;
  #endif
    vector<TableInfo*> vec_tableinfo;
    dberr_t if_gettable_success = dbs_[current_db_]->catalog_mgr_->GetTables(vec_tableinfo);
    if(if_gettable_success != DB_SUCCESS)
          return if_gettable_success;
    CatalogManager* current_CMgr = dbs_[current_db_]->catalog_mgr_;
    for(TableInfo* tmp_tableinfo: vec_tableinfo)
    {
          vector<IndexInfo*> vec_tmp_indexinfo;
          string table_name = tmp_tableinfo->GetTableName();
          dberr_t if_getindex_success = current_CMgr->GetTableIndexes(table_name, vec_tmp_indexinfo);
          if(if_getindex_success != DB_SUCCESS)
            return if_getindex_success;
          for(IndexInfo* tmp_indexinfo: vec_tmp_indexinfo)
          {
            string index_name = tmp_indexinfo->GetIndexName();
            std::cout << index_name << endl;
          }
    }

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context)
{
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
    if(ast == nullptr || current_db_.empty())
            return DB_FAILED;

    CatalogManager* current_CMgr = dbs_[current_db_]->catalog_mgr_;   // 获取当前数据库的catalog manager
    string table_name(ast->child_->next_->val_);                  // 获取表名
    string index_name(ast->child_->val_);                    // 获取索引名
    TableInfo* target_table;                            // 用于存储表的信息
    dberr_t if_gettable_success = current_CMgr->GetTable(table_name, target_table);   // 获取表的信息
    if(if_gettable_success != DB_SUCCESS)
            return if_gettable_success;
    vector<string> vec_index_colum_lists;   // 用于存储索引的列名
    pSyntaxNode pSnode_colum_list = ast->child_->next_->next_->child_;
    while(pSnode_colum_list)
    {
        vec_index_colum_lists.push_back(string(pSnode_colum_list->val_));   // 获取索引的列名
        pSnode_colum_list = pSnode_colum_list->next_;
    }
    Schema* target_schema = target_table->GetSchema();
    for(string tmp_colum_name: vec_index_colum_lists)
    {
        int if_could = 0;
        vector<string> uni_colum_list = target_table->GetTableMeta()->unique_key_name;
        for(string name: uni_colum_list)
        {
            if(name == tmp_colum_name)    // 如果索引的列名在unique列中，那么可以创建索引
            {
                if_could = 1;
                break;
            }
        }
        if(!if_could)
        {
            std::cout << "can not build index on column(s) not unique" << endl;
            return DB_FAILED;
        }
        uint32_t tmp_index;
        dberr_t if_getcolum_success = target_schema->GetColumnIndex(tmp_colum_name, tmp_index);
        if(if_getcolum_success != DB_SUCCESS)
          return if_getcolum_success;
    }
    IndexInfo* new_indexinfo;
    dberr_t if_createindex_success = current_CMgr->CreateIndex(table_name, index_name, vec_index_colum_lists, nullptr, new_indexinfo, "");
    if(if_createindex_success != DB_SUCCESS)
            return if_createindex_success;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context)
{
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
    if(current_db_.empty())
      return DB_FAILED;

    CatalogManager* current_CMgr = dbs_[current_db_]->catalog_mgr_;
    string index_name(ast->child_->val_);
    vector<TableInfo*> table_names;
    current_CMgr->GetTables(table_names);
    string table_name;
    for(TableInfo* tmp_info: table_names)   // 遍历所有表，找到索引所在的表
    {
        vector<IndexInfo*> index_infos;
        current_CMgr->GetTableIndexes(tmp_info->GetTableName(), index_infos);
        for(IndexInfo* stp_idx_info: index_infos)
        {
            if(stp_idx_info->GetIndexName() == index_name)
            {
                  table_name = tmp_info->GetTableName();
                  goto out;
            }
        }
    }
  out:;
    IndexInfo* tmp_indexinfo;
    dberr_t if_getindex_success = current_CMgr->GetIndex(table_name, index_name, tmp_indexinfo);
    if(if_getindex_success != DB_SUCCESS)
    {
        std::cout << "no index: " << index_name << endl;
        return if_getindex_success;
    }
    dberr_t if_create_success = current_CMgr->DropIndex(table_name, index_name);
    if(if_create_success != DB_SUCCESS)
    {
        std::cout << "fail to drop index: " << index_name << endl;
        return if_create_success;
    }

    return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context)
{
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif

    string filename(ast->child_->val_);       // 获取文件名
    fstream exefstream(filename);             // 打开文件
    if(!exefstream.is_open())                 // 打开失败
    {
        std::cout << "fail to open '" << filename << "'" << endl;
        return DB_FAILED;
    }
    int buffer_size = 1024;
    char* cmd = new char[buffer_size];

    while (true)
    {
        char tmp_char;
        int tmp_counter = 0;
        do
        {
            if(exefstream.eof())    // 文件结束
            {
                  delete []cmd;
                  return DB_SUCCESS;
            }
            exefstream.get(tmp_char);   // 读取一个字符
            cmd[tmp_counter++] = tmp_char;    // 存入buffer
            if(tmp_counter >= buffer_size)    // buffer溢出
            {
                  std::cout << "buffer overflow" << endl;
                  return DB_FAILED;
            }
        }while(tmp_char != ';');
        cmd[tmp_counter] = 0;   // 末尾加上'\0'

        YY_BUFFER_STATE bp = yy_scan_string(cmd);
        if (bp == nullptr)
        {
            LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
            exit(1);
        }
        yy_switch_to_buffer(bp);

        // init parser module
        MinisqlParserInit();

        // parse
        yyparse();

        // parse result handle
        if (MinisqlParserGetError())
        {
            // error
            printf("%s\n", MinisqlParserGetErrorMessage());
        }

        this->Execute(MinisqlGetParserRootNode());

        // clean memory after parse
        MinisqlParserFinish();
        yy_delete_buffer(bp);
        yylex_destroy();
  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context)
{
  #ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteQuit" << std::endl;
  #endif
    ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
    cout << "Bye" << endl;
    return DB_SUCCESS;
}

