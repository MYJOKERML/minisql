#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
    ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
    MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
    buf += 4;
    MACH_WRITE_UINT32(buf, table_meta_pages_.size());
    buf += 4;
    MACH_WRITE_UINT32(buf, index_meta_pages_.size());
    buf += 4;
    for (auto iter : table_meta_pages_) {
        MACH_WRITE_TO(table_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
    for (auto iter : index_meta_pages_) {
        MACH_WRITE_TO(index_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
    // check valid
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
    // get table and index nums
    uint32_t table_nums = MACH_READ_UINT32(buf);
    buf += 4;
    uint32_t index_nums = MACH_READ_UINT32(buf);
    buf += 4;
    // create metadata and read value
    CatalogMeta *meta = new CatalogMeta();
    for (uint32_t i = 0; i < table_nums; i++) {
        auto table_id = MACH_READ_FROM(table_id_t, buf);
        buf += 4;
        auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
    }
    for (uint32_t i = 0; i < index_nums; i++) {
        auto index_id = MACH_READ_FROM(index_id_t, buf);
        buf += 4;
        auto index_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->index_meta_pages_.emplace(index_id, index_page_id);
    }
    return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const 
{
    uint32_t size = 12;
    size += table_meta_pages_.size() * 8;
    size += index_meta_pages_.size() * 8;
    return size;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) 
{
    if(init)
    {
        catalog_meta_ = CatalogMeta::NewInstance();
        next_table_id_ = catalog_meta_->GetNextTableId();
        next_index_id_ = catalog_meta_->GetNextIndexId();
    }
    else
    {
        Page *meta_data_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
        catalog_meta_ = CatalogMeta::DeserializeFrom(meta_data_page->GetData());
        next_table_id_ = catalog_meta_->GetNextTableId();
        next_index_id_ = catalog_meta_->GetNextIndexId();

        for (auto table_meta_page_it : catalog_meta_->table_meta_pages_) 
        {
            LoadTable(table_meta_page_it.first, table_meta_page_it.second);
        }
        for (auto index_meta_page_it : catalog_meta_->index_meta_pages_) 
        {
            LoadIndex(index_meta_page_it.first, index_meta_page_it.second);
        }

        buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
    }
    FlushCatalogMetaPage();
}

CatalogManager::~CatalogManager() {
    FlushCatalogMetaPage();
}

/**
* TODO: Student Implement
*/
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) 
{
    if (table_names_.count(table_name)) //检查table是否存在
        return DB_TABLE_ALREADY_EXIST;
    table_info = TableInfo::Create();       // 新建一个TableInfo
    table_id_t table_id = next_table_id_++; // 分配一个table_id
    TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema, nullptr, log_manager_, lock_manager_);  // 新建一个table_heap
    TableMetadata *meta_data = TableMetadata::Create(table_id, table_name, table_heap->GetFirstPageId(), schema);   // 新建一个table_meta_data
    table_info->Init(meta_data, table_heap);    // 初始化table_info
    table_names_[table_name] = table_id;        //将catalog manager中的存放table_id和table_info的map初始化
    tables_[table_id] = table_info;
    page_id_t meta_data_page_id;
    Page *meta_data_page = buffer_pool_manager_->NewPage(meta_data_page_id);
    meta_data->SerializeTo(meta_data_page->GetData());      //将table的元信息序列化到一个新页中
    catalog_meta_->table_meta_pages_[table_id] = meta_data_page_id;
    buffer_pool_manager_->UnpinPage(meta_data_page_id,true);
    FlushCatalogMetaPage();
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info)
{
    auto table_id_it = table_names_.find(table_name);
    if (table_id_it == table_names_.end()) return DB_TABLE_NOT_EXIST;   //如果没找到

    return GetTable(table_id_it->second, table_info);       // 使用table_id继续查找
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const
{
    for (auto table : tables_) 
    {
        tables.push_back(table.second);     //将所有的table_info放到tables里面并返回
    }
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type)
{
    if (!table_names_.count(table_name))
        return DB_TABLE_NOT_EXIST;
    if (index_names_[table_name].count(index_name)) 
        return DB_INDEX_ALREADY_EXIST;
    //如果table存在但是index不存在，进行创建
    index_info = IndexInfo::Create();
    index_id_t index_id = next_index_id_++;     //分配一个index_id
    table_id_t table_id = table_names_.find(table_name)->second;
    TableInfo *table_info = tables_[table_id];

    std::vector<uint32_t> key_map;              //储存index中key对应的下标
    for (const auto &index_key_name : index_keys) 
    {
        uint32_t key_index;
        if (table_info->GetSchema()->GetColumnIndex(index_key_name, key_index) == DB_COLUMN_NAME_NOT_EXIST) //假如index中有key不存在
            return DB_COLUMN_NAME_NOT_EXIST;
        key_map.push_back(key_index);
    }
    IndexMetadata *meta_data = IndexMetadata::Create(index_id, index_name, table_id, key_map);  //创建Index对应的MetaData
    index_info->Init(meta_data, table_info, buffer_pool_manager_);  //初始化index信息

    if (!index_names_.count(table_name))
    {
        std::unordered_map<std::string, index_id_t> map;    //如果没有对应的table
        map[index_name] = index_id;                         //建立index_name和index_id对应的关系
        index_names_[table_name] = map;                     //建立table和index的对应关系
    } 
    else 
    {
        index_names_.find(table_name)->second[index_name] = index_id;   //如果存在直接更新table中对应的index
    }

    indexes_[index_id] = index_info;

    page_id_t meta_data_page_id;        //新建一个数据页来储存index的元信息
    Page *meta_data_page = buffer_pool_manager_->NewPage(meta_data_page_id);
    meta_data->SerializeTo(meta_data_page->GetData());
    catalog_meta_->index_meta_pages_[index_id] = meta_data_page_id;
    buffer_pool_manager_->UnpinPage(meta_data_page_id, true);
    FlushCatalogMetaPage();
    return DB_SUCCESS;
}                                    


/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const 
{
    if(!table_names_.count(table_name))      //查找是否存在这个table
        return DB_TABLE_NOT_EXIST;
    auto table_index_it = index_names_.find(table_name);    //查找这个table是否有和index的对应关系
    if (table_index_it == index_names_.end())
        return DB_INDEX_NOT_FOUND;
    else 
    {
        auto index_id_it = table_index_it->second.find(index_name);
        if (index_id_it == table_index_it->second.end()) 
            return DB_INDEX_NOT_FOUND;
        auto index_info_it = indexes_.find(index_id_it->second);    //从indexes_中拿到对应的信息并赋值
        if (index_info_it == indexes_.end()) 
            return DB_FAILED;
        index_info = index_info_it->second;
    }

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const 
{
    if(!table_names_.count(table_name))      //查找是否存在这个table
        return DB_TABLE_NOT_EXIST;

    if (index_names_.count(table_name))     //该table有和index的对应关系
    {
        for (const auto &index_map : index_names_.find(table_name)->second) 
        {
            auto indexes_it = indexes_.find(index_map.second);
            if (indexes_it == indexes_.end()) 
                return DB_FAILED;
            indexes.push_back(indexes_it->second);
        }
    }

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) 
{
    auto table_id_it = table_names_.find(table_name);
    if (table_id_it == table_names_.end())  //如果table不存在
        return DB_TABLE_NOT_EXIST;
    table_id_t table_id = table_id_it->second;      //找到对应的table_id

    if (!buffer_pool_manager_->DeletePage(tables_[table_id]->GetRootPageId())) 
        return DB_FAILED;
    if (!buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_[table_id])) 
        return DB_FAILED;

    tables_.erase(tables_.find(table_id));      //删除各map中储存的data信息
    table_names_.erase(table_names_.find(table_name));
    catalog_meta_->table_meta_pages_.erase(catalog_meta_->table_meta_pages_.find(table_id));

    if (index_names_.count(table_name))         // 删除各个map中该table的index
    {
        for (const auto &index_pair : index_names_[table_name]) 
        {
            catalog_meta_->index_meta_pages_.erase(index_pair.second);
            indexes_.erase(index_pair.second);
        }
        index_names_.erase(table_name);
    }
    FlushCatalogMetaPage();
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name)
{
    if (!table_names_.count(table_name))
        return DB_TABLE_NOT_EXIST;
    auto table_index_it = index_names_.find(table_name);
    if (table_index_it == index_names_.end())   //如果该table没有和index的对应关系
    {
        return DB_INDEX_NOT_FOUND;
    } 
    else
    {
        auto index_name_it = table_index_it->second.find(index_name);
        if (index_name_it == table_index_it->second.end())
            return DB_INDEX_NOT_FOUND;
        else 
        {
            index_id_t index_id = index_name_it->second;
            IndexInfo *index_info = indexes_[index_id];
            index_info->GetIndex()->Destroy();                      // 删除索引
            if (!buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_[index_id])) 
                return DB_FAILED;                // 删除存放metadata的页
            if (table_index_it->second.size() == 1) 
            {
                index_names_.erase(table_index_it);                     // 如果这个table只有这个Index
            } 
            else 
            {
                table_index_it->second.erase(index_name);
            }
            indexes_.erase(index_id);
            catalog_meta_->index_meta_pages_.erase(catalog_meta_->index_meta_pages_.find(index_id));
        }
    }
    FlushCatalogMetaPage();
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const 
{
    // 直接序列化CatalogMetaData到数据页中
    auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_->SerializeTo(catalog_meta_page->GetData());

    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
    buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) 
{
    auto meta_data_page = buffer_pool_manager_->FetchPage(page_id); //拿到table对应的元信息的页
    TableInfo *table_info = TableInfo::Create();
    TableMetadata *meta_data;
    TableMetadata::DeserializeFrom(meta_data_page->GetData(), meta_data);   //反序列化到meta_data中
    table_names_[meta_data->GetTableName()] = table_id;     //将table_name和table_id对应起来

    TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, meta_data->GetFirstPageId(), meta_data->GetSchema(),
                                                log_manager_, lock_manager_);
    table_info->Init(meta_data, table_heap);
    tables_[table_id] = table_info;
    buffer_pool_manager_->UnpinPage(page_id, false);
    return DB_SUCCESS;
}
 
 
/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) 
{
    auto meta_data_page = buffer_pool_manager_->FetchPage(page_id);
    IndexInfo *index_info = IndexInfo::Create();
    IndexMetadata *meta_data;
    IndexMetadata::DeserializeFrom(meta_data_page->GetData(), meta_data);   //反序列化到meta_data中

    table_id_t table_id = meta_data->GetTableId();
    std::string index_name = meta_data->GetIndexName();
    std::string table_name = tables_[table_id]->GetTableName();

    if (!table_names_.count(table_name))    //如果没有对应的table
    {
        buffer_pool_manager_->UnpinPage(page_id, false);
        return DB_TABLE_NOT_EXIST;
    } 
    else    //如果有对应的table
    {
        if (!index_names_.count(table_name))    //如果index里面没有对应的table，那么就创建一个
        {
            std::unordered_map<std::string, index_id_t> map;
            map[index_name] = index_id;
            index_names_[table_name] = map;
        }
        else 
        {
            index_names_.find(table_name)->second[index_name] = index_id;   //如果有对应的table，那么就更新
        }
    }

    index_info->Init(meta_data, tables_[table_id], buffer_pool_manager_);
    indexes_[index_id] = index_info;
    buffer_pool_manager_->UnpinPage(page_id, false);
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) 
{
    auto table_it = tables_.find(table_id);
    if (table_it == tables_.end()) 
        return DB_TABLE_NOT_EXIST;
    table_info = table_it->second;
    return DB_SUCCESS;
}