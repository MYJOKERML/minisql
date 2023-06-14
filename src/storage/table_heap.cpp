#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Transaction *txn) 
{
    if (row.GetSerializedSize(schema_) > PAGE_SIZE - 32)                                                    //如果size比数据页除去表头之后的剩余空间还大，说明每个数据页都不能容纳，返回false
    {  
        return false;
    }
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(GetFirstPageId()));           //从buffer中取出第一个数据页（从堆表的第一个数据页开始遍历）
    while (true) 
    {
        // If the page could not be found, then abort the transaction.
        if (page == nullptr)                                                                                
        {
            return false;
        }
        if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_))                              //如果页中有空间可以插入，即page的InsertTuple函数返回true
        {
            buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);                                  //写入后该页不再被调用，使用UnpinPage函数，将pin_count减一，由于这里写入了数据，所有该页变成了脏页，所以第二个参数is_dirty为true
            return true;                                                                                    //插入成功后返回True
        }

        buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);                                     //如果该页没有空间可以插入，使用UnpinPage函数，将pin_count减一，由于这里没有写入数据，所以第二个参数is_dirty为false
        page_id_t next = page->GetNextPageId();                                                             //由于该页没有空间可以插入，所以要获取下一个数据页的id
        if (next != INVALID_PAGE_ID)                                                                        //如果下一个数据页的id不是INVALID_PAGE_ID，说明还有下一个数据页
        {
            page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next));
        } 
        else                                                                                                //如果后面没有数据页了，就new一个数据页
        {
            auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next));             //new一个数据页，将该页的page_id放在next中
            page->SetNextPageId(next);                                                                      //将该页的next_page_id设置为next
            new_page->Init(next, page->GetPageId(), log_manager_, txn);                                     //初始化该页，将该页插入到堆表中(其中第二个参数为prev_page_id)
            new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);                          //将tuple插入到该页中
            buffer_pool_manager_->UnpinPage(next, true);                                                    //写入后该页不再被调用，使用UnpinPage函数，将pin_count减一，由于这里写入了数据，所有该页变成了脏页，所以第二个参数is_dirty为true
            return true;
        }
    }
}


bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) 
{
    // rid is old row, get its page and update
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));                    //获取rid所在的数据页
    if (page == nullptr) return false;                                                                              //如果该页不存在，返回false
    Row old(rid);                                                                                                   //声明一个row，保存了old row的信息
    if (!GetTuple(&old, txn)) {                                                                                     //如果获取不到old row，返回false
        return false;
    }

    page->UpdateTuple(row, &old, schema_, txn, lock_manager_, log_manager_);                             //调用该页的UpdateTuple函数，更新该页中的tuple

    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);                                              //写入后该页不再被调用，使用UnpinPage函数，将pin_count减一，由于这里写入了数据，所有该页变成了脏页，所以第二个参数is_dirty为true
    return true;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) 
{
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));                    //根据RowId找到对应的页
    if(page == nullptr)                                                                                             //如果该页不存在，输出错误信息
    {
        LOG(WARNING) << "The page does not exist" << std::endl;
    }
    // delete
    page->WLatch();
    page->ApplyDelete(rid, txn, log_manager_);                                                                      //调用该页的ApplyDelete函数，将该页中的tuple删除
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);                                                  //写入后该页不再被调用，使用UnpinPage函数，将pin_count减一，由于这里删除了一条记录，所以该页变成了脏页，所以第二个参数is_dirty为true
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Transaction *txn) 
{
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));        //根据row中的row_id找到对应的页
    if (page == nullptr) return false;
    page->RLatch();
    bool is_true = page->GetTuple(row, schema_, txn, lock_manager_);                                               //调用该页的GetTuple函数，将该页中的tuple读出来
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);                                                //使用UnpinPage函数，将pin_count减一，由于这里没有写入数据，所以第二个参数is_dirty为false
    return is_true;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Transaction *txn) 
{
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));                 //从buffer中取出第一个数据页
    RowId rid;
    page->RLatch();
    page->GetFirstTupleRid(&rid);                                                                               //调用该页的GetFirstTupleRid函数，获取该页中第一条记录的rid
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(first_page_id_, false);                                                     //使用UnpinPage函数，将pin_count减一，由于这里没有写入数据，所以第二个参数is_dirty为false
    return TableIterator(this, rid);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() 
{
    return TableIterator(this,INVALID_ROWID);                                                                   //使用INVALID_ROWID标注end，作为尾迭代器
}
