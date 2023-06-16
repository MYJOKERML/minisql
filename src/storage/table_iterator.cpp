#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *TbHeap, RowId rowid) : table_heap(TbHeap) 
{
      if (rowid.GetPageId() != INVALID_PAGE_ID && TbHeap != nullptr)  //如果rid的page_id不是INVALID_PAGE_ID，说明该rid是有效的
      {
          row = new Row(rowid);
          table_heap->GetTuple(row, nullptr);
      }
      else  //如果rid的page_id是INVALID_PAGE_ID，说明该rid是无效的
      {
          row = new Row(INVALID_ROWID);
      }
}

TableIterator::TableIterator(const TableIterator &other) //拷贝构造函数
{ 
    table_heap = other.table_heap;
    row = other.row;
}

TableIterator::~TableIterator() {}

bool TableIterator::operator==(const TableIterator &itr) const 
{
    if(itr.row->GetRowId()==INVALID_ROWID&&row->GetRowId()==INVALID_ROWID)//如果两个迭代器都是尾迭代器，那么相等
    {
        return true;
    }
    else if(itr.row->GetRowId()==INVALID_ROWID||row->GetRowId()==INVALID_ROWID)//如果两个迭代器中有一个是尾迭代器，那么不相等
    {
        //LOG(WARNING)<< "end it 1 " << row->GetRowId().GetPageId() <<" "<<row->GetRowId().GetSlotNum() <<std::endl;
        return false;
    }
    else if(itr.row->GetRowId().GetPageId()==row->GetRowId().GetPageId()&&row->GetRowId().GetSlotNum()==itr.row->GetRowId().GetSlotNum())//如果两个迭代器的rowid相等，那么相等
    {
        return true;
    }
    else 
    {
        return false;
    }
}

bool TableIterator::operator!=(const TableIterator &itr) const {
    return !(*this == itr);
}

const Row &TableIterator::operator*() 
{
    return *row;
}

Row *TableIterator::operator->() {
    return row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
    table_heap = itr.table_heap;
    row = itr.row;
    return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
     //如果当前已经是非法的iter，则返回nullptr构成的
    if (row == nullptr || row->GetRowId() == INVALID_ROWID)
    {
        delete row;
        row = new Row(INVALID_ROWID);
        return *this;
    }
    auto page = reinterpret_cast<TablePage *>(table_heap->buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId())); // 获取当前页
    RowId new_id;
    page->RLatch();
    if (page->GetNextTupleRid(row->GetRowId(), &new_id)) { // 如果当前页有下一个tuple
        delete row;
        row = new Row(new_id);
        if (*this != table_heap->End())
        {
            table_heap->GetTuple(row, nullptr);
        }
        page->RUnlatch();
        table_heap->buffer_pool_manager_->UnpinPage(row->GetRowId().GetPageId(), false);
    }
    else
    {
        // 本页没有合适的，去找下一页直到找到可以用的页
        page_id_t next_page_id = page->GetNextPageId();
        while (next_page_id != INVALID_PAGE_ID) {
            // 还没到最后一页
            auto new_page = reinterpret_cast<TablePage *>(table_heap->buffer_pool_manager_->FetchPage(next_page_id));
            page->RUnlatch();
            table_heap->buffer_pool_manager_->UnpinPage(row->GetRowId().GetPageId(), false); // 释放上一页
            page = new_page;
            page->RLatch();
            if (page->GetFirstTupleRid(&new_id)) {
                // 如果找到可用的tuple则跳出循环并读rowid
                delete row;
                row = new Row(new_id);
                break;
            }
            next_page_id = page->GetNextPageId();
        }
        if (next_page_id != INVALID_PAGE_ID) // 如果next_page_id是合法的，则读取tuple
        {
            table_heap->GetTuple(row, nullptr); // 读取tuple
        }
        else  // 如果next_page_id是非法的，则返回nullptr构成的iter
        {
            delete row;
            row = new Row(INVALID_ROWID);
        }
        page->RUnlatch();
        table_heap->buffer_pool_manager_->UnpinPage(row->GetRowId().GetPageId(), false);// 释放当前页
    }
    return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
    TableIterator newit(table_heap, row->GetRowId());
    ++(*this);
    return TableIterator{newit};
}