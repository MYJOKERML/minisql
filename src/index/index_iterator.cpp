#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) 
{
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id));
}


 IndexIterator::IndexIterator( bool type, KeyManager *processor, BPlusTreeLeafPage *Page, BufferPoolManager *BufferPoolManager){
    Processor = processor;
    buffer_pool_manager = BufferPoolManager;
    //begin
    if (type){
        item_index = 0; //从第一次结点中的第0号元素开始
        page = Page;
        current_page_id = Page->GetPageId();
        data = std::make_pair(page->KeyAt(item_index), page->ValueAt(item_index));
    }else{
        //end
        item_index = 0;
        page = Page;
        while(item_index  < page->GetSize())
        {
            item_index++;
        }
        current_page_id = Page->GetPageId();
        data = std::make_pair(page->KeyAt(item_index), page->ValueAt(item_index));
    }
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

std::pair<GenericKey *, RowId> IndexIterator::operator*() 
{
    return std::make_pair(page->KeyAt(item_index), page->ValueAt(item_index));
}

IndexIterator &IndexIterator::operator++() 
{
    if (item_index < page->GetSize() && page->GetNextPageId() == 0)
    {
        item_index++;
    }
    else if(page->GetNextPageId() !=0 && item_index + 1 < page->GetSize())
    {
        item_index++;
    }
    else 
    {
        page_id_t next_page_id = page->GetNextPageId();
        buffer_pool_manager->UnpinPage(current_page_id, false);
        current_page_id = next_page_id;
        if (current_page_id == INVALID_PAGE_ID) 
        {
            page = nullptr;
            item_index = 0;
        } 
        else 
        {
            page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id));
            item_index = 0;
        }
    }
    return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
    return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
    return !(*this == itr);
}