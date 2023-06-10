#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id)  
{
    if (page_table_.count(page_id))                             //如果能够在buffer找到该页
    {
        replacer_->Pin(page_table_[page_id]);                   //调用该页，将该页从DeleteList中删除
        pages_[page_table_[page_id]].pin_count_++;              //该页的pin_count加一
        return &(pages_[page_table_[page_id]]);                 //返回该页的指针
    }
    else                                                        //如果在buffer中找不到该页
    {
        frame_id_t FreePageIndex;                             //声明变量，用于表示该页在buffer中的位置（下标）
        if (free_list_.size() > 0)                              //如果free_list_中还有空位
        {
            FreePageIndex = free_list_.back();                //从free_list_中取出一个空位
            free_list_.pop_back();                              //free_list空位数减一
        } 
        else if (replacer_->Size() > 0)                         //如果lru_replacer中有可替换的页
        {
            frame_id_t *free_frame_receiver = new frame_id_t;   //用于接收replacer中的一个空位，因为要传一个指针到Victim函数中
            replacer_->Victim(free_frame_receiver);             //从replacer中取出一个空位到free_frame_receiver中
            FreePageIndex = *free_frame_receiver;             //将free_frame_receiver中的空位取出
            delete free_frame_receiver;                         //释放内存空间
        } 
        else                                                    //如果free_list没有空位且replacer中没有可被替换的页
        {
            return nullptr;
        }
        if (pages_[FreePageIndex].is_dirty_)                  //如果该页是dirty的，就将该页写回disk
        {
            disk_manager_->WritePage(pages_[FreePageIndex].page_id_, pages_[FreePageIndex].data_);
        }
        page_table_.erase(pages_[FreePageIndex].page_id_);    //更新page_table_，将该页原先对应的那一条记录删除
        pages_[FreePageIndex].ResetMemory();                  //将该页的data_清空
        pages_[FreePageIndex].is_dirty_ = false;              //由于该页原先的数据已经被写回disk，所以将is_dirty_置为false
        pages_[FreePageIndex].page_id_ = page_id;             //更新该页对应的磁盘id
        page_table_[page_id] = FreePageIndex;                 //更新page_table_，将该页对应的那一条记录更新
        disk_manager_->ReadPage(page_id, pages_[FreePageIndex].data_);    //从disk中读取该页的数据
        replacer_->Pin(FreePageIndex);                        //引用该页，并将该页从DeleteList中删除
        pages_[FreePageIndex].pin_count_++;                   //该页的pin_count加一
        return &(pages_[FreePageIndex]);                      //返回该页的指针
    }
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) 
{
    frame_id_t FreePageIndex;
    if (free_list_.size() > 0)                                  //如果free_list_中还有空位
    {
        FreePageIndex = free_list_.back();
        free_list_.pop_back();
    } 
    else if (replacer_->Size() > 0)                             //如果replacer中有可替换的页
    {
        frame_id_t *free_frame_receiver = new frame_id_t;
        replacer_->Victim(free_frame_receiver); 
        FreePageIndex = *free_frame_receiver;
        delete free_frame_receiver;
    } 
    else 
    {
        return nullptr;
    }
    if (pages_[FreePageIndex].is_dirty_)                      //如果该页是dirty的，就将该页写回disk
    {
        disk_manager_->WritePage(pages_[FreePageIndex].page_id_, pages_[FreePageIndex].data_);
    }
    page_table_.erase(pages_[FreePageIndex].page_id_);        //更新page_table_，将该页原先对应的那一条记录删除
    page_id = AllocatePage();                                   //在disk中分配一个新的页，得到一个新的page_id
    pages_[FreePageIndex].ResetMemory();                      //将该页的data_清空
    pages_[FreePageIndex].is_dirty_ = false;                  //由于该页原先的数据已经被写回disk，所以将is_dirty_置为false
    pages_[FreePageIndex].page_id_ = page_id;                 //更新该页对应的磁盘id
    page_table_[page_id] = FreePageIndex;                     //更新page_table_，将该页对应的那一条记录更新
    replacer_->Pin(FreePageIndex);                            //引用该页，并将该页从DeleteList中删除
    pages_[FreePageIndex].pin_count_ = 1;                     //该页的pin_count置为1
    return &pages_[FreePageIndex];                            //返回该页的指针
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) 
{
    if (page_table_.count(page_id) == 0)                        //如果在buffer中找不到该页
    {
        DeallocatePage(page_id);                                //就直接在disk中删除该页
        return true;
    } 
    else if (pages_[page_table_[page_id]].pin_count_ > 0)       //如果该页被pin，则不能删除
    {
        return false;
    } 
    else                                                        //如果该页在buffer中且没有被pin
    {
        replacer_->Pin(page_table_[page_id]);                   //利用pin将他从DeleteList中删除
        DeallocatePage(page_id);                                //在disk中删除该页
        pages_[page_table_[page_id]].ResetMemory();             //将该页的data_清空
        pages_[page_table_[page_id]].is_dirty_ = false;         //由于该页被删除，所以将is_dirty_置为false
        free_list_.emplace_back(page_table_[page_id]);          //由于该页被删除，所以将该空页的下标加入free_list_
        page_table_.erase(page_id);                             //更新page_table_，将该页原先对应的那一条记录删除
        return true;
    }
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty)        
{
    if (page_table_.count(page_id) == 0) return false;                  //如果在buffer中找不到该页
    pages_[page_table_[page_id]].pin_count_--;                          //如果找到该页，该页的pin_count减一
    pages_[page_table_[page_id]].is_dirty_ = pages_[page_table_[page_id]].is_dirty_ || is_dirty;    //如果该页原先是dirty的或者现在是dirty的，就将is_dirty_置为true
    if (pages_[page_table_[page_id]].pin_count_ == 0)                   //如果该页的pin_count为0，说明现在该页不被引用，就将该页放到DeleteList中
    {
        replacer_->Unpin(page_table_[page_id]);
    }
    return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) 
{
    if (page_table_.count(page_id) == 0) return false;                      //如果在buffer中找不到该页
    disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].data_);  //如果该页在buffer中，则将该页的数据写回disk
    pages_[page_table_[page_id]].is_dirty_ = false;                         //由于已经写回disk，所以将is_dirty_置为false
    return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;

    }
  }
  return res;
}