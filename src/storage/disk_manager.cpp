#include "storage/disk_manager.h"

#include <sys/stat.h>
#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if(p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() 
{
    uint32_t meta_data_uint[PAGE_SIZE/4]; 
    memcpy(meta_data_uint, meta_data_, 4096);   //读取meta_data到一个新的数组中用于修改

    size_t page_id;
    // 寻找第一个没有满额的extent
    uint32_t extent_id = 0;

    //meta_data_uint[2]开始存储每个extent已经分配的page数量，meta_data_uint[0]代表page的总数，meta_data_uint[1]代表extent的总数
    while (*(meta_data_uint+2+extent_id) == BITMAP_SIZE) //如果分配的page的数量等于BITMAP_SIZE(Bitmap的size即为该分区page的总数)，说明这个extent已经满了，需要寻找下一个extent
    {
        extent_id++;
    };

    // 读取对应extent的bitmap_page，寻找第一个free的page
    char Bitmap[PAGE_SIZE];
    page_id_t BitmapPhysicalID = 1 + extent_id * (BITMAP_SIZE + 1);   //计算Bitmap的物理页号
    ReadPhysicalPage(BitmapPhysicalID, Bitmap);   //读取bitmap_page

    BitmapPage<PAGE_SIZE> *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(Bitmap); //将char*转换为BitmapPage*，方便操作（使用已实现的类BitmapPage）
    uint32_t next_free_page = bitmap_page->GetNextFreePage();
    page_id = extent_id * BITMAP_SIZE + next_free_page;   //计算逻辑页号

    bitmap_page->AllocatePage(next_free_page);
    // 修改meta_data
    if (extent_id >= *(meta_data_uint+1))
        ++ *(meta_data_uint+1);
    ++ *(meta_data_uint+2+extent_id);
    ++ *(meta_data_uint);

    memcpy(meta_data_,meta_data_uint, 4096);
    WritePhysicalPage(META_PAGE_ID, meta_data_);    //将修改后的meta_data写回磁盘
    WritePhysicalPage(BitmapPhysicalID, Bitmap);    //将修改后的bitmap_page写回磁盘
    return page_id; //返回逻辑页号
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) 
{
    char bitmap[PAGE_SIZE];
    size_t pages_per_extent = 1 + BITMAP_SIZE;
    page_id_t bitmap_physical_id = 1 + (logical_page_id / BITMAP_SIZE) * pages_per_extent;  //计算bitmap_page的物理页号
    ReadPhysicalPage(bitmap_physical_id, bitmap);   //读取bitmap_page
    auto *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap); //将char*转换为BitmapPage*，方便操作（使用已实现的类BitmapPage）

    bitmap_page->DeAllocatePage(logical_page_id % BITMAP_SIZE);     //利用已实现的类BitmapPage中的DeAllocatePage()函数释放page

    uint32_t meta_data_uint[PAGE_SIZE/4];
    memcpy(meta_data_uint, meta_data_, 4096);   //读取meta_data到一个新的数组中用于修改
    //   修改meta_data
    uint32_t extent_id = logical_page_id / BITMAP_SIZE;
    if (-- *(meta_data_uint + 2 + extent_id) == 0) -- *( meta_data_uint + 1 );  //如果该extent的page数量为0，说明该extent已经空了，需要减少extent的数量
    -- *(meta_data_uint);   //总的page数量减少1

    memcpy(meta_data_,meta_data_uint, 4096);
    WritePhysicalPage(META_PAGE_ID, meta_data_);    //将修改后的meta_data写回磁盘
    WritePhysicalPage(bitmap_physical_id, bitmap);  //将修改后的bitmap_page写回磁盘
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) 
{
    char bitmap[PAGE_SIZE];
    size_t pages_per_extent = 1 + BITMAP_SIZE;
    page_id_t bitmap_physical_id = 1 + (logical_page_id / BITMAP_SIZE) * pages_per_extent;  //计算bitmap_page的物理页号
    ReadPhysicalPage(bitmap_physical_id, bitmap);   //读取bitmap_page

    BitmapPage<PAGE_SIZE> *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap); //将char*转换为BitmapPage*，方便操作（使用已实现的类BitmapPage）

    if (bitmap_page->IsPageFree(logical_page_id % BITMAP_SIZE)) return true;    //利用已实现的类BitmapPage中的IsPageFree()函数判断page是否空闲
    return false;   //如果不空闲，返回false
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id)
{
    page_id_t extent_num = logical_page_id / BITMAP_SIZE + 1; // 相当于要加上的位图页的个数
    return logical_page_id + 1 + extent_num;    //位图页个数加上逻辑页数加1即为物理页数
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}