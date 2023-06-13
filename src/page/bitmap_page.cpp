#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset)  // 分配内存页
{
    if (page_allocated_ < GetMaxSupportedSize()) {
        page_allocated_++;
        page_offset = next_free_page_;  // 首先把当前空闲页的位置赋給page_offset用于返回
        bytes[page_offset / 8] = bytes[page_offset / 8] | (0x01 << (7 - page_offset % 8));  // 添加的那一位置1
        uint32_t index = 0;
        while (!IsPageFree(index) && index < GetMaxSupportedSize())  // 从零开始，找到下一个位置为0的页
        {
        index++;
        }
        next_free_page_ = index;  // 更新下一个空闲页的位置
        return true;
    }
    return false;  // 如果没有空间，就返回false
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
    if (!IsPageFree(page_offset)) {
        if (page_allocated_ == GetMaxSupportedSize()) {
            next_free_page_ = page_offset;  // 如果所有页是满的，那么下一个空闲页就是当前页
        }
        // 将删掉的那一位置0
        bytes[page_offset / 8] = bytes[page_offset / 8] & (~(0x01 << (7 - page_offset % 8)));
        page_allocated_--;
        return true;
    }
    return false;  // 如果当前页是空的，那么就返回false
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
    return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
    return (bytes[byte_index] & (0x80 >> bit_index)) ? false : true;
}

template <size_t PageSize>
uint32_t BitmapPage<PageSize>::GetNextFreePage() const {
  return next_free_page_;
};

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;