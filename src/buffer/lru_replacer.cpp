#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) 
{
    if (Size() > 0) //如果有元素
    {
        *frame_id = DeleteList.back();//返回最后一个元素
        DeleteList.pop_back();//删除最后一个元素
        Map.erase(*frame_id);//删除哈希表中的元素
        return true;//返回true
    }
    return false;
}
/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) 
{
    if (Map.count(frame_id)) 
    {//如果哈希表中有这个frame_id
        DeleteList.erase(Map[frame_id]);//删除这个frame_id
        Map.erase(frame_id);//删除哈希表中的这个frame_id
    }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) 
{
  if (Map.count(frame_id))  //如果哈希表中有这个frame_id，就不做任何操作
  {
    return;
  }
  DeleteList.emplace_front(frame_id); //如果哈希表中没有这个frame_id，就把这个frame_id放到链表的头部
  Map[frame_id] = DeleteList.begin(); //把这个frame_id放到哈希表中
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() 
{
    return DeleteList.size();
}