#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) 
{
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetMaxSize(max_size);
    SetKeySize(key_size);
    SetSize(0);
    SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) 
{
    return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) 
{
    memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const 
{
    return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) 
{
    *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const 
{
    for (int i = 0; i < GetSize(); ++i) 
    {
        if (ValueAt(i) == value)
        return i;
    }
    return -1;
}

void *InternalPage::PairPtrAt(int index) 
{
    return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) 
{
    memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) 
{
    for (int i = 1; i < GetSize(); ++i)
    {
        if (KM.CompareKeys(KeyAt(i), key) > 0)
        {
          return ValueAt(i-1);
        }
    }
    return ValueAt(GetSize() - 1);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) 
{
    SetSize(2);                         // 新的节点中首先把旧的指针放入，再放入新的key和新的指针
    SetValueAt(0, old_value);           // 旧的指针放入
    SetKeyAt(1, new_key);               // 新的key放入
    SetValueAt(1, new_value);           // 新的指针放入
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) 
{
    int index = ValueIndex(old_value);
    if (index == -1)                        // 没有找到old_value，直接返回节点的大小，因为不能插入
    {
        return GetSize();
    }
    IncreaseSize(1);                        // 否则，增加节点的大小
    for (int i = GetSize() - 1; i > index + 1; --i)     //从最后对节点进行移动
    {
        SetValueAt(i, ValueAt(i - 1));
        SetKeyAt(i, KeyAt(i - 1));
    }
    SetValueAt(index + 1, new_value);       // 插入新的指针
    SetKeyAt(index + 1, new_key);           // 插入新的key
    return GetSize();                       // 返回节点的大小
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) 
{
    int size = GetSize();                   // 获取当前节点的大小
    int half = size / 2;                    // 获取一半的大小
    recipient->CopyNFrom(pairs_off + half * pair_size, size - half, buffer_pool_manager);       // 把一半的数据拷贝到recipient中
    SetSize(half);                          // 设置当前节点的大小
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) 
{
    for(int i=0; i<size; i++)           // 遍历所有的数据
    {
        SetKeyAt(GetSize() + i, reinterpret_cast<GenericKey *>(src + i * pair_size + key_off));     // 逐步将key拷贝到当前节点中
        SetValueAt(GetSize() + i, *reinterpret_cast<page_id_t *>(src + i * pair_size + val_off));   // 逐步将value拷贝到当前节点中
        auto page = buffer_pool_manager->FetchPage(ValueAt(GetSize() + i));                         // 获取value对应的数据页
        auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
        node->SetParentPageId(GetPageId());                                             // 设置子节点对应的数据页的父节点为当前节点
        buffer_pool_manager->UnpinPage(ValueAt(GetSize() + i), true);
    }
    IncreaseSize(size);                                                // 设置当前节点的大小
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) 
{
    int size = GetSize();                   // 获取当前节点的大小
    for (int i = index; i < size - 1; ++i)  // 从index开始，将后面的数据向前移动
    {
        SetValueAt(i, ValueAt(i + 1));
        SetKeyAt(i, KeyAt(i + 1));
    }
    IncreaseSize(-1);                       // 减少当前节点的大小
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() 
{
    IncreaseSize(-1);                       // 减少当前节点的大小
    return ValueAt(0);                      // 返回当前节点的value
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) 
{
    SetKeyAt(0, middle_key);                // 设置中间的key
    recipient->CopyNFrom(pairs_off, GetSize(), buffer_pool_manager);     // 将当前节点的数据拷贝到recipient中
    SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) 
{
    SetKeyAt(0, middle_key);
    recipient->CopyLastFrom(KeyAt(0), ValueAt(0), buffer_pool_manager);
    for (int i = 0; i < GetSize() - 1; ++i) {
        SetValueAt(i, ValueAt(i + 1));
        SetKeyAt(i, KeyAt(i + 1));
    }
    IncreaseSize(-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) 
{
    SetKeyAt(GetSize(), key);
    SetValueAt(GetSize(), value);
    IncreaseSize(1);
    auto page = buffer_pool_manager->FetchPage(value);
    auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(value, true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) 
{
    recipient->SetKeyAt(0, middle_key);
    recipient->CopyFirstFrom(KeyAt(GetSize() - 1), ValueAt(GetSize() - 1), buffer_pool_manager);
    buffer_pool_manager->UnpinPage(ValueAt(GetSize() - 1), true);
    IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(GenericKey* key, const page_id_t value, BufferPoolManager *buffer_pool_manager)
{
    for (int i = GetSize(); i > 0; --i) 
    {
        SetValueAt(i, ValueAt(i - 1));
        SetKeyAt(i, KeyAt(i - 1));
    }
    SetKeyAt(0, key);
    SetValueAt(0, value);
    IncreaseSize(1);
    auto page = buffer_pool_manager->FetchPage(value);
    auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(value, true);
}