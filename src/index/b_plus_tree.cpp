#include "index/b_plus_tree.h"

#include <string>
#include <exception>
#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"
#include "gtest/gtest.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) 
{
    if(leaf_max_size_ == UNDEFINED_SIZE)
    {
      leaf_max_size_ = (int)((PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (KM.GetKeySize() + sizeof(RowId)) - 1);
    }
    if(internal_max_size_ == UNDEFINED_SIZE)
    {
      internal_max_size_ = (int)((PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (KM.GetKeySize() + sizeof(page_id_t)) - 1);
    }
    auto root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID));

    if (!root_page->GetRootId(index_id, &this->root_page_id_)) {
      this->root_page_id_ = INVALID_PAGE_ID;
    }
    buffer_pool_manager->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

void BPlusTree::Destroy(page_id_t current_page_id) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const 
{
    return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction) 
{
    if (IsEmpty()) // 如果树为空
    {
        return false;
    }
    auto leaf =  reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key, root_page_id_, false)->GetData()); // 找到叶子节点
    RowId value;
    bool ret = leaf->Lookup(key, value, processor_);   // 在叶子节点中查找
    if(ret)
        result.push_back(value);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Transaction *transaction) 
{
    if (IsEmpty())
    {
        StartNewTree(key, value);  // 如果树为空，新建树
        return true;
    }
    return InsertIntoLeaf(key, value, transaction);   // 否则插入叶子节点
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) 
{
    auto page = buffer_pool_manager_->NewPage(root_page_id_);
    if (page == nullptr) 
    {
        throw ("Out of memory!");
    }
    auto root = reinterpret_cast<LeafPage *>(page->GetData()); 
    root->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
    root->Insert(key, value, processor_);  // 插入到根节点
    buffer_pool_manager_->UnpinPage(root_page_id_, true);

    UpdateRootPageId(1);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Transaction *transaction) 
{
    Page* find_leaf_page = this->FindLeafPage(key,root_page_id_, false);

    auto tmp_leaf_page = reinterpret_cast<LeafPage*>(find_leaf_page->GetData());
    int org_size = tmp_leaf_page->GetSize();
    int new_size = tmp_leaf_page->Insert(key, value, processor_);

    if (new_size == org_size)
    {
        buffer_pool_manager_->UnpinPage(find_leaf_page->GetPageId(), false);
        return false;
    }
    else if (new_size < leaf_max_size_)
    {
        buffer_pool_manager_->UnpinPage(find_leaf_page->GetPageId(), true);
        return true;
    }
    else
    {
        auto sibling_leaf_node = Split(tmp_leaf_page,transaction);
        GenericKey * risen_key = sibling_leaf_node->KeyAt(0);
        InsertIntoParent(tmp_leaf_page, risen_key, sibling_leaf_node, transaction);
        buffer_pool_manager_->UnpinPage(find_leaf_page->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(sibling_leaf_node->GetPageId(), true);
        return true;
    }
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Transaction *transaction) 
{
    page_id_t page_id_;
    auto page = buffer_pool_manager_->NewPage(page_id_);
    if (page == nullptr) 
    {
        throw ("Out of memory!");
    }
    auto new_node = reinterpret_cast<InternalPage *>(page->GetData());
    new_node->Init(page_id_, INVALID_PAGE_ID ,processor_.GetKeySize(), internal_max_size_);
    node->MoveHalfTo(new_node, buffer_pool_manager_);
//    for(int i=0; i<new_node->GetSize(); i++)
//    {
//        auto child_page = buffer_pool_manager_->FetchPage(new_node->ValueAt(i));
//        auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
//        child_node->SetParentPageId(new_node->GetPageId());
//        buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
//    }
    return new_node;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Transaction *transaction) {
    page_id_t page_id_;
    auto page = buffer_pool_manager_->NewPage(page_id_);
    if (page == nullptr)
    {
        throw ("Out of memory!");
    }
    auto new_node = reinterpret_cast<BPlusTreeLeafPage *>(page->GetData());
    new_node->Init(page_id_, INVALID_PAGE_ID ,processor_.GetKeySize(), leaf_max_size_);
    node->MoveHalfTo(new_node);

    return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
                                 Transaction *transaction) 
{
    if (old_node->IsRootPage()) // 如果old_node是根节点
    {
        auto page = buffer_pool_manager_->NewPage(root_page_id_);
        if (page == nullptr)
        {
            throw ("Out of memory!");
        }
        auto root = reinterpret_cast<InternalPage *>(page->GetData());
        root->Init(page->GetPageId(), INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
        root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
        old_node->SetParentPageId(root_page_id_); // 更新old_node的父节点id
        new_node->SetParentPageId(root_page_id_); // 更新new_node的父节点id
        buffer_pool_manager_->UnpinPage(root_page_id_, true);

        UpdateRootPageId(0);
        return;
    }
    auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(old_node->GetParentPageId())->GetData());
    if (parent->GetSize() < parent->GetMaxSize()) // 如果父节点未满
    {
        new_node->SetParentPageId(parent->GetPageId()); // 更新new_node的父节点id
        parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId()); // 直接插入
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
        return;
    }
    auto new_parent = Split(parent, transaction); // 否则分裂
    new_parent->SetParentPageId(parent->GetParentPageId());
    if (processor_.CompareKeys(key, new_parent->KeyAt(0)) < 0) // 如果key小于新父节点的第一个key
    {
        parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId()); // 插入到旧父节点
        new_node->SetParentPageId(parent->GetPageId()); // 更新new_node的父节点id
    }
    else // 否则插入到新父节点
    {
        new_parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        new_node->SetParentPageId(new_parent->GetPageId()); // 更新new_node的父节点id
    }
    InsertIntoParent(parent, new_parent->KeyAt(0), new_parent, transaction); // 插入到父节点
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_parent->GetPageId(), true);
}

void BPlusTree::ClrDeletePages()
{
    for(int i=0; i<int(delete_pages.size()); i++)
    {
        buffer_pool_manager_->DeletePage(delete_pages[i]);
    }
    delete_pages.clear();
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Transaction *transaction) 
{

    if (IsEmpty())      // 如果树为空
    {
        return;
    }

    Page* leaf_page = FindLeafPage(key, root_page_id_, false);      // 找到叶子节点
    LeafPage *node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
    int org_size = node->GetSize();

    if (org_size == node->RemoveAndDeleteRecord(key, processor_))   // 如果删除后叶子节点大小不变，即没有删除成功
    {
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
        return;
    }
    else
    {
        bool if_deletion_occur = CoalesceOrRedistribute(node, transaction); // 合并或者重分配
        if(if_deletion_occur)
        {
            delete_pages.push_back(node->GetPageId());    // 如果删除后叶子节点大小为0，就将整个LeafPage删除
        }
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
        this->ClrDeletePages();
    }
}

bool BPlusTree::CoalesceOrRedistribute(BPlusTreeLeafPage *&node, Transaction *transaction)
{

    if (node->IsRootPage())
    {
        bool if_delete_root = AdjustRoot(node);
        return if_delete_root;
    }
    else if (node->GetSize() >= node->GetMinSize())
    {
        return false;
    }

    Page* fth_parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    auto tmp_parent_page = reinterpret_cast<BPlusTreeInternalPage *>(fth_parent_page->GetData());
    int index = tmp_parent_page->ValueIndex(node->GetPageId());
    int r_index;
    if(index == 0)
        r_index = 1;
    else
        r_index = index -1;

    Page* sibling_page = buffer_pool_manager_->FetchPage(
        tmp_parent_page->ValueAt(r_index));
    // sibling_page->WLatch();
    auto sibling_node = reinterpret_cast<BPlusTreeLeafPage*>(sibling_page->GetData());

    if (node->GetSize() + sibling_node->GetSize() >= node->GetMaxSize())
    {
        Redistribute(sibling_node, node, index);
        buffer_pool_manager_->UnpinPage(tmp_parent_page->GetPageId(), true);
//        sibling_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);

        return false;
    }
    else
    {
        bool if_should_delete = Coalesce(sibling_node, node, tmp_parent_page, index, transaction);
        if(if_should_delete)
        {
            delete_pages.push_back(tmp_parent_page->GetPageId());
        }
        buffer_pool_manager_->UnpinPage(tmp_parent_page->GetPageId(), true);
//        sibling_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);

        return true;
    }
    return false;
}


bool BPlusTree::CoalesceOrRedistribute(BPlusTreeInternalPage *&node, Transaction *transaction)
{

    if (node->IsRootPage())
    {
        bool if_delete_root = AdjustRoot(node);
        return if_delete_root;
    }
    else if (node->GetSize() >= node->GetMinSize())
    {
        return false;
    }
    else
    {}

    Page* fth_parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    auto tmp_parent_page = reinterpret_cast<BPlusTreeInternalPage *>(fth_parent_page->GetData());
    int index = tmp_parent_page->ValueIndex(node->GetPageId());
    int r_index;
    if(index == 0)
        r_index = 1;
    else
        r_index = index -1;

    Page* sibling_page = buffer_pool_manager_->FetchPage(
        tmp_parent_page->ValueAt(r_index));

    auto sibling_node = reinterpret_cast<BPlusTreeInternalPage*>(sibling_page->GetData());

    if (node->GetSize() + sibling_node->GetSize() >= node->GetMaxSize())
    {
        Redistribute(sibling_node, node, index);
        buffer_pool_manager_->UnpinPage(tmp_parent_page->GetPageId(), true);
//        sibling_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);

        return false;
    }
    else
    {
        bool if_should_delete = Coalesce(sibling_node, node, tmp_parent_page, index, transaction);
        if(if_should_delete)
        {
            delete_pages.push_back(tmp_parent_page->GetPageId());
        }
        buffer_pool_manager_->UnpinPage(tmp_parent_page->GetPageId(), true);
//        sibling_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);

        return true;
    }
    return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) 
{
    int r_index = index;
    if (index == 0)
    {
        r_index = 1;
//        std::swap(node, neighbor_node);
        LeafPage* tmp = node;
        node = neighbor_node;
        neighbor_node = tmp;
    }
    LeafPage* leaf_node = reinterpret_cast<LeafPage *>((node));
    LeafPage* last_leaf_node = reinterpret_cast<LeafPage *>((neighbor_node));
    leaf_node->MoveAllTo(last_leaf_node);
    buffer_pool_manager_->UnpinPage(last_leaf_node->GetPageId(), true);
    (parent)->Remove(r_index);
    bool if_got_deletion = CoalesceOrRedistribute(parent);
    return if_got_deletion;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) 
{
    int r_index = index;
    if (index == 0)
    {
        r_index = 1;
//        std::swap(node, neighbor_node);
        InternalPage* tmp = node;
        node = neighbor_node;
        neighbor_node = tmp;
    }
    GenericKey* middle_key = (parent)->KeyAt(r_index);

    InternalPage *internal_node = reinterpret_cast<InternalPage *>((node));
    InternalPage *last_internal_node = reinterpret_cast<InternalPage *>((neighbor_node));
    internal_node->MoveAllTo(last_internal_node, middle_key, buffer_pool_manager_);
    for(int i=0; i<last_internal_node->GetSize(); i++)
    {
        Page* child_page = buffer_pool_manager_->FetchPage(last_internal_node->ValueAt(i));
        auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
        child_node->SetParentPageId(last_internal_node->GetPageId());
        buffer_pool_manager_->UnpinPage(child_node->GetPageId(), true);
    }
        buffer_pool_manager_->UnpinPage(last_internal_node->GetPageId(), true);

    parent->Remove(r_index);
    bool if_got_deletion = CoalesceOrRedistribute(parent);
    return if_got_deletion;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) 
{
      LeafPage *leaf_node = reinterpret_cast<LeafPage *>(node);
      LeafPage *neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);
      Page* tmp_parent = buffer_pool_manager_->FetchPage(leaf_node->GetParentPageId());
      InternalPage* parent = reinterpret_cast<InternalPage*>(tmp_parent);

      if (index == 0)
      {
          neighbor_leaf_node->MoveFirstToEndOf(leaf_node);
          parent->SetKeyAt(1, neighbor_leaf_node->KeyAt(0));
      }
      else
      {
          neighbor_leaf_node->MoveLastToFrontOf(leaf_node);
          parent->SetKeyAt(index, leaf_node->KeyAt(0));
      }
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}

void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) 
{

      InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
      InternalPage *neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);
      Page* tmp_parent = buffer_pool_manager_->FetchPage(internal_node->GetParentPageId());
      InternalPage* parent = reinterpret_cast<InternalPage*>(tmp_parent);
      if (index == 0)
      {
          neighbor_internal_node->MoveFirstToEndOf(internal_node,
                                                   parent->KeyAt(1), buffer_pool_manager_);
          parent->SetKeyAt(1, neighbor_internal_node->KeyAt(0));
          for(int i=0; i<internal_node->GetSize(); i++)
          {
              Page* tmp_page = buffer_pool_manager_->FetchPage(internal_node->ValueAt(i));
              BPlusTreePage* tmp_bpt_page = reinterpret_cast<BPlusTreePage*>(tmp_page);
              tmp_bpt_page->SetParentPageId(internal_node->GetPageId());
          }
          for(int i=0; i<neighbor_internal_node->GetSize(); i++)
          {
              Page* tmp_page = buffer_pool_manager_->FetchPage(neighbor_internal_node->ValueAt(i));
              BPlusTreePage* tmp_bpt_page = reinterpret_cast<BPlusTreePage*>(tmp_page);
              tmp_bpt_page->SetParentPageId(neighbor_internal_node->GetPageId());
          }
      }
      else
      {
          neighbor_internal_node->MoveLastToFrontOf(internal_node,
                                                    parent->KeyAt(index), buffer_pool_manager_);
          parent->SetKeyAt(index, internal_node->KeyAt(0));
          for(int i=0; i<internal_node->GetSize(); i++)
          {
              Page* tmp_page = buffer_pool_manager_->FetchPage(internal_node->ValueAt(i));
              BPlusTreePage* tmp_bpt_page = reinterpret_cast<BPlusTreePage*>(tmp_page);
              tmp_bpt_page->SetParentPageId(internal_node->GetPageId());
          }
          for(int i=0; i<neighbor_internal_node->GetSize(); i++)
          {
              Page* tmp_page = buffer_pool_manager_->FetchPage(neighbor_internal_node->ValueAt(i));
              BPlusTreePage* tmp_bpt_page = reinterpret_cast<BPlusTreePage*>(tmp_page);
              tmp_bpt_page->SetParentPageId(neighbor_internal_node->GetPageId());
          }
      }
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);

}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) 
{
    if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1)
    {
        InternalPage *root_node =
            reinterpret_cast<InternalPage*>(old_root_node);
        Page* fth_child_page = buffer_pool_manager_->FetchPage(root_node->ValueAt(0));

        BPlusTreePage *tmp_child_node = reinterpret_cast<BPlusTreePage*>(fth_child_page->GetData());

        tmp_child_node->SetParentPageId(INVALID_PAGE_ID);

        this->root_page_id_ = tmp_child_node->GetPageId();
        UpdateRootPageId(0);

        buffer_pool_manager_->UnpinPage(fth_child_page->GetPageId(), true);
        return true;
    }
    return old_root_node->IsLeafPage() && old_root_node->GetSize() == 0;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() 
{
    Page * page = FindLeafPage(nullptr, root_page_id_, true);
    auto leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(page->GetData());
    return IndexIterator(true, &processor_, leaf_page, buffer_pool_manager_);
}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) 
{
    Page * page = FindLeafPage(key, root_page_id_,false);
    auto leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(page->GetData());
    auto iter = IndexIterator(true, &processor_, leaf_page, buffer_pool_manager_);
    while (processor_.CompareKeys((*iter).first, key) < 0) {
        ++iter;
    }
    return iter;
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() 
{
    GenericKey *key = new GenericKey();
    Page *page = FindLeafPage(key, root_page_id_,true);
    auto leaf_page = reinterpret_cast<BPlusTreeLeafPage*>(page->GetData());
    return IndexIterator(false, &processor_, leaf_page, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) 
{
    if (page_id == INVALID_PAGE_ID) return nullptr;
    auto node = buffer_pool_manager_->FetchPage(page_id);
    auto node_page = reinterpret_cast<BPlusTreePage *>(node->GetData());
    while (!node_page->IsLeafPage()) {
        auto internal_node = reinterpret_cast<BPlusTreeInternalPage *>(node_page);
        page_id_t child_page_id;
        if (leftMost) {
            child_page_id = internal_node->ValueAt(0);
        } else {
            child_page_id = internal_node->Lookup(key, processor_);
        }

        Page* child_page = buffer_pool_manager_->FetchPage(child_page_id);
        auto child_BP_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

        buffer_pool_manager_->UnpinPage(node->GetPageId(), false);

        node = child_page;
        node_page = child_BP_node;
    }

    return node;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) 
{
    auto tmp_index_root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));

    if (insert_record != 0) {
        tmp_index_root_page->Insert(index_id_, root_page_id_);
    } else {
        tmp_index_root_page->Update(index_id_, root_page_id_);
    }
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}