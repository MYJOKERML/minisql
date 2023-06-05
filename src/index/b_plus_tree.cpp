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
    root_page_id_ = INVALID_PAGE_ID;
    auto index_roots_page = buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID);
    auto index_roots = reinterpret_cast<IndexRootsPage*>(index_roots_page->GetData());
    page_id_t* RootPtr = new page_id_t();
    if (index_roots->GetRootId(index_id, RootPtr)) // 如果根节点存在
    {
        root_page_id_ = *RootPtr; // 更新根节点id
    }
    delete RootPtr; 
    buffer_pool_manager->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
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
    auto leaf =  reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key, root_page_id_, true)); // 找到叶子节点
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
    UpdateRootPageId(true);
    root_page_id_ = page->GetPageId();  // 更新根节点id
    auto root = reinterpret_cast<LeafPage *>(page->GetData()); 
    root->Init(root_page_id_, INVALID_PAGE_ID, sizeof(GenericKey *), leaf_max_size_);
    root->Insert(key, value, processor_);  // 插入到根节点
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
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
    auto leaf = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key, root_page_id_, true)); // 找到叶子节点
    RowId tmp;
    // LOG(WARNING) << leaf->GetSize() << std::endl;
    // LOG(WARNING) << leaf->GetMaxSize() << std::endl;
    if (leaf->Lookup(key,tmp,processor_)) // 如果key已经存在
    {
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
        return false;
    }
    if (leaf->GetSize() < leaf->GetMaxSize()) // 如果叶子节点未满
    {
        leaf->Insert(key, value, processor_); // 直接插入
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
        return true;
    }
    auto new_leaf = Split(leaf, transaction); // 否则分裂

    if (processor_.CompareKeys(key, new_leaf->KeyAt(0)) < 0) // 如果key小于新叶子节点的第一个key
    {
        leaf->Insert(key, value, processor_); // 插入到旧叶子节点
    }
    else // 否则插入到新叶子节点
    {
        new_leaf->Insert(key, value, processor_);
    }

    InsertIntoParent(leaf, new_leaf->KeyAt(0), new_leaf, transaction); // 插入到父节点
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);
    return true;
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
    auto page = buffer_pool_manager_->NewPage(root_page_id_);
    if (page == nullptr) 
    {
        throw ("Out of memory!");
    }
    auto new_node = reinterpret_cast<InternalPage *>(page->GetData());
    new_node->Init(page->GetPageId(), node->GetParentPageId(),sizeof(GenericKey *), internal_max_size_);
    node->MoveHalfTo(new_node, buffer_pool_manager_);
    return new_node;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Transaction *transaction) {
    auto page = buffer_pool_manager_->NewPage(root_page_id_);
    if (page == nullptr) {
        throw ("Out of memory!");
    }
    auto new_node = reinterpret_cast<LeafPage *>(page->GetData());
    new_node->Init(page->GetPageId(), node->GetParentPageId(), sizeof(GenericKey *), leaf_max_size_);
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
        root->Init(page->GetPageId(), INVALID_PAGE_ID, sizeof(GenericKey *), internal_max_size_);
        root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
        root_page_id_ = root->GetPageId(); // 更新根节点id
        old_node->SetParentPageId(root_page_id_); // 更新old_node的父节点id
        new_node->SetParentPageId(root_page_id_); // 更新new_node的父节点id
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
        return;
    }
    auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(old_node->GetParentPageId())->GetData());
    if (parent->GetSize() < parent->GetMaxSize()) // 如果父节点未满
    {
        parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId()); // 直接插入
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
        return;
    }
    auto new_parent = Split(parent, transaction); // 否则分裂
    if (processor_.CompareKeys(key, new_parent->KeyAt(0)) < 0) // 如果key小于新父节点的第一个key
    {
        parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId()); // 插入到旧父节点
    }
    else // 否则插入到新父节点
    {
        new_parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    }
    InsertIntoParent(parent, new_parent->KeyAt(0), new_parent, transaction); // 插入到父节点
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_parent->GetPageId(), true);
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
    if (IsEmpty()) // 如果树为空
    {
        return;
    }
    auto leaf = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key, root_page_id_, true)); // 找到叶子节点
    RowId tmp;
    if (!leaf->Lookup(key,tmp,processor_)) // 如果key不存在
    {
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
        return;
    }
    leaf->RemoveAndDeleteRecord(key, processor_); // 删除key
    if (AdjustRoot(leaf)) // 如果需要调整根节点
    {
        return;
    }
    CoalesceOrRedistribute(leaf, transaction); // 否则合并或重分配
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    return;
}

bool BPlusTree::CoalesceOrRedistribute(BPlusTreeLeafPage *&Node, Transaction *transaction) 
{ 
    if (Node->IsRootPage()) // 如果node是根节点
    {
        return AdjustRoot(Node); // 调整根节点
    }
    auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(Node->GetParentPageId())->GetData());
    int index = parent->ValueIndex(Node->GetPageId()); // 找到node在父节点中的位置
    if (index == 0) // 如果node是父节点的第一个孩子
    {
        auto sibling = reinterpret_cast<BPlusTreeLeafPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(1))->GetData()); // 找到node的右兄弟
        if (sibling->GetSize() + Node->GetSize() > Node->GetMaxSize()) // 如果右兄弟的大小加上node的大小大于node的最大大小
        {
            Redistribute(sibling, Node, 0); // 重分配
            buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
            buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
            return false;
        }
        else // 否则合并
        {
            Coalesce(sibling, Node, parent, 0, transaction);
            buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
            return true;
        }
    }
    else // 如果node不是父节点的第一个孩子
    {
        auto sibling = reinterpret_cast<BPlusTreeLeafPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index - 1))->GetData()); // 找到node的左兄弟
        if (sibling->GetSize() + Node->GetSize() > Node->GetMaxSize()) // 如果左兄弟的大小加上node的大小大于node的最大大小
        {
            Redistribute(sibling, Node, 1); // 重分配
            buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
            buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
            return false;
        }
        else // 否则合并
        {
            Coalesce(sibling, Node, parent, index - 1, transaction);
            buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
            return true;
        }
    }
}


bool BPlusTree::CoalesceOrRedistribute(BPlusTreeInternalPage *&Node, Transaction *transaction) 
{
    if (Node->IsRootPage()) // 如果node是根节点
    {
        return AdjustRoot(Node); // 调整根节点
    }
    auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(Node->GetParentPageId())->GetData());
    int index = parent->ValueIndex(Node->GetPageId()); // 找到node在父节点中的位置
    if (index == 0) // 如果node是父节点的第一个孩子
    {
        auto sibling = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(1))->GetData()); // 找到node的右兄弟
        if (sibling->GetSize() + Node->GetSize() > Node->GetMaxSize()) // 如果右兄弟的大小加上node的大小大于node的最大大小
        {
            Redistribute(sibling, Node, 0); // 重分配
            buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
            buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
            return false;
        }
        else // 否则合并
        {
            Coalesce(sibling, Node, parent, 0, transaction);
            buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
            return true;
        }
    }
    else // 如果node不是父节点的第一个孩子
    {
        auto sibling = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index - 1))->GetData()); // 找到node的左兄弟
        if (sibling->GetSize() + Node->GetSize() > Node->GetMaxSize()) // 如果左兄弟的大小加上node的大小大于node的最大大小
        {
            Redistribute(sibling, Node, 1); // 重分配
            buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
            buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
            return false;
        }
        else // 否则合并
        {
            Coalesce(sibling, Node, parent, index - 1, transaction);
            buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
            return true;
        }
    }
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
    neighbor_node->MoveAllTo(node); // 将neighbor_node的所有元素移动到node
    parent->Remove(index); // 在父节点中删除node
    if (parent->GetSize() < parent->GetMinSize()) // 如果父节点的大小小于最小值
    {
        return CoalesceOrRedistribute(parent, transaction); // 合并或重分配
    }
    return false;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) 
{
    neighbor_node->MoveAllTo(node, 0, buffer_pool_manager_); // 将neighbor_node的所有元素移动到node
    parent->Remove(index); // 在父节点中删除node
    if (parent->GetSize()  < parent->GetMinSize()) // 如果父节点的大小为0
    {
        return CoalesceOrRedistribute(parent, transaction); // 合并或重分配
    }
    return false;
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
    if (index == 0) // 如果node是父节点的第一个孩子
    {
        neighbor_node->MoveFirstToEndOf(node); // 将neighbor_node的第一个元素移动到node的最后
    }
    else // 如果node不是父节点的第一个孩子
    {
        neighbor_node->MoveLastToFrontOf(node); // 将neighbor_node的最后一个元素移动到node的最前
    }
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) 
{
    auto parent_page_id = node->GetParentPageId();
    auto parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
    auto parent = reinterpret_cast<BPlusTreeInternalPage*>(parent_page->GetData());
    if (index == 0) // 如果node是父节点的第一个孩子
    {
        auto key = parent->KeyAt(parent->ValueIndex(neighbor_node->GetPageId()));
        neighbor_node->MoveFirstToEndOf(node,key,buffer_pool_manager_); // 将neighbor_node的第一个元素移动到node的最后
    }
    else // 如果node不是父节点的第一个孩子
    {
        auto key = parent->KeyAt(parent->ValueIndex(node->GetPageId()));
        neighbor_node->MoveLastToFrontOf(node,key,buffer_pool_manager_); // 将neighbor_node的最后一个元素移动到node的最前
    }
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
    if (old_root_node->IsLeafPage()) // 如果old_root_node是叶子节点
    {
        if (old_root_node->GetSize() == 0) // 如果old_root_node的大小为0
        {
            root_page_id_ = INVALID_PAGE_ID; // 更新根节点id
            UpdateRootPageId(false);
            return true;
        }
        return false;
    }
    auto root = reinterpret_cast<InternalPage *>(old_root_node);
    if (root->GetSize() == 1) // 如果old_root_node的大小为1
    {
        root_page_id_ = root->RemoveAndReturnOnlyChild(); // 更新根节点id
        auto new_root = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
        new_root->SetParentPageId(INVALID_PAGE_ID); // 更新new_root的父节点id
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
        UpdateRootPageId(false);
        return true;
    }
    return false;
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
    Page * page = FindLeafPage(key, root_page_id_,true);
    auto leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(page->GetData());
    return IndexIterator(true, &processor_, leaf_page, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() 
{
    return IndexIterator(false, &processor_, nullptr, buffer_pool_manager_);
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
    auto page = buffer_pool_manager_->FetchPage(page_id);
    auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (node->IsLeafPage()) // 如果node是叶子节点
    {
        return page;
    }
    auto internal = reinterpret_cast<InternalPage *>(node);
    if (leftMost) // 如果需要找到最左边的叶子节点
    {
        return FindLeafPage(key, internal->ValueAt(0), true); // 递归查找
    }
    int index = internal->Lookup(key, processor_); // 找到key在node中的位置
    if (index == -1) // 如果key不存在
    {
        return FindLeafPage(key, internal->ValueAt(internal->GetSize() - 1), false); // 递归查找
    }
    return FindLeafPage(key, internal->ValueAt(index), false); // 递归查找
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
    auto index_roots_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
    if (index_roots_page == nullptr) {
        throw ("out of memory");
    }
    auto index_roots = reinterpret_cast<IndexRootsPage *>(index_roots_page->GetData());
    if (insert_record) {
        index_roots->Insert(index_id_, root_page_id_);
    } else {
        index_roots->Update(index_id_, root_page_id_);
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