#ifndef MINISQL_INDEX_ITERATOR_H
#define MINISQL_INDEX_ITERATOR_H

#include "page/b_plus_tree_leaf_page.h"

class IndexIterator {
  using LeafPage = BPlusTreeLeafPage;

 public:
  // you may define your own constructor based on your member variables
  explicit IndexIterator();

  explicit IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index = 0);

   explicit IndexIterator(bool type, KeyManager *processor, BPlusTreeLeafPage *page, BufferPoolManager *buffer_pool_manager);
  ~IndexIterator();

  /** Return the key/value pair this iterator is currently pointing at. */
  std::pair<GenericKey *, RowId> operator*();

  /** Move to the next key/value pair.*/
  IndexIterator &operator++();

  /** Return whether two iterators are equal */
  bool operator==(const IndexIterator &itr) const;

  /** Return whether two iterators are not equal. */
  bool operator!=(const IndexIterator &itr) const;

  int GetItemIndex() const { return item_index; }

 private:
  page_id_t current_page_id{INVALID_PAGE_ID};
  LeafPage *page{nullptr};
  int item_index{0};
  std::pair<GenericKey *, RowId>  data;
  BufferPoolManager *buffer_pool_manager{nullptr};
  KeyManager *Processor;
  // add your own private member variables here
};

#endif  // MINISQL_INDEX_ITERATOR_H
