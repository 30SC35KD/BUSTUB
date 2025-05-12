//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.cpp
//
// Identification: src/storage/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new leaf page
 *
 * After creating a new leaf page from buffer pool, must call initialize method to set default values,
 * including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size.
 *
 * @param max_size Max size of the leaf node
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) { //UNIMPLEMENTED("TODO(P2): Add implementation.");
  SetMaxSize(max_size); // set max size to max size
  SetSize(0); // set current size to 0
  SetNextPageId(INVALID_PAGE_ID); // set next page id to 0
   SetPageType(IndexPageType::LEAF_PAGE); // set page type to leaf page
  SetPageID(0); // set page id to 0
  SetParentID(0); // set parent id to 0
  
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t {//UNIMPLEMENTED("TODO(P2): Add implementation."); 
  return next_page_id_; // return the next page id_t
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  //UNIMPLEMENTED("TODO(P2): Add implementation.");
  next_page_id_ = next_page_id; // set the next page id to the given value
}
/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { //UNIMPLEMENTED("TODO(P2): Add implementation.");
  
  // if (index < 0 || index > GetSize()) { // check if the index is valid
  //   throw Exception(ExceptionType::OUT_OF_RANGE, "Index out of range"); // throw exception if invalid
  // } else {
    return key_array_[index];  
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Find(const KeyType &key,ValueType *value, const KeyComparator &comparator) const -> bool {
  // 使用二分查找提高效率
  int left = 0;
  int right = GetSize() - 1;

  while (left <= right) {
    int mid = left + (right - left) / 2;
    int cmp = comparator(KeyAt(mid), key);

    if (cmp == 0) {
      // 找到目标键，返回对应的值
    if(value!=nullptr) *value=rid_array_[mid]; // 返回对应的 RID
      return true;
    } else if (cmp < 0) {
      // 如果目标键大于当前键，向右查找
      left = mid + 1;
    } else {
      // 如果目标键小于当前键，向左查找
      right = mid - 1;
    }
  }

  // 未找到目标键
  return false;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  // 插入键值对到叶子页中
  int index = GetSize();
  while (index > 0 && comparator(KeyAt(index - 1), key) > 0) {
      key_array_[index] = key_array_[index - 1];
      rid_array_[index] = rid_array_[index - 1];
      index--;
  }
  key_array_[index] = key;
  rid_array_[index] = value;
  SetSize(GetSize() + 1);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Give(BPlusTreeLeafPage *page) {
  // 将当前页的键值对复制到目标页中
  int mid=GetSize() / 2;
  for (int i = mid; i < GetSize(); i++) {
    page->key_array_[i - mid] = key_array_[i];
    page->rid_array_[i - mid] = rid_array_[i];
    
  }
  page->SetSize(GetSize()-mid);
  SetSize(mid); // 更新当前页的大小
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator)
{
  // 删除键值对
  int index = 0;
  while (index < GetSize() && comparator(KeyAt(index), key) < 0) {
    index++;
  }
  if (index < GetSize() && comparator(KeyAt(index), key) == 0) {
    // 找到目标键，删除对应的键值对
    for (int i = index; i < GetSize() - 1; i++) {
      key_array_[i] = key_array_[i + 1];
      rid_array_[i] = rid_array_[i + 1];
    }
    SetSize(GetSize() - 1); // 更新当前页的大小
  }
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::L_Lend(BPlusTreeLeafPage *bro_page,BPlusTreeInternalPage *parent,int index) 
{
  for(int i=GetSize();i>0;i--)
  {
    key_array_[i] = key_array_[i-1];
    rid_array_[i] = rid_array_[i-1];
  }

  KeyType borrowed_key = bro_page->KeyAt(bro_page->GetSize() - 1);
  ValueType borrowed_value = bro_page->rid_array_[bro_page->GetSize() - 1];
  // 将左兄弟的最后一个键值对插入到当前节点的第一个位置
  key_array_[0] = borrowed_key;
  rid_array_[0] = borrowed_value;
  // 更新当前节点的大小
  SetSize(GetSize() + 1);
  // 删除左兄弟的最后一个键值对
  bro_page->SetSize(bro_page->GetSize() - 1);
  parent->SetKeyAt(index, KeyAt(0)); // 更新父节点的键
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::R_Lend(BPlusTreeLeafPage *bro_page,BPlusTreeInternalPage *parent,int index) 
{
  KeyType borrowed_key = bro_page->KeyAt(0);
  ValueType borrowed_value = bro_page->rid_array_[0];
  // 将左兄弟的最后一个键值对插入到当前节点的第一个位置
  key_array_[GetSize()] = borrowed_key;
  rid_array_[GetSize()] = borrowed_value;
  // 更新当前节点的大小
  SetSize(GetSize() + 1);
  // 删除左兄弟的最后一个键值对
  for (int i = 0; i < bro_page->GetSize() - 1; i++) {
    bro_page->key_array_[i] = bro_page->key_array_[i + 1];
    bro_page->rid_array_[i] = bro_page->rid_array_[i + 1];
  }
  bro_page->SetSize(bro_page->GetSize() - 1);
  parent->SetKeyAt(index+1, bro_page->KeyAt(0)); // 更新父节点的键
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Merge(BPlusTreeLeafPage *sibling) {
  // 将兄弟节点的键值对追加到当前节点
  for (int i = 0; i < sibling->GetSize(); i++) {
    key_array_[GetSize() + i] = sibling->KeyAt(i);
    rid_array_[GetSize() + i] = sibling->rid_array_[i];
  }

  // 更新当前节点的大小
  SetSize(GetSize() + sibling->GetSize());

  // 更新链表指针
  SetNextPageId(sibling->GetNextPageId());
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertSafe() const->bool {
  // 如果当前节点的大小小于最大容量，则插入是安全的
  return GetSize() < GetMaxSize();
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteSafe(bool t) const->bool {
  // 如果当前节点的大小小于最大容量，则插入是安全的
  if(t) return GetSize() > 2;
  else return GetSize() > GetMinSize();
}
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
