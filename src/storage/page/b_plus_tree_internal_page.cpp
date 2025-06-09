//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_internal_page.cpp
//
// Identification: src/storage/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new internal page.
 *
 * Writes the necessary header information to a newly created page,
 * including set page type, set current size, set page id, set parent id and set max page size,
 * must be called after the creation of a new page to make a valid BPlusTreeInternalPage.
 *
 * @param max_size Maximal size of the page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) { 
  //UNIMPLEMENTED("TODO(P2): Add implementation."); 
  SetMaxSize(max_size); // 
  SetSize(1); 
 SetPageType(IndexPageType::INTERNAL_PAGE);
 SetPageID(0);
 SetParentID(0);
}

/**
 * @brief Helper method to get/set the key associated with input "index"(a.k.a
 * array offset).
 *
 * @param index The index of the key to get. Index must be non-zero.
 * @return Key at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  //UNIMPLEMENTED("TODO(P2): Add implementation.");
  
  return key_array_[index]; // return the key at index
}

/**
 * @brief Set key at the specified index.
 *
 * @param index The index of the key to set. Index must be non-zero.
 * @param key The new value for key
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  //UNIMPLEMENTED("TODO(P2): Add implementation.");
  
  key_array_[index] = key; // set the key at index to the new value
}

/**
 * @brief Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 *
 * @param index The index of the value to get.
 * @return Value at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  //NIMPLEMENTED("TODO(P2): Add implementation.");
  
  return page_id_array_[index]; // return the value at index
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Next(const KeyType &key, const KeyComparator &comparator) const -> page_id_t {
  // 如果页面为空，抛出异常
  // if (GetSize() == 0) {
  //   throw Exception(ExceptionType::INVALID, "Internal page is empty");
  // }

  // 二分查找目标键的位置
  int left = 1;  // 跳过第一个无效键
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (comparator(KeyAt(mid), key) <= 0) {
      // 如果 key >= KeyAt(mid)，继续向右查找
      left = mid + 1;
    } else {
      // 如果 key < KeyAt(mid)，继续向左查找
      right = mid - 1;
    }
  }

  // 返回对应的子页面 ID
  // 如果 key 小于所有键，返回第一个子页面 ID
  // 如果 key 大于所有键，返回最后一个子页面 ID
  return ValueAt(left - 1);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  // 插入键值对到叶子页中
  int index = GetSize();
  while (index > 1 && comparator(KeyAt(index - 1), key) > 0) {//移动元素，为插入的元素腾出位置
      key_array_[index] = key_array_[index - 1];
      page_id_array_[index] = page_id_array_[index - 1];
      index--;
  }
  key_array_[index] = key;
  page_id_array_[index] = value;
  SetSize(GetSize()+ 1);//更新节点大小
}
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Give(BPlusTreeInternalPage *page){
  // 将当前页的键值对复制到目标页中
  int mid=GetSize() / 2;
  for (int i = mid; i < GetSize(); i++) {
    page->page_id_array_[i - mid] = page_id_array_[i];
  }
  for (int i = mid+1; i < GetSize(); i++) {
    page->key_array_[i - mid] = key_array_[i];
  }
  page->SetSize(GetSize()-mid);//设置新页面大小
  SetSize(mid); // 更新当前页的大小
  return key_array_[mid]; // 返回中间键
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetIndex(page_id_t page_id) const -> int
{
  for (int i = 0; i < GetSize(); i++) {//寻找某个页面在当前节点的哪个指针
    if (page_id_array_[i] == page_id) {
      return i;
    }
  }
  return -1;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::L_Lend(BPlusTreeInternalPage *bro_page,BPlusTreeInternalPage *parent,int index) 
{
  for(int i=GetSize();i>0;i--)
  {
    key_array_[i] = key_array_[i-1];
    page_id_array_[i] = page_id_array_[i-1];
  }//为从左侧接收腾出空间
  
  KeyType borrowed_key = bro_page->KeyAt(bro_page->GetSize() - 1);
  ValueType borrowed_value = bro_page->page_id_array_[bro_page->GetSize() - 1];
  // 将左兄弟的最后一个键值对插入到当前节点的第一个位置
  key_array_[1] = parent->KeyAt(index); // 更新当前节点的第一个键
  page_id_array_[0] = borrowed_value;
  // 更新当前节点的大小
  SetSize(GetSize() + 1);
  // 删除左兄弟的最后一个键值对
  bro_page->SetSize(bro_page->GetSize() - 1);//更新兄弟节点大小
  parent->SetKeyAt(index, borrowed_key); // 更新父节点的键
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::R_Lend(BPlusTreeInternalPage *bro_page,BPlusTreeInternalPage *parent,int index) 
{
 
  
  KeyType borrowed_key = bro_page->KeyAt(1);
  ValueType borrowed_value = bro_page->page_id_array_[0];
  // 将右兄弟的第一个键值对插入到当前节点的最后一个位置
  key_array_[GetSize()] = parent->KeyAt(index+1); // 更新当前节点的第一个键
  page_id_array_[GetSize()] = borrowed_value;
  // 更新当前节点的大小
  SetSize(GetSize() + 1);
  // 删除左兄弟的最后一个键值对
  for (int i = 0; i < bro_page->GetSize() - 1; i++) {
    bro_page->key_array_[i] = bro_page->key_array_[i + 1];
    bro_page->page_id_array_[i] = bro_page->page_id_array_[i + 1];
  }//调整右节点元素
  bro_page->SetSize(bro_page->GetSize() - 1);
  parent->SetKeyAt(index+1, borrowed_key); // 更新父节点的键
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(BPlusTreeInternalPage *sibling, const KeyType &parent_key) {
  
  // 将父节点的分隔键插入到当前节点的末尾
  key_array_[GetSize()] = parent_key;

  // 将兄弟节点的键值对追加到当前节点
  for (int i = 0; i < sibling->GetSize()-1; i++) {
    key_array_[GetSize() + 1 + i] = sibling->KeyAt(i + 1);  // 跳过兄弟节点的无效键 key_array_[0]
    page_id_array_[GetSize()  + i] = sibling->ValueAt(i);
  }
  page_id_array_[GetSize() + sibling->GetSize()-1] = sibling->ValueAt(sibling->GetSize()-1);

  // 更新当前节点的大小
  SetSize(GetSize() + sibling->GetSize() );
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertSafe()const -> bool
{
  // 检查当前节点是否超过最大大小
  if (GetSize() < GetMaxSize()) {
    return true; // 插入不安全，返回 false
  }
  return false; // 插入安全，返回 true
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteSafe(bool t)const -> bool//t记录是否为根节点
{if(t) return GetSize() > 2;//如果是根节点，那么size>2是安全的
  else
  {if (GetSize() > GetMinSize()) {//不是根节点，要大于最小size
    return true; //安全
  }
  return false; // 不安全
  }
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
