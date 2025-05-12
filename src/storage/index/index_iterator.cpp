//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */
//INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { //UNIMPLEMENTED("TODO(P2): Add implementation."); 
if(leaf_==nullptr) {
    return true; // 如果当前叶子页面为空，返回 true
  } else if(index_<leaf_->GetSize()) {
    return false; // 如果当前索引小于叶子页面的大小，返回 false
  } else {
    return true; // 否则返回 true
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  //UNIMPLEMENTED("TODO(P2): Add implementation.");
  ValueType value;
  leaf_->Find(leaf_->KeyAt(index_), &value, comparator_); // 查找当前索引位置的键值对
  return {leaf_->KeyAt(index_), value}; // 返回当前索引位置的键值对
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & { //UNIMPLEMENTED("TODO(P2): Add implementation."); 
index_++;
if(index_>=leaf_->GetSize()) {
    // 如果当前索引大于等于叶子页面的大小，说明已经到达叶子页面的末尾
    auto next_page_id = leaf_->GetNextPageId(); // 获取下一个页面 ID
    if(next_page_id!=INVALID_PAGE_ID) {
      ReadPageGuard next_page_guard = bpm_->ReadPage(next_page_id); // 读取下一个页面
      leaf_ = next_page_guard.As<LeafPage>(); // 更新当前叶子页面
      index_ = 0; // 重置索引为 0
    } else {
      return *this; // 与终止End相比
    }
  }
  return *this; // 返回当前迭代器
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
