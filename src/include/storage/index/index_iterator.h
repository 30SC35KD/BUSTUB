//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.h
//
// Identification: src/include/storage/index/index_iterator.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <utility>
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(const LeafPage *leaf, int index, BufferPoolManager *bpm,
                const KeyComparator &comparator)  // NOLINT
      : leaf_(leaf), index_(index), bpm_(bpm),comparator_(comparator) {}  // NOLINT
  
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> std::pair<const KeyType &, const ValueType &>;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool { //UNIMPLEMENTED("TODO(P2): Add implementation."); 
  if(leaf_==itr.leaf_ && index_==itr.index_) {
    return true; // 如果当前迭代器和传入的迭代器指向同一位置，返回 true
  } else {
    return false; // 否则返回 false
  }
  }
  auto operator!=(const IndexIterator &itr) const -> bool { //UNIMPLEMENTED("TODO(P2): Add implementation.");
    
  if(leaf_!=itr.leaf_ || index_!=itr.index_) {
    return true; // 如果当前迭代器和传入的迭代器指向不同位置，返回 true
  } else {
    return false; // 否则返回 false
  }
  }

 //private:
  // add your own private member variables here
  const LeafPage *leaf_;  // 当前叶子页面
  int index_;         // 当前索引位置
  BufferPoolManager *bpm_;  // 缓冲池管理器
  KeyComparator comparator_;
};

}  // namespace bustub
