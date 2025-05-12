//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID; // Initialize the root page ID to INVALID_PAGE_ID
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { //UNIMPLEMENTED("TODO(P2): Add implementation."); 
  // 获取根页面 ID
  auto root_page_id = GetRootPageId();
  // 如果根页面 ID 无效，说明树为空
  if (root_page_id == INVALID_PAGE_ID) {
      return true;
  }

  // 根据根页面 ID 获取根页面
  ReadPageGuard root_guard = bpm_->ReadPage(root_page_id);
  auto root_page = root_guard.As<BPlusTreePage>();
  // 判断根页面是否为叶子页面
  if (root_page->IsLeafPage()) 
  {
      auto l_page = root_guard.As<LeafPage>();
      // 如果是叶子页面，检查其大小是否为 0
    if(l_page->GetSize() == 0) 
          // 如果叶子页面的大小为 0，说明树为空
          return true;
    else 
          // 如果叶子页面的大小不为 0，说明树不为空
          return false;
  } 
  else {
      // 如果根页面不是叶子页面（是内部页面），由于内部页面不存储实际数据，只要存在根页面就说明树不为空
      return false;
  }
  }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with input key, if the value exists
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  //UNIMPLEMENTED("TODO(P2): Add implementation.");
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;
  if(IsEmpty()) {
    return false;
  }
  auto r_id = GetRootPageId();
  ReadPageGuard r_guard = bpm_->ReadPage(r_id);
  
  auto curr = r_guard.As<BPlusTreePage>();
  auto c_guard = std::move(r_guard);
  while (!curr->IsLeafPage()) {
    auto i_page = c_guard.As<InternalPage>();
    auto c_id = i_page->Next(key, comparator_);
  
    // 先获取子节点的锁
    ReadPageGuard next_guard = bpm_->ReadPage(c_id);
  
    // 确保成功获取子节点的锁后再释放父节点的锁
   c_guard.Drop();  // 释放父节点的读锁
  
    // 更新当前节点为子节点
    c_guard = std::move(next_guard);
    curr = c_guard.As<BPlusTreePage>();
  }
  auto l_page = c_guard.As<LeafPage>();

  ValueType value;
  if(l_page->Find(key,&value,comparator_)) {
    result->push_back(value);
    return true;
  } else {
    return false;
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 *
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 *
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  //UNIMPLEMENTED("TODO(P2): Add implementation.");
  // Declaration of context instance. Using the Context is not necessary but advised.
  // bool op=OptimisticInsert(key, value);
  // if(op==true) return true;
  Context ctx;
  if(IsEmpty())
  {
    // 如果树为空，创建新的根节点
    ctx.header_page_ = bpm_->WritePage(header_page_id_);
    auto new_r_id = bpm_->NewPage();
    auto new_r_guard= bpm_->WritePage(new_r_id);
    auto new_r_page = new_r_guard.AsMut<LeafPage>();
    new_r_page->Init(leaf_max_size_);
    new_r_page->SetPageID(new_r_id); 
    new_r_page->Insert(key, value, comparator_);
    auto h_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    h_page->root_page_id_ = new_r_id;
    new_r_guard.Drop(); // 释放当前页面的写锁
    return true;
  }
  ctx.header_page_ =bpm_->WritePage(header_page_id_);
  ctx.root_page_id_=ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_;
  auto r_id = ctx.root_page_id_;
  WritePageGuard r_guard = bpm_->WritePage(r_id);
  auto curr = r_guard.AsMut<BPlusTreePage>();
  WritePageGuard c_guard=std::move(r_guard);
  while(true)
  {
    if (curr->IsLeafPage()) break;
    auto i_page = reinterpret_cast<InternalPage *>(curr);
    if(i_page->InsertSafe()) {for(auto &guard : ctx.write_set_) guard.Drop(); 
      // Release the read lock on the current page
      ctx.write_set_.clear();}
    ctx.write_set_.push_back(std::move(c_guard));
    auto c_id = i_page->Next(key, comparator_);
    c_guard = bpm_->WritePage(c_id);
   
    curr = c_guard.template AsMut<BPlusTreePage>();
    
  }
  auto l_page = reinterpret_cast<LeafPage *>(curr);//一定不安全
  if(l_page->InsertSafe()) {for(auto &guard : ctx.write_set_) guard.Drop(); 
    // Release the read lock on the current page
  ctx.write_set_.clear();}
  ctx.write_set_.push_back(std::move(c_guard));
 
  if(l_page->Find(key,nullptr, comparator_)) {
    return false;
  }
  else
  l_page->Insert(key, value, comparator_);
  if (l_page->GetSize() > leaf_max_size_) 
    Split(l_page, &ctx);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Split(BPlusTreePage *page, Context *ctx)
{
  page_id_t new_page_id = bpm_->NewPage();
  auto new_page_guard = bpm_->WritePage(new_page_id);
  auto new_page = new_page_guard.AsMut<BPlusTreePage>();
  
  if(page->IsLeafPage())
  {
    auto l_page =reinterpret_cast<LeafPage *>(page);
    auto new_l_page = reinterpret_cast<LeafPage *>(new_page);
    new_l_page->Init(leaf_max_size_);
    new_l_page->SetPageID(new_page_id);
    l_page->Give(new_l_page);
    new_l_page->SetNextPageId(l_page->GetNextPageId());
    l_page->SetNextPageId(new_page_id);
    auto mid=new_l_page->KeyAt(0);
    ctx->write_set_.push_back(std::move(new_page_guard));
    Up(l_page, new_l_page,mid, ctx);
  }
  else
  {
   auto i_page = reinterpret_cast<InternalPage *>(page);
    auto new_i_page = reinterpret_cast<InternalPage *>(new_page);
    new_i_page->Init(internal_max_size_);
    new_i_page->SetPageID(new_page_id);
    auto mid=i_page->Give(new_i_page);
    ctx->write_set_.push_back(std::move(new_page_guard));
    Up(i_page, new_i_page, mid, ctx);
  }
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Up(BPlusTreePage *page, BPlusTreePage * new_page, const KeyType &mid, Context *ctx)
{
  
 if(ctx->IsRootPage(page->GetPageID()))
 {
  
   auto new_r_id = bpm_->NewPage();
   auto new_r_guard = bpm_->WritePage(new_r_id);
   auto new_h_page=ctx->header_page_->AsMut<BPlusTreeHeaderPage>();
   
   auto new_r_page = new_r_guard.AsMut<InternalPage>();
   new_r_page->Init(internal_max_size_);
   new_h_page->root_page_id_=new_r_id;
   ctx->root_page_id_ = new_r_id;
    new_r_page->SetPageID(new_r_id); 
    new_r_page->page_id_array_[0] = page->GetPageID(); // 设置第一个子页面 ID
   new_r_page->Insert(mid, new_page->GetPageID(), comparator_);  
   for(auto &guard : ctx->write_set_) guard.Drop();
  ctx->write_set_.clear(); 
   return;
 }
if(ctx->write_set_.size() == 2) //兄弟节点也被加入
{
  for(auto &guard : ctx->write_set_) guard.Drop();
  ctx->write_set_.clear(); 
   return;
}
 auto p_guard = std::move(ctx->write_set_[ctx->write_set_.size() - 3]);
 auto p_page = p_guard.AsMut<InternalPage>();
 p_page->Insert(mid, new_page->GetPageID(), comparator_);
 ctx->write_set_[ctx->write_set_.size() - 1].Drop(); // 释放兄弟页面的写锁
  ctx->write_set_.pop_back();
  ctx->write_set_[ctx->write_set_.size() - 1].Drop(); // 释放当前页面的写锁
  ctx->write_set_.pop_back();
 if(p_page->GetSize() > internal_max_size_) Split(p_page, ctx);
 
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::OptimisticInsert(const KeyType &key, const ValueType &value) -> bool
{
  if(IsEmpty())
  {
    // 如果树为空，创建新的根节点
    auto h_guard = bpm_->WritePage(header_page_id_);
    auto new_r_id = bpm_->NewPage();
    auto new_r_guard= bpm_->WritePage(new_r_id);
    auto new_r_page = new_r_guard.AsMut<LeafPage>();
    new_r_page->Init(leaf_max_size_);
    new_r_page->SetPageID(new_r_id); 
    new_r_page->Insert(key, value, comparator_);
    auto h_page = h_guard.AsMut<BPlusTreeHeaderPage>();
    h_page->root_page_id_ = new_r_id;
    return true;
  }
  auto h_guard =bpm_->WritePage(header_page_id_);
  auto root_page_id=h_guard.AsMut<BPlusTreeHeaderPage>()->root_page_id_;
  auto r_id = root_page_id;
  ReadPageGuard r_guard = bpm_->ReadPage(r_id);
  auto curr = r_guard.As<BPlusTreePage>();
  auto c_guard = std::move(r_guard);
  while (!curr->IsLeafPage()) {
    auto i_page = c_guard.As<InternalPage>();
    auto c_id = i_page->Next(key, comparator_);
  
    // 先获取子节点的锁
    ReadPageGuard next_guard = bpm_->ReadPage(c_id);
  
    // 确保成功获取子节点的锁后再释放父节点的锁
    c_guard.Drop();  // 释放父节点的读锁
  
    // 更新当前节点为子节点
    c_guard = std::move(next_guard);
    curr = c_guard.As<BPlusTreePage>();
  }
  auto l_page = c_guard.As<LeafPage>();
 
  if(l_page->InsertSafe())
  {  c_guard.Drop(); // Release the read lock on the current page
    WritePageGuard w_guard = bpm_->WritePage(l_page->GetPageID());  // 获取写锁
    auto writable_l_page = w_guard.AsMut<LeafPage>();
    // 在写锁保护下执行插入操作
    writable_l_page->Insert(key, value, comparator_);
    w_guard.Drop(); // 释放写锁
    return true;
  }
  else{c_guard.Drop(); 
  return false;}
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  Context ctx;
  bool t=false;//是否是根节点
  //UNIMPLEMENTED("TODO(P2): Add implementation.");
  if(IsEmpty()) {
    return;
  }
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  ctx.root_page_id_ = ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_;
  auto r_id = ctx.root_page_id_;
  WritePageGuard r_guard = bpm_->WritePage(r_id);
  auto curr = r_guard.AsMut<BPlusTreePage>();
  
  WritePageGuard c_guard=std::move(r_guard);
  while(true)
  {
    if (curr->IsLeafPage()) break;
    auto i_page = reinterpret_cast<InternalPage *>(curr);
    if(i_page->GetPageID()==ctx.root_page_id_) t=true;
  else t=false;
    if(i_page->DeleteSafe(t)) {for(auto &guard : ctx.write_set_) guard.Drop(); 
      // Release the read lock on the current page
      ctx.write_set_.clear();}
    ctx.write_set_.push_back(std::move(c_guard));
    auto c_id = i_page->Next(key, comparator_);
    c_guard = bpm_->WritePage(c_id);
    curr = c_guard.template AsMut<BPlusTreePage>();
  }
  auto l_page = reinterpret_cast<LeafPage *>(curr);
  if(l_page->GetPageID()==ctx.root_page_id_) t=true;
  else t=false;
  if(l_page->DeleteSafe(t)) {for(auto &guard : ctx.write_set_) guard.Drop(); 
    // Release the read lock on the current page
    ctx.write_set_.clear();}
  ctx.write_set_.push_back(std::move(c_guard));
  if(!l_page->Find(key,nullptr, comparator_)) return;
  else
  {

    l_page->Remove(key,comparator_);
    if(ctx.IsRootPage(l_page->GetPageID())) {
      if(l_page->GetSize()==0) {
      auto h_page=ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
      h_page->root_page_id_=INVALID_PAGE_ID;
      }
      return;}
    if(ctx.write_set_.size() == 1) 
    {
      for(auto &guard : ctx.write_set_) guard.Drop();
      ctx.write_set_.clear(); 
      return;
    }
    auto p_guard = std::move(ctx.write_set_[ctx.write_set_.size() - 2]);
    auto p_page = p_guard.AsMut<InternalPage>();
    auto index=p_page->GetIndex(l_page->GetPageID());
    p_page->SetKeyAt(index, l_page->KeyAt(0));
    if(l_page->GetSize()<l_page->GetMinSize())
      Adjust(l_page,p_page,index,&ctx);
  }

}

INDEX_TEMPLATE_ARGUMENTS
 auto BPLUSTREE_TYPE::Adjust(BPlusTreePage *page,InternalPage *p_page,int index, Context *ctx) -> void {
  while(true)
  {
  if(ctx->IsRootPage(page->GetPageID())) {
    if(page->IsLeafPage()&&page->GetSize()==0) 
    {
      auto h_page = ctx->header_page_->AsMut<BPlusTreeHeaderPage>();
      h_page->root_page_id_ = INVALID_PAGE_ID;
      return;
    }
    else if(!page->IsLeafPage()&&page->GetSize()==1)
    {
      auto i_page = reinterpret_cast<InternalPage *>(page);
      auto h_page = ctx->header_page_->AsMut<BPlusTreeHeaderPage>();
      h_page->root_page_id_ = i_page->ValueAt(0);
    }
    return;
  }
  if(ctx->write_set_.size() == 1) 
  {
    for(auto &guard : ctx->write_set_) guard.Drop();
    ctx->write_set_.clear(); 
    return;
  }
 if(Brother(page,p_page,index,ctx)) return;
 Parent(page, p_page, index, ctx);
 // 如果父节点大小小于最小大小，递归向祖父节点请求帮助
 if (p_page->GetSize() < p_page->GetMinSize()) {

  if(ctx->write_set_.size() < 2) { page=p_page;continue;} 
  //Adjust(p_page,nullptr,0,ctx); // 如果没有祖父节点，直接返回
   auto grand_guard = std::move(ctx->write_set_[ctx->write_set_.size() - 2]);
   auto grand_page = grand_guard.AsMut<InternalPage>();
   auto grand_index = grand_page->GetIndex(p_page->GetPageID());
   //Adjust(p_page, grand_page, grand_index, ctx);  // 递归调用 Adjust
   page=p_page;
   p_page=grand_page;
   index=grand_index;
}
else return;
}
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Brother(BPlusTreePage *page, InternalPage *p_page,int index,Context *ctx) -> bool
{
  
  
  if(index>0)
  {
    auto bro_id = p_page->ValueAt(index - 1);
    WritePageGuard bro_guard = bpm_->WritePage(bro_id);
    auto bro_page = bro_guard.AsMut<BPlusTreePage>();
    if(bro_page->GetSize() > bro_page->GetMinSize())
    {
      if(page->IsLeafPage()) 
      {auto l_page=reinterpret_cast<LeafPage *>(page);
        auto b_page=reinterpret_cast<LeafPage *>(bro_page);
        l_page->L_Lend(b_page, p_page,index);
      }
      else {
        auto i_page=reinterpret_cast<InternalPage *>(page);
        auto b_page=reinterpret_cast<InternalPage *>(bro_page);
        i_page->L_Lend(b_page, p_page,index);
      }
      
      return true;
    }
  }
  if(index<p_page->GetSize()-1)
  {
    auto bro_id = p_page->ValueAt(index + 1);
    WritePageGuard bro_guard = bpm_->WritePage(bro_id);
    auto bro_page = bro_guard.AsMut<BPlusTreePage>();
    if(bro_page->GetSize() > bro_page->GetMinSize())
    {if(page->IsLeafPage()) 
      {auto l_page=reinterpret_cast<LeafPage *>(page);
        auto b_page=reinterpret_cast<LeafPage *>(bro_page);
        l_page->R_Lend(b_page, p_page,index);
      }
      else {
        auto i_page=reinterpret_cast<InternalPage *>(page);
        auto b_page=reinterpret_cast<InternalPage *>(bro_page);
        i_page->R_Lend(b_page, p_page,index);
      }
      return true;
    }
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Parent(BPlusTreePage *page, InternalPage *p_page, int index, Context *ctx) {
  // 检查是否有左兄弟
  if (index > 0) {
    auto left_id = p_page->ValueAt(index - 1);
    WritePageGuard left_guard = bpm_->WritePage(left_id);
    auto left_page = left_guard.AsMut<BPlusTreePage>();

    // 合并当前节点到左兄弟节点
    if (page->IsLeafPage()) {
      auto l_page = reinterpret_cast<LeafPage *>(page);
      auto left_l_page = reinterpret_cast<LeafPage *>(left_page);
      left_l_page->Merge(l_page);
    } else {
      auto i_page = reinterpret_cast<InternalPage *>(page);
      auto left_i_page = reinterpret_cast<InternalPage *>(left_page);
      left_i_page->Merge(i_page, p_page->KeyAt(index));
    }

    // 从父节点中移除分隔键和当前节点的指针
   // 删除中间节点中索引为 index 的键值对
    for (int i = index; i < p_page->GetSize() - 1; i++) {
      p_page->key_array_[i] = p_page->key_array_[i + 1];          // 将后续键向前移动
      p_page->page_id_array_[i] = p_page->page_id_array_[i + 1];  // 将后续指针向前移动
    }
    // 更新节点大小
    p_page->SetSize(p_page->GetSize() - 1);
  
    ctx->write_set_.pop_back();
    return;
  }

  // 检查是否有右兄弟
  if (index < p_page->GetSize() - 1) {
    auto right_id = p_page->ValueAt(index + 1);
    WritePageGuard right_guard = bpm_->WritePage(right_id);
    auto right_page = right_guard.AsMut<BPlusTreePage>();

    // 合并右兄弟节点到当前节点
    if (page->IsLeafPage()) {
      auto l_page = reinterpret_cast<LeafPage *>(page);
      auto right_l_page = reinterpret_cast<LeafPage *>(right_page);
      l_page->Merge(right_l_page);
      right_guard.Drop();
    } else {
      auto i_page = reinterpret_cast<InternalPage *>(page);
      auto right_i_page = reinterpret_cast<InternalPage *>(right_page);
      i_page->Merge(right_i_page, p_page->KeyAt(index + 1));
    }

    // 从父节点中移除分隔键和右兄弟节点的指针
    // 删除中间节点中索引为 index 的键值对
    for (int i = index+1; i < p_page->GetSize() - 1; i++) {
      p_page->key_array_[i] = p_page->key_array_[i + 1];          // 将后续键向前移动
      p_page->page_id_array_[i ] = p_page->page_id_array_[i + 1];  // 将后续指针向前移动
    }
    // 更新节点大小
    p_page->SetSize(p_page->GetSize() - 1);
    ctx->write_set_[ctx->write_set_.size() - 1].Drop(); // 释放当前页面的写锁
    ctx->write_set_.pop_back();
    return ;
  }
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
 
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { //UNIMPLEMENTED("TODO(P2): Add implementation."); 
  if(IsEmpty()) return INDEXITERATOR_TYPE(nullptr, 0, bpm_,comparator_);
  auto h_id = GetRootPageId();
  ReadPageGuard h_guard = bpm_->ReadPage(h_id);
  auto curr = h_guard.As<BPlusTreePage>();
  auto c_guard = std::move(h_guard);
  while(!curr->IsLeafPage()) {
    auto i_page = c_guard.As<InternalPage>();
    auto c_id = i_page->ValueAt(0);
    c_guard = bpm_->ReadPage(c_id);
    curr = c_guard.As<BPlusTreePage>();
  }
  auto l_page = c_guard.As<LeafPage>();
  return INDEXITERATOR_TYPE(l_page, 0, bpm_,comparator_);
}
/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { //UNIMPLEMENTED("TODO(P2): Add implementation."); 
 if(IsEmpty()) return INDEXITERATOR_TYPE(nullptr, 0, bpm_,comparator_);
 auto h_id = GetRootPageId();
 ReadPageGuard h_guard = bpm_->ReadPage(h_id);
  auto curr = h_guard.As<BPlusTreePage>();
  auto c_guard = std::move(h_guard);
  while(!curr->IsLeafPage()) {
    auto i_page = c_guard.As<InternalPage>();
    auto c_id = i_page->Next(key, comparator_);
    c_guard = bpm_->ReadPage(c_id);
    curr = c_guard.As<BPlusTreePage>();
  }
  auto l_page = c_guard.As<LeafPage>();
  ValueType value;
  if(l_page->Find(key,&value, comparator_)) {
    return INDEXITERATOR_TYPE(l_page,value.GetSlotNum(), bpm_,comparator_);
  }
  else return INDEXITERATOR_TYPE(nullptr, 0, bpm_,comparator_);
 
}

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { //UNIMPLEMENTED("TODO(P2): Add implementation."); 
  if(IsEmpty()) return INDEXITERATOR_TYPE(nullptr, 0, bpm_,comparator_);
  auto h_id = GetRootPageId();
  ReadPageGuard h_guard = bpm_->ReadPage(h_id);
  auto curr = h_guard.As<BPlusTreePage>();
  auto c_guard = std::move(h_guard);
  while(!curr->IsLeafPage()) {
    auto i_page = c_guard.As<InternalPage>();
    auto c_id = i_page->ValueAt(i_page->GetSize() - 1);
    c_guard = bpm_->ReadPage(c_id);
    curr = c_guard.As<BPlusTreePage>();
  }
  auto l_page = c_guard.As<LeafPage>();
  return INDEXITERATOR_TYPE(l_page, l_page->GetSize(), bpm_,comparator_);
}

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() const -> page_id_t { //UNIMPLEMENTED("TODO(P2): Add implementation."); 
    // // 尝试从缓冲池管理器中读取头部页面
    if (header_page_id_ == INVALID_PAGE_ID) {
      return INVALID_PAGE_ID;
    }
    ReadPageGuard header_page_guard = bpm_->ReadPage(header_page_id_);
   
    // 将读取到的页面转换为 BPlusTreeHeaderPage 类型
    auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();
    // 从头部页面中获取根页面 ID
    return header_page->root_page_id_;
   
}



template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
