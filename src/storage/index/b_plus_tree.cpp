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
  //查询函数
  Context ctx;
  if(IsEmpty()) {
    return false;
  }
  auto r_id = GetRootPageId();//获得根节点
  ReadPageGuard r_guard = bpm_->ReadPage(r_id);//获取读锁
  
  auto curr = r_guard.As<BPlusTreePage>();//获得读页面
  auto c_guard = std::move(r_guard);
  while (!curr->IsLeafPage()) {
    auto i_page = c_guard.As<InternalPage>();//没到子叶节点，解释为内部节点
    auto c_id = i_page->Next(key, comparator_);//寻找下一个节点
  
    // 先获取子节点的锁
    ReadPageGuard next_guard = bpm_->ReadPage(c_id);
  
    // 确保成功获取子节点的锁后再释放父节点的锁
   c_guard.Drop();  // 释放父节点的读锁
  
    // 更新当前节点为子节点
    c_guard = std::move(next_guard);
    curr = c_guard.As<BPlusTreePage>();
  }
  auto l_page = c_guard.As<LeafPage>();//循环结束说明找到子叶节点，所以解释为叶节点

  ValueType value;
  if(l_page->Find(key,&value,comparator_)) {//在叶节点中真有对应元素，则保存结果
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
    // 如果树为空，创建新的头节点
    ctx.header_page_ = bpm_->WritePage(header_page_id_);
    auto new_r_id = bpm_->NewPage();//创建根节点
    auto new_r_guard= bpm_->WritePage(new_r_id);//给写锁
    auto new_r_page = new_r_guard.AsMut<LeafPage>();//只有根节点，解释为子叶节点
    new_r_page->Init(leaf_max_size_);//初始化
    new_r_page->SetPageID(new_r_id); 
    new_r_page->Insert(key, value, comparator_);//执行插入（向叶节点插入）
    auto h_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    h_page->root_page_id_ = new_r_id;//头页面获取根页面id
    new_r_guard.Drop(); // 释放当前页面的写锁
    return true;
  }
  ctx.header_page_ =bpm_->WritePage(header_page_id_);//树已存在，获取头节点
  ctx.root_page_id_=ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_;//从头结点获取根节点，都记录在ctx中
  auto r_id = ctx.root_page_id_;
  WritePageGuard r_guard = bpm_->WritePage(r_id);
  auto curr = r_guard.AsMut<BPlusTreePage>();//获取当前（根）页面，相当于指针，便于遍历寻找
  WritePageGuard c_guard=std::move(r_guard);
  while(true)
  {
    if (curr->IsLeafPage()) break;//找到叶节点才能停
    auto i_page = reinterpret_cast<InternalPage *>(curr);//否则解释为中间节点
    if(i_page->InsertSafe()) {for(auto &guard : ctx.write_set_) guard.Drop(); //如果节点是插入安全的，那么之前保存的锁都不需要了
      // Release the read lock on the current page
      ctx.write_set_.clear();}
    ctx.write_set_.push_back(std::move(c_guard));//把当前节点锁保存
    auto c_id = i_page->Next(key, comparator_);//向下搜索
    c_guard = bpm_->WritePage(c_id);
   
    curr = c_guard.template AsMut<BPlusTreePage>();
    
  }
  auto l_page = reinterpret_cast<LeafPage *>(curr);//找到了子叶节点
  if(l_page->InsertSafe()) {for(auto &guard : ctx.write_set_) guard.Drop(); 
    // Release the read lock on the current page
  ctx.write_set_.clear();}//如果是插入安全的，那么释放保存的所有锁
  ctx.write_set_.push_back(std::move(c_guard));
 
  if(l_page->Find(key,nullptr, comparator_)) {//要插入的元素已经存在，那么false
    return false;
  }
  else
  l_page->Insert(key, value, comparator_);//元素不存在，可以执行叶节点插入
  if (l_page->GetSize() > leaf_max_size_) //如果叶节点大小超过最大值，则需要分裂
    Split(l_page, &ctx);//递归分裂......
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Split(BPlusTreePage *page, Context *ctx)//用于分裂插入后超标的节点
{
  page_id_t new_page_id = bpm_->NewPage();//需要新节点保存另一半
  auto new_page_guard = bpm_->WritePage(new_page_id);
  auto new_page = new_page_guard.AsMut<BPlusTreePage>();
  
  if(page->IsLeafPage())//如果是叶节点
  {
    auto l_page =reinterpret_cast<LeafPage *>(page);
    auto new_l_page = reinterpret_cast<LeafPage *>(new_page);
    new_l_page->Init(leaf_max_size_);
    new_l_page->SetPageID(new_page_id);//把新节点解释为叶节点并且初始化
    l_page->Give(new_l_page);//原节点分配一半给新节点
    new_l_page->SetNextPageId(l_page->GetNextPageId());//设置叶结点指针指向原来节点的下一个节点
    l_page->SetNextPageId(new_page_id);//原叶节点指针指向新叶节点
    auto mid=new_l_page->KeyAt(0);//要提升的那个元素
    ctx->write_set_.push_back(std::move(new_page_guard));//保存新节点的锁
    Up(l_page, new_l_page,mid, ctx);//向上递归
  }
  else
  {
   auto i_page = reinterpret_cast<InternalPage *>(page);
    auto new_i_page = reinterpret_cast<InternalPage *>(new_page);
    new_i_page->Init(internal_max_size_);
    new_i_page->SetPageID(new_page_id);//把新节点解释为中间节点并且初始化
    auto mid=i_page->Give(new_i_page);//分出一半元素，并获得要提升的那个元素
    ctx->write_set_.push_back(std::move(new_page_guard));
    Up(i_page, new_i_page, mid, ctx);
  }
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Up(BPlusTreePage *page, BPlusTreePage * new_page, const KeyType &mid, Context *ctx)
{
  
 if(ctx->IsRootPage(page->GetPageID()))//终止条件：目前获取的要向上的页面是根页面
 {
  
   auto new_r_id = bpm_->NewPage();
   auto new_r_guard = bpm_->WritePage(new_r_id);//那么需要一个新的根页面
   auto new_h_page=ctx->header_page_->AsMut<BPlusTreeHeaderPage>();
   
   auto new_r_page = new_r_guard.AsMut<InternalPage>();//根页面解释为中间节点
   new_r_page->Init(internal_max_size_);
   new_h_page->root_page_id_=new_r_id;
   ctx->root_page_id_ = new_r_id;
    new_r_page->SetPageID(new_r_id); 
    new_r_page->page_id_array_[0] = page->GetPageID(); // 设置第一个子页面 ID
   new_r_page->Insert(mid, new_page->GetPageID(), comparator_);  //在新的根页面插入被提升上来的元素以及子页面（使用中间结点的insert）
   for(auto &guard : ctx->write_set_) guard.Drop();
  ctx->write_set_.clear(); //释放锁
   return;//终止
 }
if(ctx->write_set_.size() == 2) //因为新的兄弟节点也被加入，所以最终剩下的可能就是自己和兄弟
{
  for(auto &guard : ctx->write_set_) guard.Drop();
  ctx->write_set_.clear(); 
   return;//这种情况也是终止情况
}
 auto p_guard = std::move(ctx->write_set_[ctx->write_set_.size() - 3]);//-1是兄弟，-2是自己，-3才是保存的父节点
 auto p_page = p_guard.AsMut<InternalPage>();
 p_page->Insert(mid, new_page->GetPageID(), comparator_);//调用中间节点的insert，更新父节点
 ctx->write_set_[ctx->write_set_.size() - 1].Drop(); // 释放兄弟页面的写锁
  ctx->write_set_.pop_back();
  ctx->write_set_[ctx->write_set_.size() - 1].Drop(); // 释放当前页面的写锁
  ctx->write_set_.pop_back();
 if(p_page->GetSize() > internal_max_size_) Split(p_page, ctx);//如果向上插入之后节点超标，继续向上分裂
 
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::OptimisticInsert(const KeyType &key, const ValueType &value) -> bool //本想实现乐观锁，但这一部分好像有些问题，最后没有使用；并发测试没通过应该也不是锁的问题（虽然至今也不知道问题是什么）
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
  auto curr = r_guard.AsMut<BPlusTreePage>();//获取头节点和根节点
  
  WritePageGuard c_guard=std::move(r_guard);
  while(true)
  {
    if (curr->IsLeafPage()) break;
    auto i_page = reinterpret_cast<InternalPage *>(curr);//只有找到叶节点才会结束寻找
    if(i_page->GetPageID()==ctx.root_page_id_) t=true;//叶节点就是根节点，记录下来
  else t=false;
    if(i_page->DeleteSafe(t)) {for(auto &guard : ctx.write_set_) guard.Drop(); //如果当前节点是删除安全的，那么释放之前的锁
      // Release the read lock on the current page
      ctx.write_set_.clear();}
    ctx.write_set_.push_back(std::move(c_guard));//记录当前的页面
    auto c_id = i_page->Next(key, comparator_);//向下寻找
    c_guard = bpm_->WritePage(c_id);
    curr = c_guard.template AsMut<BPlusTreePage>();
  }
  auto l_page = reinterpret_cast<LeafPage *>(curr);//找到子叶节点
  if(l_page->GetPageID()==ctx.root_page_id_) t=true;//判断是不是根节点
  else t=false;
  if(l_page->DeleteSafe(t)) {for(auto &guard : ctx.write_set_) guard.Drop(); 
    // Release the read lock on the current page
    ctx.write_set_.clear();}//如果子叶节点是删除安全的，那么释放所有锁
  ctx.write_set_.push_back(std::move(c_guard));
  if(!l_page->Find(key,nullptr, comparator_)) return;//如果没找到要删除的元素则直接返回
  else
  {

    l_page->Remove(key,comparator_);//调用叶结点的删除函数
    if(ctx.IsRootPage(l_page->GetPageID())) {//如果操作的是根节点，进行特判
      if(l_page->GetSize()==0) {
      auto h_page=ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
      h_page->root_page_id_=INVALID_PAGE_ID;
      }//这是整棵树删完了的情况
      return;}
    if(ctx.write_set_.size() == 1)//不是根节点，只剩自己一个被保存，说明不会有与父节点的交互了，没有后续操作，这时也会结束 
    {
      for(auto &guard : ctx.write_set_) guard.Drop();
      ctx.write_set_.clear(); 
      return;
    }
    auto p_guard = std::move(ctx.write_set_[ctx.write_set_.size() - 2]);//-1是自己，-2是父节点
    auto p_page = p_guard.AsMut<InternalPage>();
    auto index=p_page->GetIndex(l_page->GetPageID());//获取叶节点的位置
    p_page->SetKeyAt(index, l_page->KeyAt(0));//对于叶节点的父节点来说，叶节点指针的那个位置的元素与叶节点的第一个元素相同
    if(l_page->GetSize()<l_page->GetMinSize())//如果size不够，那么需要adjust
      Adjust(l_page,p_page,index,&ctx);
  }

}

INDEX_TEMPLATE_ARGUMENTS
 auto BPLUSTREE_TYPE::Adjust(BPlusTreePage *page,InternalPage *p_page,int index, Context *ctx) -> void {
  while(true)
  {
  if(ctx->IsRootPage(page->GetPageID())) {//如果调整到了根节点，就到了终点
    if(page->IsLeafPage()&&page->GetSize()==0) //只剩根节点并且元素删没了
    {
      auto h_page = ctx->header_page_->AsMut<BPlusTreeHeaderPage>();
      h_page->root_page_id_ = INVALID_PAGE_ID;//根节点不存在了
      return;
    }
    else if(!page->IsLeafPage()&&page->GetSize()==1)//根节点还有，但实际上只剩一个指针了
    {
      auto i_page = reinterpret_cast<InternalPage *>(page);
      auto h_page = ctx->header_page_->AsMut<BPlusTreeHeaderPage>();
      h_page->root_page_id_ = i_page->ValueAt(0);//那么把根节点更新为指针所指的页面
    }
    return;
  }
  if(ctx->write_set_.size() == 1) //保存的页面只剩自己，说明没有操作了
  {
    for(auto &guard : ctx->write_set_) guard.Drop();
    ctx->write_set_.clear(); 
    return;
  }
 if(Brother(page,p_page,index,ctx)) return;//如果兄弟能借，那借完之后就结束了
 Parent(page, p_page, index, ctx);//如果兄弟不能借，只能向上求助
 // 如果父节点大小小于最小大小，递归向祖父节点请求帮助
 if (p_page->GetSize() < p_page->GetMinSize()) {

  if(ctx->write_set_.size() < 2) { page=p_page;continue;} 
  //Adjust(p_page,nullptr,0,ctx); // 如果没有祖父节点，直接返回
   auto grand_guard = std::move(ctx->write_set_[ctx->write_set_.size() - 2]);
   auto grand_page = grand_guard.AsMut<InternalPage>();
   auto grand_index = grand_page->GetIndex(p_page->GetPageID());
   //Adjust(p_page, grand_page, grand_index, ctx);  // 递归调用 Adjust
   page=p_page;
   p_page=grand_page;//更新page为父节点，p_page为祖父节点
   index=grand_index;//指针位置也进行更新
}
else return;
}
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Brother(BPlusTreePage *page, InternalPage *p_page,int index,Context *ctx) -> bool
{
  
  
  if(index>0)//有左兄弟，先向左借
  {
    auto bro_id = p_page->ValueAt(index - 1);
    WritePageGuard bro_guard = bpm_->WritePage(bro_id);
    auto bro_page = bro_guard.AsMut<BPlusTreePage>();//找到左兄弟
    if(bro_page->GetSize() > bro_page->GetMinSize())//左兄弟可以借
    {
      if(page->IsLeafPage()) //是叶节点则调用叶结点的lend
      {auto l_page=reinterpret_cast<LeafPage *>(page);
        auto b_page=reinterpret_cast<LeafPage *>(bro_page);
        l_page->L_Lend(b_page, p_page,index);
      }
      else {//不是叶节点则调用中间节点的lend
        auto i_page=reinterpret_cast<InternalPage *>(page);
        auto b_page=reinterpret_cast<InternalPage *>(bro_page);
        i_page->L_Lend(b_page, p_page,index);//真从右兄弟借着了
      }
      
      return true;
    }
  }
  if(index<p_page->GetSize()-1)//左兄弟借不了，这时有右兄弟，找右兄弟
  {
    auto bro_id = p_page->ValueAt(index + 1);
    WritePageGuard bro_guard = bpm_->WritePage(bro_id);
    auto bro_page = bro_guard.AsMut<BPlusTreePage>();
    if(bro_page->GetSize() > bro_page->GetMinSize())//右兄弟能借
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
      return true;//真从右兄弟借着了
    }
  }
  return false;//左右兄弟都没借着
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Parent(BPlusTreePage *page, InternalPage *p_page, int index, Context *ctx) {
  // 检查是否有左兄弟
  if (index > 0) {
    auto left_id = p_page->ValueAt(index - 1);
    WritePageGuard left_guard = bpm_->WritePage(left_id);
    auto left_page = left_guard.AsMut<BPlusTreePage>();//找到左兄弟

    // 合并当前节点到左兄弟节点
    if (page->IsLeafPage()) {
      auto l_page = reinterpret_cast<LeafPage *>(page);
      auto left_l_page = reinterpret_cast<LeafPage *>(left_page);
      left_l_page->Merge(l_page);//调用子叶合并
    } else {
      auto i_page = reinterpret_cast<InternalPage *>(page);
      auto left_i_page = reinterpret_cast<InternalPage *>(left_page);
      left_i_page->Merge(i_page, p_page->KeyAt(index));//调用中间节点合并
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
  if(IsEmpty()) return INDEXITERATOR_TYPE(nullptr, 0, bpm_,comparator_);//返回空
  auto h_id = GetRootPageId();
  ReadPageGuard h_guard = bpm_->ReadPage(h_id);
  auto curr = h_guard.As<BPlusTreePage>();
  auto c_guard = std::move(h_guard);//从根节点开始（这里用h_id不太准确）
  while(!curr->IsLeafPage()) {
    auto i_page = c_guard.As<InternalPage>();
    auto c_id = i_page->ValueAt(0);//寻找最左面的
    c_guard = bpm_->ReadPage(c_id);
    curr = c_guard.As<BPlusTreePage>();
  }
  auto l_page = c_guard.As<LeafPage>();//找到了最左面的叶节点
  return INDEXITERATOR_TYPE(l_page, 0, bpm_,comparator_);//返回迭代器
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
    auto c_id = i_page->Next(key, comparator_);//寻找含键为key的叶节点
    c_guard = bpm_->ReadPage(c_id);
    curr = c_guard.As<BPlusTreePage>();
  }
  auto l_page = c_guard.As<LeafPage>();
  ValueType value;
  if(l_page->Find(key,&value, comparator_)) {
    return INDEXITERATOR_TYPE(l_page,value.GetSlotNum(), bpm_,comparator_);//找到了键key所在的叶节点及元素所在位置
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
  auto c_guard = std::move(h_guard);//同样从根节点开始
  while(!curr->IsLeafPage()) {
    auto i_page = c_guard.As<InternalPage>();
    auto c_id = i_page->ValueAt(i_page->GetSize() - 1);//寻找最右侧的叶节点
    c_guard = bpm_->ReadPage(c_id);
    curr = c_guard.As<BPlusTreePage>();
  }
  auto l_page = c_guard.As<LeafPage>();
  return INDEXITERATOR_TYPE(l_page, l_page->GetSize(), bpm_,comparator_);//最右侧叶结点的最后一个元素
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
