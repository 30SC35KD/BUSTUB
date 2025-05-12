
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// skiplist.cpp
//
// Identification: src/primer/skiplist.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/skiplist.h"
#include <cassert>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include "common/macros.h"
#include "fmt/core.h"
#include <mutex>  
		namespace bustub {

			/** @brief Checks whether the container is empty. */
			SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Empty() -> bool {
				//UNIMPLEMENTED("TODO(P0): Add implementation.");
				std::shared_lock lk(rwlock_);
				if (Header()->Next(0) == nullptr) return true;
				else return false;
				//  if(size_==0) return true;
				//  return false;
			}

			/** @brief Returns the number of elements in the skip list. */
			SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Size() -> size_t {
				//UNIMPLEMENTED("TODO(P0): Add implementation.");
				size_t i = 0;
				std::shared_ptr<SkipNode> p = Header();
				while (p->Next(0) != nullptr)
				{
					i++;
					p = p->Next(0);
				}
				return i;
				//std::shared_lock lk(rwlock_);
				//return size_;
			}

			/**
			 * @brief Iteratively deallocate all the nodes.
			 *
			 * We do this to avoid stack overflow when the skip list is large.
			 *
			 * If we let the compiler handle the deallocation, it will recursively call the destructor of each node,
			 * which could block up the the stack.
			 */
			SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::Drop() {
				for (size_t i = 0; i < MaxHeight; i++) {
					auto curr = std::move(header_->links_[i]);
					while (curr != nullptr) {
						// std::move sets `curr` to the old value of `curr->links_[i]`,
						// and then resets `curr->links_[i]` to `nullptr`.
						curr = std::move(curr->links_[i]);
					}
				}
			}

			/**
			 * @brief Removes all elements from the skip list.
			 *
			 * Note: You might want to use the provided `Drop` helper function.
			 */
			SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::Clear() {
				//UNIMPLEMENTED("TODO(P0): Add implementation.");
				//std::unique_lock lk(m);
				std::shared_lock lk(rwlock_);
				Drop();
				for (size_t i = 0; i < MaxHeight; ++i) {
					header_->links_[i] = nullptr; // 直接操作头节点的 links_ 向量
					size_ = 0;
				}


			}



			/**
			 * @brief Inserts a key into the skip list.
			 *
			 * Note: `Insert` will not insert the key if it already exists in the skip list.
			 *
			 * @param key key to insert.
			 * @return true if the insertion is successful, false if the key already exists.
			 */
       
			 SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Insert(const K& key) -> bool {
				//UNIMPLEMENTED("TODO(P0): Add implementation.");
				std::unique_lock lk(rwlock_);
				auto newnode = std::make_shared<SkipNode>(RandomHeight(), key);
				size_t height = newnode->Height();
				auto curr = Header();
				std::vector<std::shared_ptr<SkipNode>> update(MaxHeight,nullptr);

				for (size_t i = MaxHeight ;i > 0; i--) //Max才能充分利用跳表特性
				{
					while (curr->Next(i - 1) != nullptr && compare_(curr->Next(i - 1)->Key(), key)) curr = curr->Next(i - 1);
					update[i - 1] = curr;
				}
				if (curr->Next(0) != nullptr && !compare_(curr->Next(0)->Key(), key) && !compare_(key, curr->Next(0)->Key()))return false;
				for (size_t i = 0; i < newnode->Height(); i++) {newnode->SetNext(i,update[i]->Next(i));update[i]->SetNext(i, newnode);}
				size_++;
				if (height > height_) height_ = height;
				return true;
        }

			// /**
			//  * @brief Erases the key from the skip list.
			//  *
			//  * @param key key to erase.
			//  * @return bool true if the element got erased, false otherwise.
			//  */
			SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Erase(const K& key) -> bool {
				//UNIMPLEMENTED("TODO(P0): Add implementation.");
				std::unique_lock lk(rwlock_);
				bool t = false;
				auto curr = Header();
				size_t up = 0;
				std::vector<std::shared_ptr<SkipNode>> update(MaxHeight);
				for (size_t i = MaxHeight; i > 0; i--)
				{

					while (curr->Next(i - 1) != nullptr)
					{
						if (!compare_(curr->Next(i - 1)->Key(), key) && !compare_(key, curr->Next(i - 1)->Key()))
						{
							//  auto n=curr->Next(i-1);
							//   curr->Next(i-1)=n->Next(i-1);
							update[i - 1] = curr;
							up++;
							t = true;
							break;
						}
						else if (compare_(key, curr->Next(i - 1)->Key())) break;
						else curr = curr->Next(i - 1);
					}
				}
				for (size_t i = 0; i < up; i++) update[i]->links_[i] = update[i]->Next(i)->Next(i);
				if (t == true)
				{
					size_--;
				}
				return t;
			}


			// /**
			//  * @brief Checks whether a key exists in the skip list.
			//  *
			//  * @param key key to look up.
			//  * @return bool true if the element exists, false otherwise.
			//  */
			SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Contains(const K& key) -> bool {
				// Following the standard library: Key `a` and `b` are considered equivalent if neither compares less
				// than the other: `!compare_(a, b) && !compare_(b, a)`.
				//UNIMPLEMENTED("TODO(P0): Add implementation.");
				std::shared_lock lk(rwlock_);
        auto curr = Header();
				for (size_t i = MaxHeight; i > 0; i--)
				{
					
					while (curr->Next(i - 1) != nullptr&&compare_(curr->Next(i-1)->Key(),key))
					{
						curr = curr->Next(i - 1);
						//if (compare_(key, curr->Key())) break;
						
					}
				}
        curr=curr->Next(0);
        if (curr!=nullptr&&!compare_(curr->Key(), key) && !compare_(key, curr->Key())) return true;
				else return false;
			}

     
      
    

			/**
			 * @brief Prints the skip list for debugging purposes.
			 *
			 * Note: You may modify the functions in any way and the output is not tested.
			 */
			SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::Print() {
				auto node = header_->Next(LOWEST_LEVEL);
				while (node != nullptr) {
					fmt::println("Node {{ key: {}, height: {} }}", node->Key(), node->Height());
					node = node->Next(LOWEST_LEVEL);
				}
			}

			/**
			 * @brief Generate a random height. The height should be cappped at `MaxHeight`.
			 * Note: we implement/simulate the geometric process to ensure platform independence.
			 */
			SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::RandomHeight() -> size_t {
				// Branching factor (1 in 4 chance), see Pugh's paper.
				static constexpr unsigned int branching_factor = 4;
				// Start with the minimum height
				size_t height = 1;
				while (height < MaxHeight && (rng_() % branching_factor == 0)) {
					height++;
				}
				return height;
			}

			/**
			 * @brief Gets the current node height.
			 */
			SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::SkipNode::Height() const -> size_t {
				//UNIMPLEMENTED("TODO(P0): Add implementation.");
			
				return h_;
			}

			/**
			 * @brief Gets the next node by following the link at `level`.
			 *
			 * @param level index to the link.
			 * @return std::shared_ptr<SkipNode> the next node, or `nullptr` if such node does not exist.
			 */
			SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::SkipNode::Next(size_t level) const
				-> std::shared_ptr<SkipNode> {
				//UNIMPLEMENTED("TODO(P0): Add implementation.");

				if (links_[level] != nullptr) return links_[level];
				else return nullptr;
			}

			/**
			 * @brief Set the `node` to be linked at `level`.
			 *
			 * @param level index to the link.
			 */
			SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::SkipNode::SetNext(
				size_t level, const std::shared_ptr<SkipNode>& node) {
				//UNIMPLEMENTED("TODO(P0): Add implementation.");

				//node->links_[level] = this->links_[level];
				this->links_[level] = node;
			}

			/** @brief Returns a reference to the key stored in the node. */
			SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::SkipNode::Key() const -> const K& {
				//UNIMPLEMENTED("TODO(P0): Add implementation.");

				return this->key_;
			}

			// Below are explicit instantiation of template classes.
			template class SkipList<int>;
			template class SkipList<std::string>;
			template class SkipList<int, std::greater<>>;
			template class SkipList<int, std::less<>, 8>;

		}  // namespace bustub
