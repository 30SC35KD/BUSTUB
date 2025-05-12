//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) :  k_(k) {replacer_size_=num_frames;node_store_.reserve(num_frames);}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return true if a frame is evicted successfully, false if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> 
{ 
    std::lock_guard<std::mutex> lock(latch_);
    size_t inf=std::numeric_limits<size_t>::max();
    frame_id_t base=0;
    bool done=0;
    size_t dis=0;
    size_t max_dis=0;
    for (auto itr = node_store_.begin(); itr != node_store_.end(); ++itr) {
        frame_id_t key=itr->first ;
        auto &value=itr->second ;
        //不可驱逐的帧不进行操作
        if(value.is_evictable()==false) continue;
        //不足k个记录的帧设置为最长距离
       if(value.history().size()<k_) dis=inf;
       else dis=current_timestamp_-value.history().front();
       value.get_dis(dis);
       if(done) 
       {//寻找最远距离
        if(dis>max_dis) {base=key;max_dis=dis;}
        else if(dis==max_dis) 
        {//距离相同找最早开始时间
        if(value.history().front()<node_store_[base].history().front()) base=key;
        }
       }
       else {base=key;max_dis=dis;done=1;}
    }
    
    if(done) 
    {node_store_.erase(base); curr_size_--;return base;}
    else {
    return std::nullopt;} 

}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
 //这里发现用history_比history()函数快很多，故将history_设为public
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]]AccessType access_type) 
{std::lock_guard<std::mutex> lock(latch_);
    
    current_timestamp_++;
    auto itr = node_store_.find(frame_id);
    //出现过
    if(itr==node_store_.end())
    {
        auto node=LRUKNode(frame_id);
        node.record(current_timestamp_);
        //永远保持在<=k的大小，提高效率
        if(node.history_.size()>k_) node.history_.pop_front();   
        node_store_.emplace(frame_id, node);
    }
    //第一次出现
    else
    {
        auto &node=itr->second;
        node.record(current_timestamp_);   
        if(node.history_.size()>k_) {
            //node.get_k();
            node.history_.pop_front();}
    }
    
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) 
{std::lock_guard<std::mutex> lock(latch_);
   
    auto itr = node_store_.find(frame_id);
    //没有出现
    if(itr == node_store_.end())return;
    bool e=itr->second.is_evictable();
    itr->second.set_evic(set_evictable);
    //不可驱逐->可驱逐
    if(!e&&set_evictable) curr_size_++;
     //可驱逐->不可驱逐
    else if(e&&!set_evictable) curr_size_--;

}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) 
{std::lock_guard<std::mutex> lock(latch_);
    auto itr = node_store_.find(frame_id);
    if(itr == node_store_.end())return;
    auto &node = itr->second;
    if(node.is_evictable()==true)
    {

        node_store_.erase(frame_id);
        curr_size_--;
    }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t {std::lock_guard<std::mutex> lock(latch_); return curr_size_; }

}  // namespace bustub
