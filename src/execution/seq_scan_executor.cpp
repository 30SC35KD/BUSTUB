//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

/**
 * Construct a new SeqScanExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sequential scan plan to be executed
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) ,plan_(plan)
{table_heap_ = nullptr;}

/** Initialize the sequential scan */
void SeqScanExecutor::Init() { //throw NotImplementedException("SeqScanExecutor is not implemented"); 
    auto table_oid = plan_->GetTableOid();
    auto catalog = exec_ctx_->GetCatalog();
    auto table_info = catalog->GetTable(table_oid).get();
    table_heap_ = table_info->table_.get();
    table_iter_ = std::make_unique<TableIterator>(table_heap_->MakeIterator()) ;
}

/**
 * Yield the next tuple from the sequential scan.
 * @param[out] tuple The next tuple produced by the scan
 * @param[out] rid The next tuple RID produced by the scan
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    while (!table_iter_->IsEnd()) {
        // 获取当前元组和 RID
        auto [meta, current_tuple] = table_iter_->GetTuple();
        *tuple = current_tuple;
        *rid = table_iter_->GetRID();
        ++(*table_iter_);  // 移动到下一个元组
        auto predicate = plan_->filter_predicate_;
       
        // 如果有谓词，检查元组是否符合条件
        if (predicate==nullptr||predicate->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>()) {
          if(!meta.is_deleted_) 
          return true;  // 返回符合条件的元组
        }
      }
    
      // 如果没有更多元组，返回 false
      return false;
    }


}  // namespace bustub
