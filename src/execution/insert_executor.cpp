//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the insert */
void InsertExecutor::Init() { //throw NotImplementedException("InsertExecutor is not implemented"); 
    auto table_oid = plan_->GetTableOid();
    auto catalog = exec_ctx_->GetCatalog();
    table_heap_ = catalog->GetTable(table_oid)->table_.get();  // 获取目标表的 TableHeap

    // 如果有子执行器，初始化子执行器
    if (child_executor_ != nullptr) {
        child_executor_->Init();
    }

    rows_inserted_ = 0;  // 初始化插入行数
    executed_ = false;   // 标记未执行
}

/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple The integer tuple indicating the number of rows inserted into the table
 * @param[out] rid The next tuple RID produced by the insert (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
 */
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    if (executed_) {
        return false;  // 插入操作已经执行过，返回 false
      }
    
      if (child_executor_ != nullptr) {
        // 从子执行器中获取元组并插入
        Tuple child_tuple;
        RID child_rid;
        while (child_executor_->Next(&child_tuple, &child_rid)) {
            TupleMeta tuple_meta{0,false}; 
          if (!table_heap_->InsertTuple(tuple_meta,child_tuple,nullptr, nullptr, plan_->GetTableOid())) {
            throw std::runtime_error("Failed to insert tuple");
          }
          rows_inserted_++;
        }
      } else {
        // 插入常量元组（如果计划支持常量插入）
        throw std::runtime_error("InsertPlanNode does not support constant values.");
      }
    
      // 返回插入的行数作为结果
      *tuple = Tuple({Value(TypeId::INTEGER, rows_inserted_)}, &GetOutputSchema());
      executed_ = true;  // 标记插入操作已完成
      return true;
    }

}  // namespace bustub
