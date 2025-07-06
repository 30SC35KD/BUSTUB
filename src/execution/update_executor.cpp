//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

/**
 * Construct a new UpdateExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The update plan to be executed
 * @param child_executor The child executor that feeds the update
 */
UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan), child_executor_(std::move(child_executor)){
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

/** Initialize the update */
void UpdateExecutor::Init() { //throw NotImplementedException("UpdateExecutor is not implemented"); 
  auto table_oid = plan_->GetTableOid();
    auto catalog = exec_ctx_->GetCatalog();
    table_info_ = catalog->GetTable(table_oid).get();
    table_heap_ = catalog->GetTable(table_oid)->table_.get();  // 获取目标表的 TableHeap

    // 如果有子执行器，初始化子执行器
    if (child_executor_ != nullptr) {
        child_executor_->Init();
    }
    executed_ = false;  // 标记未执行
    rows_updated_ = 0;  // 初始化更新行数
}

/**
 * Yield the next tuple from the update.
 * @param[out] tuple The next tuple produced by the update
 * @param[out] rid The next tuple RID produced by the update (ignore this)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: UpdateExecutor::Next() does not use the `rid` out-parameter.
 */
auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 

    if (executed_||child_executor_==nullptr) {return false; }  // 更新操作已经执行过，返回 false
      Tuple old_tuple;
      RID old_rid;
      
      while (child_executor_ != nullptr && child_executor_->Next(&old_tuple, &old_rid)) {
        // 根据更新表达式生成新的元组
        const auto &update_expressions = plan_->target_expressions_;
        std::vector<Value> new_values;
        for (const auto &expr : update_expressions) {
            if (expr == nullptr) {
                throw std::runtime_error("Null expression in update plan");
            }
            new_values.push_back(expr->Evaluate(&old_tuple, table_info_->schema_));
        }
        Tuple new_tuple(new_values, &table_info_->schema_);
        
        // 获取事务上下文
        auto txn = exec_ctx_->GetTransaction();
        TupleMeta tuple_meta{0,false}; 
        // 更新目标表中的元组（注意正确的参数）
        if (!table_heap_->UpdateTupleInPlace(tuple_meta, new_tuple, old_rid)) {
            throw std::runtime_error("Failed to update tuple");
        }
        rows_updated_++;
    }
    
    // 返回更新的行数
    *tuple = Tuple({Value(TypeId::INTEGER, rows_updated_)}, &GetOutputSchema());
      executed_ = true;  // 标记更新操作已完成
       return true;
    }
}  // namespace bustub
