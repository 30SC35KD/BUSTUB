//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

/**
 * Construct a new DeleteExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The delete plan to be executed
 * @param child_executor The child executor that feeds the delete
 */
DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {}

/** Initialize the delete */
void DeleteExecutor::Init() { //throw NotImplementedException("DeleteExecutor is not implemented"); 
    auto table_oid = plan_->GetTableOid();
    auto catalog = exec_ctx_->GetCatalog();
    table_info_ = catalog->GetTable(table_oid).get();
    table_heap_ = catalog->GetTable(table_oid)->table_.get();  // 获取目标表的 TableHeap

    // 如果有子执行器，初始化子执行器
    if (child_executor_ != nullptr) {
        child_executor_->Init();
    }
    executed_ = false;  // 标记未执行
    rows_deleted_ = 0;  // 初始化删除行数
    }

/**
 * Yield the number of rows deleted from the table.
 * @param[out] tuple The integer tuple indicating the number of rows deleted from the table
 * @param[out] rid The next tuple RID produced by the delete (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: DeleteExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: DeleteExecutor::Next() returns true with the number of deleted rows produced only once.
 */
auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { //return false; 
    if (executed_) {
        return false;
    }

    // 从子执行器获取元组并删除
    Tuple child_tuple;
    RID child_rid;
    while (child_executor_->Next(&child_tuple, &child_rid)) {
        // 标记元组为删除状态
        TupleMeta tuple_meta{0, true};
        table_heap_->UpdateTupleMeta(tuple_meta, child_rid);
        table_heap_->UpdateTupleInPlace(tuple_meta, child_tuple, child_rid);
        rows_deleted_++;
        
    }

    // 返回删除的行数作为结果
    *tuple = Tuple({Value(TypeId::INTEGER, rows_deleted_)}, &GetOutputSchema());
    executed_ = true;
    return true;

}

}  // namespace bustub
