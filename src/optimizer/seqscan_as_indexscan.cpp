//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seqscan_as_indexscan.cpp
//
// Identification: src/optimizer/seqscan_as_indexscan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/optimizer.h"

namespace bustub {

/**
 * @brief optimize seq scan as index scan if there's an index on a table
 * @note Fall 2023 only: using hash index and only support point lookup
 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
      if (plan->GetType() != PlanType::SeqScan) {
        return plan;  // 如果不是 SeqScan，直接返回原计划
    }

    auto seq_scan_plan = std::dynamic_pointer_cast<SeqScanPlanNode>(plan);
    auto table_name = seq_scan_plan->GetTableName();
    auto predicate = seq_scan_plan->GetPredicate();

    // 检查是否存在索引
    auto index_info = catalog_.GetIndex(table_name);
    if (!index_info.has_value()) {
        return plan;  // 如果没有索引，直接返回原计划
    }

    auto [index_oid, index_name] = index_info.value();

    // 检查谓词是否支持索引扫描
    if (!IsPointLookupPredicate(predicate)) {
        return plan;  // 如果谓词不支持索引扫描，直接返回原计划
    }

    // 构造 IndexScanPlanNode
    auto index_scan_plan = std::make_shared<IndexScanPlanNode>(
        seq_scan_plan->GetOutputSchema(), index_oid, predicate);

    return index_scan_plan;  // 返回新的 IndexScan 计划
      return plan;
}

}  // namespace bustub
