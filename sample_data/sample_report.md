# Spark Performance Report: daily-sales-etl

- App ID: `app-20260421-0001`
- Stages analyzed: **2**
- Diagnostics raised: **6**

## Diagnostics

### [HIGH] Stage 10 - High shuffle volume
- Rule: `shuffle_volume`
- Evidence: Shuffle read/write is 1.60 GB (read=1.16 GB, write=0.44 GB).
- Recommendation: Tune partition count and aggregation strategy; review wide transformations feeding this stage.

### [HIGH] Stage 10 - Task runtime skew detected
- Rule: `runtime_skew`
- Evidence: Runtime skew ratio=2.55 (p95=5100 ms, max=5100 ms).
- Recommendation: Inspect skewed keys and enable AQE skew join handling or salting for heavy keys.

### [MEDIUM] Stage 10 - Straggler tasks observed
- Rule: `straggler_tasks`
- Evidence: Longest task is 5100 ms vs average 2000.0 ms.
- Recommendation: Investigate slow executors, data locality, and skewed partitions.

### [HIGH] Stage 10 - Retry/failure pattern in stage
- Rule: `retry_failure`
- Evidence: failed_tasks=1, retried_tasks=1, retry_ratio=25.00%.
- Recommendation: Check executor logs, GC pressure, and input corruption for this stage.

### [MEDIUM] Stage 10 - Expensive wide transformation
- Rule: `wide_transformation`
- Evidence: Stage name 'SortMergeJoin stage' and shuffle activity indicate a costly wide operation.
- Recommendation: Revisit join strategy (broadcast/sort-merge), pre-filter input, and optimize keys.

### [MEDIUM] Stage 10 - Partition imbalance detected
- Rule: `partition_imbalance`
- Evidence: Partition imbalance ratio=2.33.
- Recommendation: Repartition on better keys and tune spark.sql.shuffle.partitions for this workload.

## Stage Summary

- Stage 10 (`SortMergeJoin stage`): tasks=4, failed=1, shuffle=1640.3 MB, skew=2.55
- Stage 11 (`mapPartitions narrow stage`): tasks=2, failed=0, shuffle=2.1 MB, skew=1.03
