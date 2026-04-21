from __future__ import annotations

from .models import Diagnostic, Severity, StageMetrics


def _gb(num_bytes: int) -> float:
    return num_bytes / (1024**3)


def detect_shuffle_bottleneck(stage: StageMetrics, threshold_mb: int) -> Diagnostic | None:
    total_shuffle = stage.shuffle_read_bytes + stage.shuffle_write_bytes
    if total_shuffle < threshold_mb * 1024 * 1024:
        return None
    return Diagnostic(
        rule_id="shuffle_volume",
        title="High shuffle volume",
        severity=Severity.HIGH,
        stage_id=stage.stage_id,
        evidence=(
            f"Shuffle read/write is {_gb(total_shuffle):.2f} GB "
            f"(read={_gb(stage.shuffle_read_bytes):.2f} GB, write={_gb(stage.shuffle_write_bytes):.2f} GB)."
        ),
        recommendation="Tune partition count and aggregation strategy; review wide transformations feeding this stage.",
    )


def detect_data_skew(stage: StageMetrics, skew_ratio_threshold: float) -> Diagnostic | None:
    if stage.runtime_skew_ratio < skew_ratio_threshold:
        return None
    return Diagnostic(
        rule_id="runtime_skew",
        title="Task runtime skew detected",
        severity=Severity.HIGH,
        stage_id=stage.stage_id,
        evidence=(
            f"Runtime skew ratio={stage.runtime_skew_ratio:.2f} "
            f"(p95={stage.runtime_p95_ms:.0f} ms, max={stage.runtime_max_ms} ms)."
        ),
        recommendation="Inspect skewed keys and enable AQE skew join handling or salting for heavy keys.",
    )


def detect_stragglers(stage: StageMetrics, multiplier: float = 2.5) -> Diagnostic | None:
    if stage.runtime_avg_ms <= 0:
        return None
    if stage.runtime_max_ms < stage.runtime_avg_ms * multiplier:
        return None
    return Diagnostic(
        rule_id="straggler_tasks",
        title="Straggler tasks observed",
        severity=Severity.MEDIUM,
        stage_id=stage.stage_id,
        evidence=f"Longest task is {stage.runtime_max_ms} ms vs average {stage.runtime_avg_ms:.1f} ms.",
        recommendation="Investigate slow executors, data locality, and skewed partitions.",
    )


def detect_failure_retry_pattern(stage: StageMetrics, retry_ratio_threshold: float = 0.15) -> Diagnostic | None:
    if stage.num_tasks <= 0:
        return None
    retry_ratio = stage.retried_tasks / stage.num_tasks
    if retry_ratio < retry_ratio_threshold and stage.failed_tasks == 0:
        return None

    severity = Severity.HIGH if stage.failed_tasks > 0 else Severity.MEDIUM
    return Diagnostic(
        rule_id="retry_failure",
        title="Retry/failure pattern in stage",
        severity=severity,
        stage_id=stage.stage_id,
        evidence=(
            f"failed_tasks={stage.failed_tasks}, retried_tasks={stage.retried_tasks}, "
            f"retry_ratio={retry_ratio:.2%}."
        ),
        recommendation="Check executor logs, GC pressure, and input corruption for this stage.",
    )


def detect_expensive_wide_transformation(stage: StageMetrics) -> Diagnostic | None:
    wide_keywords = ("join", "aggregate", "reducebykey", "groupbykey", "sort")
    normalized = stage.stage_name.lower()
    if not any(token in normalized for token in wide_keywords):
        return None
    if stage.shuffle_read_bytes + stage.shuffle_write_bytes < 128 * 1024 * 1024:
        return None

    return Diagnostic(
        rule_id="wide_transformation",
        title="Expensive wide transformation",
        severity=Severity.MEDIUM,
        stage_id=stage.stage_id,
        evidence=f"Stage name '{stage.stage_name}' and shuffle activity indicate a costly wide operation.",
        recommendation="Revisit join strategy (broadcast/sort-merge), pre-filter input, and optimize keys.",
    )


def detect_partition_imbalance(stage: StageMetrics, threshold: float = 2.0) -> Diagnostic | None:
    if stage.partition_imbalance_ratio < threshold:
        return None
    return Diagnostic(
        rule_id="partition_imbalance",
        title="Partition imbalance detected",
        severity=Severity.MEDIUM,
        stage_id=stage.stage_id,
        evidence=f"Partition imbalance ratio={stage.partition_imbalance_ratio:.2f}.",
        recommendation="Repartition on better keys and tune spark.sql.shuffle.partitions for this workload.",
    )


def run_all_rules(
    stage: StageMetrics,
    shuffle_threshold_mb: int,
    skew_ratio_threshold: float,
) -> list[Diagnostic]:
    candidates = [
        detect_shuffle_bottleneck(stage, threshold_mb=shuffle_threshold_mb),
        detect_data_skew(stage, skew_ratio_threshold=skew_ratio_threshold),
        detect_stragglers(stage),
        detect_failure_retry_pattern(stage),
        detect_expensive_wide_transformation(stage),
        detect_partition_imbalance(stage),
    ]
    return [diagnostic for diagnostic in candidates if diagnostic is not None]
