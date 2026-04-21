from __future__ import annotations

from statistics import mean, quantiles
from typing import Any

from .heuristics import run_all_rules
from .models import AnalysisReport, StageMetrics, TaskMetrics
from .parser import (
    EventLogParser,
    extract_application_metadata,
    extract_stage_metadata,
    group_task_events,
)


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


class SparkPerformanceAnalyzer:
    """Build stage metrics and diagnostics from Spark event logs."""

    def __init__(self, *, shuffle_threshold_mb: int = 512, skew_ratio_threshold: float = 3.0) -> None:
        self.shuffle_threshold_mb = shuffle_threshold_mb
        self.skew_ratio_threshold = skew_ratio_threshold

    def analyze(self, event_log_path: str) -> AnalysisReport:
        events = EventLogParser(event_log_path).load()
        app_id, app_name = extract_application_metadata(events)
        stage_metadata = extract_stage_metadata(events)
        task_events = group_task_events(events)

        stages = [
            self._build_stage_metrics(stage_id, stage_metadata.get(stage_id, {}), task_events.get(stage_id, []))
            for stage_id in sorted(set(stage_metadata) | set(task_events))
        ]

        report = AnalysisReport(app_id=app_id, app_name=app_name, stages=stages)
        for stage in stages:
            report.diagnostics.extend(
                run_all_rules(
                    stage,
                    shuffle_threshold_mb=self.shuffle_threshold_mb,
                    skew_ratio_threshold=self.skew_ratio_threshold,
                )
            )
        return report

    def _build_stage_metrics(
        self,
        stage_id: int,
        meta: dict[str, Any],
        stage_task_events: list[dict[str, Any]],
    ) -> StageMetrics:
        tasks = [self._normalize_task(stage_id, event) for event in stage_task_events]
        runtimes = [task.runtime_ms for task in tasks if task.runtime_ms > 0]
        shuffle_read = sum(task.shuffle_read_bytes for task in tasks)
        shuffle_write = sum(task.shuffle_write_bytes for task in tasks)

        runtime_avg = mean(runtimes) if runtimes else 0.0
        runtime_p95 = quantiles(runtimes, n=20)[18] if len(runtimes) >= 20 else (max(runtimes) if runtimes else 0.0)
        runtime_max = max(runtimes) if runtimes else 0
        skew_ratio = (runtime_max / runtime_avg) if runtime_avg else 0.0
        imbalance_ratio = self._partition_imbalance_ratio(tasks)

        completed = sum(1 for task in tasks if task.status == "Success")
        failed = sum(1 for task in tasks if task.status != "Success")
        unique_task_ids = {task.task_id for task in tasks}
        retried_tasks = max(0, len(tasks) - len(unique_task_ids))

        return StageMetrics(
            stage_id=stage_id,
            stage_name=str(meta.get("stage_name", f"stage-{stage_id}")),
            job_ids=list(meta.get("job_ids") or []),
            parent_ids=list(meta.get("parent_ids") or []),
            num_tasks=_safe_int(meta.get("num_tasks")) or len(unique_task_ids),
            completed_tasks=completed,
            failed_tasks=failed,
            retried_tasks=retried_tasks,
            duration_ms=int(sum(runtimes)),
            shuffle_read_bytes=shuffle_read,
            shuffle_write_bytes=shuffle_write,
            runtime_avg_ms=runtime_avg,
            runtime_p95_ms=float(runtime_p95),
            runtime_max_ms=runtime_max,
            runtime_skew_ratio=skew_ratio,
            partition_imbalance_ratio=imbalance_ratio,
        )

    def _normalize_task(self, stage_id: int, event: dict[str, Any]) -> TaskMetrics:
        task_info = event.get("Task Info") or {}
        metrics = event.get("Task Metrics") or {}
        shuffle_read_metrics = metrics.get("Shuffle Read Metrics") or {}
        shuffle_write_metrics = metrics.get("Shuffle Write Metrics") or {}
        input_metrics = metrics.get("Input Metrics") or {}
        output_metrics = metrics.get("Output Metrics") or {}

        return TaskMetrics(
            stage_id=stage_id,
            task_id=_safe_int(task_info.get("Task ID")),
            attempt=_safe_int(task_info.get("Attempt")),
            runtime_ms=_safe_int(metrics.get("Executor Run Time")),
            shuffle_read_bytes=_safe_int(shuffle_read_metrics.get("Remote Bytes Read"))
            + _safe_int(shuffle_read_metrics.get("Local Bytes Read")),
            shuffle_write_bytes=_safe_int(shuffle_write_metrics.get("Shuffle Bytes Written")),
            input_bytes=_safe_int(input_metrics.get("Bytes Read")),
            output_bytes=_safe_int(output_metrics.get("Bytes Written")),
            status=str(event.get("Task End Reason", "Unknown")),
        )

    @staticmethod
    def _partition_imbalance_ratio(tasks: list[TaskMetrics]) -> float:
        partition_load = [task.shuffle_read_bytes + task.input_bytes for task in tasks]
        non_zero = [value for value in partition_load if value > 0]
        if len(non_zero) < 2:
            return 0.0
        avg_load = mean(non_zero)
        if avg_load == 0:
            return 0.0
        return max(non_zero) / avg_load
