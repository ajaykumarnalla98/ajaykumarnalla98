from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Severity(str, Enum):
    INFO = "info"
    MEDIUM = "medium"
    HIGH = "high"


@dataclass(slots=True)
class TaskMetrics:
    stage_id: int
    task_id: int
    attempt: int
    runtime_ms: int
    shuffle_read_bytes: int
    shuffle_write_bytes: int
    input_bytes: int
    output_bytes: int
    status: str


@dataclass(slots=True)
class StageMetrics:
    stage_id: int
    stage_name: str
    job_ids: list[int]
    parent_ids: list[int]
    num_tasks: int
    completed_tasks: int
    failed_tasks: int
    retried_tasks: int
    duration_ms: int
    shuffle_read_bytes: int
    shuffle_write_bytes: int
    runtime_avg_ms: float
    runtime_p95_ms: float
    runtime_max_ms: int
    runtime_skew_ratio: float
    partition_imbalance_ratio: float


@dataclass(slots=True)
class Diagnostic:
    rule_id: str
    title: str
    severity: Severity
    stage_id: int
    evidence: str
    recommendation: str


@dataclass(slots=True)
class AnalysisReport:
    app_id: str
    app_name: str
    stages: list[StageMetrics] = field(default_factory=list)
    diagnostics: list[Diagnostic] = field(default_factory=list)
