from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any


class EventLogParser:
    """Parse Spark event logs (JSON lines) into normalized structures."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def load(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            raise FileNotFoundError(f"Event log file not found: {self.path}")

        events: list[dict[str, Any]] = []
        with self.path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"Invalid JSON in {self.path} at line {line_number}: {exc.msg}"
                    ) from exc
        return events


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def extract_application_metadata(events: list[dict[str, Any]]) -> tuple[str, str]:
    app_id = "unknown-app-id"
    app_name = "unknown-app"
    for event in events:
        if event.get("Event") == "SparkListenerApplicationStart":
            app_name = str(event.get("App Name", app_name))
        if event.get("Event") == "SparkListenerEnvironmentUpdate":
            props = event.get("Spark Properties") or {}
            app_id = str(props.get("spark.app.id", app_id))
    return app_id, app_name


def extract_stage_metadata(events: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    stages: dict[int, dict[str, Any]] = {}
    for event in events:
        if event.get("Event") != "SparkListenerStageSubmitted":
            continue

        info = event.get("Stage Info") or {}
        stage_id = info.get("Stage ID")
        if stage_id is None:
            continue

        stages[int(stage_id)] = {
            "stage_name": str(info.get("Stage Name", f"stage-{stage_id}")),
            "num_tasks": _safe_int(info.get("Number of Tasks")),
            "parent_ids": [int(parent) for parent in (info.get("Parent IDs") or [])],
            "job_ids": [],
        }

    for event in events:
        if event.get("Event") != "SparkListenerJobStart":
            continue
        job_id = _safe_int(event.get("Job ID"))
        for stage_id in event.get("Stage IDs") or []:
            sid = _safe_int(stage_id)
            if sid in stages and job_id not in stages[sid]["job_ids"]:
                stages[sid]["job_ids"].append(job_id)

    return stages


def group_task_events(events: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    task_events: dict[int, list[dict[str, Any]]] = defaultdict(list)

    for event in events:
        if event.get("Event") != "SparkListenerTaskEnd":
            continue
        stage_id = event.get("Stage ID")
        if stage_id is None:
            continue
        task_events[int(stage_id)].append(event)

    return task_events
