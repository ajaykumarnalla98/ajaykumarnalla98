from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from .models import AnalysisReport


def report_to_json(report: AnalysisReport) -> str:
    payload = {
        "app_id": report.app_id,
        "app_name": report.app_name,
        "stages": [asdict(stage) for stage in report.stages],
        "diagnostics": [
            {
                **asdict(diagnostic),
                "severity": diagnostic.severity.value,
            }
            for diagnostic in report.diagnostics
        ],
    }
    return json.dumps(payload, indent=2)


def report_to_markdown(report: AnalysisReport) -> str:
    lines = [
        f"# Spark Performance Report: {report.app_name}",
        "",
        f"- App ID: `{report.app_id}`",
        f"- Stages analyzed: **{len(report.stages)}**",
        f"- Diagnostics raised: **{len(report.diagnostics)}**",
        "",
        "## Diagnostics",
        "",
    ]

    if not report.diagnostics:
        lines.extend(["No major issues detected for configured thresholds.", ""])
    else:
        for diagnostic in report.diagnostics:
            lines.extend(
                [
                    f"### [{diagnostic.severity.value.upper()}] Stage {diagnostic.stage_id} - {diagnostic.title}",
                    f"- Rule: `{diagnostic.rule_id}`",
                    f"- Evidence: {diagnostic.evidence}",
                    f"- Recommendation: {diagnostic.recommendation}",
                    "",
                ]
            )

    lines.extend(["## Stage Summary", ""])
    for stage in report.stages:
        lines.append(
            "- "
            f"Stage {stage.stage_id} (`{stage.stage_name}`): tasks={stage.num_tasks}, "
            f"failed={stage.failed_tasks}, shuffle={(stage.shuffle_read_bytes + stage.shuffle_write_bytes) / (1024**2):.1f} MB, "
            f"skew={stage.runtime_skew_ratio:.2f}"
        )

    return "\n".join(lines) + "\n"


def write_report(content: str, output_path: str | None) -> None:
    if output_path is None:
        print(content)
        return
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
