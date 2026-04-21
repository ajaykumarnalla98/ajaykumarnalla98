from pathlib import Path

from spark_perf_analyzer.cli import main


def test_cli_generates_markdown_report(monkeypatch) -> None:
    output = Path("/tmp/spark_perf_report.md")
    if output.exists():
        output.unlink()

    monkeypatch.setattr(
        "sys.argv",
        [
            "spark-perf-analyzer",
            "sample_data/mock_event_log.jsonl",
            "--format",
            "markdown",
            "--output",
            str(output),
            "--shuffle-threshold-mb",
            "256",
            "--skew-ratio-threshold",
            "2.0",
        ],
    )

    main()

    content = output.read_text(encoding="utf-8")
    assert "Spark Performance Report" in content
    assert "Diagnostics" in content
