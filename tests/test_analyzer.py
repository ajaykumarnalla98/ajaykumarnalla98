from spark_perf_analyzer.analyzer import SparkPerformanceAnalyzer


def test_analyzer_detects_multiple_issue_types() -> None:
    analyzer = SparkPerformanceAnalyzer(shuffle_threshold_mb=256, skew_ratio_threshold=2.0)
    report = analyzer.analyze("sample_data/mock_event_log.jsonl")

    rule_ids = {diagnostic.rule_id for diagnostic in report.diagnostics}
    assert report.app_id == "app-20260421-0001"
    assert len(report.stages) == 2
    assert "shuffle_volume" in rule_ids
    assert "runtime_skew" in rule_ids
    assert "retry_failure" in rule_ids
    assert "wide_transformation" in rule_ids
    assert "partition_imbalance" in rule_ids


def test_threshold_can_suppress_shuffle_finding() -> None:
    analyzer = SparkPerformanceAnalyzer(shuffle_threshold_mb=4096, skew_ratio_threshold=10.0)
    report = analyzer.analyze("sample_data/mock_event_log.jsonl")
    rule_ids = {diagnostic.rule_id for diagnostic in report.diagnostics}
    assert "shuffle_volume" not in rule_ids
    assert "runtime_skew" not in rule_ids
