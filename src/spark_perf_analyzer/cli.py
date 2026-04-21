from __future__ import annotations

import argparse

from .analyzer import SparkPerformanceAnalyzer
from .reporting import report_to_json, report_to_markdown, write_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Spark Performance Analysis Tool: detect shuffle, skew, retries, stragglers, and imbalance"
    )
    parser.add_argument("event_log", help="Path to Spark event log (JSON-lines)")
    parser.add_argument("--shuffle-threshold-mb", type=int, default=512, help="Shuffle threshold in MB")
    parser.add_argument("--skew-ratio-threshold", type=float, default=3.0, help="Task runtime skew ratio threshold")
    parser.add_argument(
        "--format",
        choices=["markdown", "json"],
        default="markdown",
        help="Output report format",
    )
    parser.add_argument("--output", default=None, help="Optional output path. Prints to stdout if omitted")
    return parser


def main() -> None:
    args = build_parser().parse_args()

    analyzer = SparkPerformanceAnalyzer(
        shuffle_threshold_mb=args.shuffle_threshold_mb,
        skew_ratio_threshold=args.skew_ratio_threshold,
    )
    report = analyzer.analyze(args.event_log)

    if args.format == "json":
        content = report_to_json(report)
    else:
        content = report_to_markdown(report)

    write_report(content, args.output)


if __name__ == "__main__":
    main()
