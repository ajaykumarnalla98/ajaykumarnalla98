# Spark Performance Analysis Tool

A production-oriented, CLI-first Python project that analyzes Spark event logs and stage DAG metadata to detect performance anti-patterns and produce actionable diagnostics.

## 1) Product definition

### Problem solved
Spark jobs often run slowly due to hidden execution issues (shuffle-heavy stages, skewed keys, straggler tasks, retries, and partition imbalance). Teams spend hours in Spark UI manually investigating these patterns. This tool automates that investigation from event logs.

### Target users
- Data engineers operating ETL/data lake workloads.
- Spark developers optimizing batch/SQL pipelines.
- Platform teams creating performance guardrails and CI checks.

### Core use cases
- Identify high-shuffle stages after a failed SLA run.
- Detect skewed stages and straggler tasks before production rollout.
- Summarize retry/failure hotspots for reliability triage.
- Export diagnostics for postmortems and optimization tickets.

## 2) MVP scope vs future scope

### MVP (implemented)
- Parse Spark event logs (JSON-lines, no cluster required).
- Build stage/task metrics.
- Heuristic diagnostics for:
  - shuffle bottlenecks
  - runtime skew
  - straggler tasks
  - retry/failure patterns
  - expensive wide transformations (name + shuffle signal)
  - partition imbalance
- Generate JSON or Markdown report via CLI.
- Include sample event log + sample report + unit tests.

### Future scope
- Spark SQL query plan parsing for join strategy advice.
- Historical comparison (`baseline` vs `current`) and regression alerts.
- HTML/web dashboard with trend charts.
- Plugin rule registry and configurable policy packs.
- Integration with data catalogs and orchestration tools.

## 3) Repository structure

```text
.
├── .github/workflows/ci.yml
├── sample_data/
│   ├── mock_event_log.jsonl
│   └── sample_report.md
├── src/spark_perf_analyzer/
│   ├── __init__.py
│   ├── analyzer.py
│   ├── cli.py
│   ├── heuristics.py
│   ├── models.py
│   ├── parser.py
│   └── reporting.py
├── tests/
│   ├── test_analyzer.py
│   └── test_cli.py
├── LICENSE
├── pyproject.toml
└── README.md
```

## 4) Architecture overview

### Data flow
1. **Input:** Spark event log (`SparkListener*` JSON events).
2. **Parser layer:** Normalize app metadata, stage metadata, and task events.
3. **Analyzer layer:** Compute stage metrics and task/runtime distributions.
4. **Rules engine:** Run heuristics and emit diagnostics with recommendations.
5. **Reporting layer:** Render JSON/Markdown reports.
6. **CLI:** User-facing entrypoint for local analysis and file output.

### Module responsibilities
- `parser.py`: event log loading and metadata extraction.
- `analyzer.py`: metric computation and orchestration.
- `heuristics.py`: detection rules/threshold logic.
- `reporting.py`: report serialization.
- `cli.py`: command-line UX.
- `models.py`: typed dataclasses and report entities.

## 5) Quickstart

```bash
python -m pip install -e .[dev]
spark-perf-analyzer sample_data/mock_event_log.jsonl --format markdown --output sample_data/sample_report.md
```

Inspect JSON output:

```bash
spark-perf-analyzer sample_data/mock_event_log.jsonl --format json
```

## 6) Heuristics currently implemented

- **Shuffle bottleneck:** stage shuffle read+write exceeds configurable MB threshold.
- **Data skew:** max/avg task runtime skew ratio exceeds threshold.
- **Straggler tasks:** max runtime significantly larger than average.
- **Retry/failure pattern:** high retry ratio and/or failed tasks.
- **Expensive wide transformation:** stage name suggests join/group/sort + meaningful shuffle.
- **Partition imbalance:** max partition load significantly exceeds average.

## 7) Development

Run tests:

```bash
python -m pytest
```

## 8) Contributor friendliness

### Good first issues
1. Add parser support for compressed event logs (`.gz`).
2. Add configurable rule profiles (`strict`, `balanced`, `aggressive`).
3. Add CSV export for diagnostics.
4. Add support for executor-level hotspot summaries.
5. Improve wide-transformation detection from SQL plan fields.

### Roadmap / TODO
- [ ] Add historical report comparison mode.
- [ ] Build HTML report template.
- [ ] Add Spark SQL-level join diagnostics.
- [ ] Add per-executor skew and spill detection.
- [ ] Publish package and semantic release process.

## 9) License

Suggested and included license: **MIT**.
