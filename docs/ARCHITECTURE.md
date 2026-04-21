# Architecture Notes

## Parsing strategy
- Consume Spark event logs line-by-line to support large files.
- Normalize only fields needed for diagnostics in MVP.
- Keep raw events separate from derived entities to support future plugins.

## Rule engine strategy
- Each rule is a focused function returning optional `Diagnostic`.
- `run_all_rules` orchestrates execution.
- Future: convert to pluggable registry with rule metadata and config profiles.

## Extensibility design
- `AnalysisReport` is stable interchange model for future web/API layers.
- `reporting.py` keeps output formatting isolated from analytics core.
- CLI remains thin orchestration wrapper to avoid business logic duplication.
