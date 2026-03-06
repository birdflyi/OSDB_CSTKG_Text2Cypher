# OSDB Text-to-Cypher Experiment Harness

Minimal, reproducible evaluation harness that consumes normalized graph data from the migration tool.

## Structure

- `data/`
- `generators/`
- `validators/`
- `repair/`
- `eval/`
- `metrics/`
- `runners/`

## Quick start

Single method:

```bash
python experiment-harness/cli.py --generator controlled --examples-path "experiment-harness/examples/query_examples.json" --graph-metadata-path "graph-migration/outputs/normalized_graph.json" --output-path "experiment-harness/outputs/controlled_report.json" --config-path "experiment-harness/configs/runner_config.json"
```

Compare all methods:

```bash
python experiment-harness/cli.py --compare-all --examples-path "experiment-harness/examples/query_examples.json" --graph-metadata-path "graph-migration/outputs/normalized_graph.json" --output-dir "experiment-harness/outputs/compare" --config-path "experiment-harness/configs/compare_config.json"
```

With lightweight repair:

```bash
python experiment-harness/cli.py --generator free_form --apply-repair --repair-module lightweight --examples-path "experiment-harness/examples/query_examples.json" --graph-metadata-path "experiment-harness/examples/graph_metadata.json" --output-path "experiment-harness/outputs/free_form_repair.json" --normalized-match
```

## Group-4 Repair Contributions

The **Group-4** repair module introduces a lightweight repair layer that improves failure recovery for the controlled Text-to-Cypher evaluation workflow. The main repair types cover:

- **WRONG_RELATION_TYPE**: identifies and corrects incorrect relation types while preserving node patterns and aliases when possible.
- **WRONG_DIRECTION**: repairs edge direction errors by flipping source and target endpoints while keeping edge semantics intact.
- **ILLEGAL_PROPERTY**: repairs property placement errors, especially when relation-scoped properties are incorrectly accessed as node-scoped properties.
- **WRONG_ENTITY_SCOPE**: restores the correct repo-scope prefix or gold-skeleton scope for degraded free-form queries missing entity scope constraints.
- **MISSING_PROPERTY_FILTER**: restores missing `service_rel_type` and related filter predicates required by the expected query structure.
- **TIME_RANGE_ERROR**: restores canonical `source_event_time` bounds or derives year-based ranges from natural-language input when appropriate.
- **AGGREGATION_ERROR**: restores missing aggregation components such as `RETURN`, `ORDER BY`, and aggregate fields.
- **MISSING_PATTERN**: restores missing match patterns or branches so the query shape aligns with the expected gold structure.

### Version Progression of Group-4 Repair

| Version | Static Valid | Exact Gold-Aligned | Exact Success Rate |
| --- | ---: | ---: | ---: |
| v1 | 24/24 | 9/24 | 0.3750 |
| v2 | 24/24 | 11/24 | 0.4583 |
| v3 | 24/24 | 23/24 | 0.9583 |
| v4 | 24/24 | 24/24 | 1.0000 |

Key observations:
- Static validity was reached early because bounded fallback repairs could remain within schema constraints.
- Exact gold-aligned recovery improved sharply in v3 and v4 after targeted operator upgrades.

### How Group-4 Repairs Fit into the Pipeline

Group-4 repair operators are implemented as a lightweight post-generation repair layer in the evaluation workflow. When invalid queries are detected, the repair module applies minimal, typed edits and validates the repaired query against the static schema. This allows repair quality to be measured at two levels: static-valid recovery and exact gold-aligned recovery.

## Results Snapshot

| Mode           | Success | Fail | Skip |
| -------------- | ------: | ---: | ---: |
| template_first |      12 |    1 |    2 |
| controlled     |      13 |    0 |    2 |

`COUPLES_WITH` and `RESOLVES` remain injection-pending and are therefore counted in the skipped set rather than the executable success set.

## Notes

- `execution_accuracy` currently uses exact/normalized text match proxy.
- TODO(neo4j): replace proxy with real execution backend in `runners/experiment_runner.py`.
- Per-example traces are saved in each report under `details`.
- Controlled main method is deterministic and modular:
  - constraints: `generators/constraints.py`
  - typed slots: `generators/slots.py`
  - template skeletons: `generators/templates.py`
  - controlled orchestration: `generators/controlled.py`
- Lightweight repair module is bounded and interpretable:
  - diagnosis schema: `repair/diagnosis.py`
  - repair actions + search: `repair/lightweight_repair.py`
