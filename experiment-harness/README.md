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
