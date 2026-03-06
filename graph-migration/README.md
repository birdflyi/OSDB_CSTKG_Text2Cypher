# OSDB Graph Migration Tool

Deterministic migration pipeline that converts noisy raw graph records into a normalized, query-friendly graph model.

## Run

```bash
python graph-migration/cli.py migrate ^
  --input fixtures/raw_records.json ^
  --input-format json ^
  --export-mode json ^
  --output outputs/normalized_graph.json ^
  --placeholder-policy external_if_url ^
  --relation-mapping-config-path fixtures/relation_mapping_config.json
```

Legacy compatibility is preserved:
- `python graph-migration/cli.py --input ... --input-format ... --export-mode ...`
- This old form is internally routed to `migrate` so there is only one execution path.

## Export modes

- `memory`: returns in-memory object summary only.
- `json`: writes normalized graph JSON.
- `jsonl`: writes one node/edge per line.
- `csv`: writes `nodes.csv` and `edges.csv`.
- `cypher`: writes Cypher statements for optional Neo4j import.

## Notes

- Node labels are semantic and stable (`Actor`, `Issue`, `Repo`, etc).
- Entity IDs are stored as node properties (`entity_id`) and never used as labels.
- Relationship types are normalized from semantic rules, not raw `event_type` alone.
- TODO markers in code indicate dataset-specific knobs you can customize safely.
