# OSDB Material Pack

This pack is the canonical handoff format for plugging real OSDB data into:

- `graph-migration` (Task 0)
- `experiment-harness` (Task 1-3)

## Layout

```text
osdb_material_pack/
├─ 00_raw_samples/
├─ 01_field_dictionary/
├─ 02_mappings/
├─ 03_placeholder_rules/
└─ 04_queries_pilot/
```

## Expected flow

1. Fill `00_raw_samples/raw_records_sample.jsonl` with real raw records (desensitized).
2. Fill `01_field_dictionary/field_dictionary.csv` with actual source field semantics.
3. Fill `02_mappings/*.csv` to define node/relation normalization.
4. Fill `03_placeholder_rules/placeholder_rules.yaml` for `nan`/empty ID policy.
5. Fill `04_queries_pilot/*` for pilot evaluation set and schema constraints.

## Integration notes

- Migration tool currently consumes JSON config for mappings. Use:
  - `graph-migration/scripts/build_config_from_pack.py`
  to convert `02_mappings/*.csv` to migration config JSON.
- Experiment harness can directly consume:
  - `04_queries_pilot/queries_pilot.jsonl`
  after converting JSONL -> JSON array if needed.

