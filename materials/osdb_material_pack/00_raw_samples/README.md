# Raw Samples

Put real raw OSDB records here.

- File: `raw_records_sample.jsonl`
- One JSON object per line.
- Keep 200-1000 lines for first calibration pass.
- Desensitize identifiers if needed.

Required preferred keys (best effort):

- `source_entity_id`, `source_entity_type`
- `target_entity_id`, `target_entity_type`
- `relation_type`, `relation_label_repr`
- `event_type`, `event_trigger`, `event_time`
- `source_event_id`

Optional aux keys:

- `match_text`, `match_pattern`, `object_properties`, `multiplicity`, `weight`

