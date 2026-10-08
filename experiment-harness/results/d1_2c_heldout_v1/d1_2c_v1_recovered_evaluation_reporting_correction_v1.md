# D1.2c-v1 recovered evaluation reporting correction v1

## Scope

This is a post-run provenance-reporting correction. It does not rerun generation, alter the immutable raw trace, change any generated IR/Cypher, or change any correctness metric.

The v1 recovered rows incorrectly labeled all 45 rows as `HISTORICAL_REFERENCE` and reported `REFERENCE_CORRECTION_COUNT = 0`. The frozen gold package is authoritative for evaluation-reference provenance.

## Corrected provenance

| Source reference kind | Rows | Correction applied |
|---|---:|---:|
| `HISTORICAL_REFERENCE` | 36 | 0 |
| `CORRECTED_EVALUATION_REFERENCE` | 3 | 3 |
| `KNOWN_PLACEHOLDER_BOUNDARY` | 6 | 0 |

The three corrected rows are `q_comp_01` variants V1/V2/V3. The six known-boundary rows are the three `q_ch5_01` and three `q_ch6_01` variants.

## Invariants checked

The v1 and v2 row artifacts are semantically identical for classification, rendered Cypher, static validity, semantic-signature result/differences, failure stage, selected template, and repair status/edits. All frozen metrics remain unchanged. Only `source_reference_kind`, `reference_correction_applied`, `REFERENCE_CORRECTION_COUNT`, source-kind counts, and the rows-file hash were updated.

`GENERATION_RERUN = NO`  
`RAW_TRACE_IMMUTABLE = YES`  
`CORRECTNESS_METRICS_CHANGED = NO`
