# Repair Eval v4

Scope:
- Input corpus: `experiment-harness/repair_corpus/repair_failure_corpus_v1.jsonl`
- Repair module: `experiment-harness/repair/lightweight_repair.py`
- Frozen Group-3 artifacts were not modified during evaluation.

## Overall
- total_cases: 24
- static_valid_after_repair: 24/24
- exact_gold_match_after_repair: 24/24
- exact_success_rate: 1.0000
- source_distribution: {'free_form': 13, 'controlled_perturbation': 11}

## Focus Categories
- `WRONG_RELATION_TYPE`: exact=5/5, static_valid=5/5
- `MISSING_PROPERTY_FILTER`: exact=2/2, static_valid=2/2
- `AGGREGATION_ERROR`: exact=3/3, static_valid=3/3
- `MISSING_PATTERN`: exact=5/5, static_valid=5/5

## WRONG_ENTITY_SCOPE Focus
- exact_gold_match_after_repair: 2/2
- static_valid_after_repair: 2/2
- q_l1_02__free_form: success

## By Failure Type
| Failure Type            | Total | Static Valid | Exact Match | Success Rate |
| ----------------------- | ----- | ------------ | ----------- | ------------ |
| AGGREGATION_ERROR       | 3     | 3            | 3           | 1.0000       |
| ILLEGAL_PROPERTY        | 2     | 2            | 2           | 1.0000       |
| MISSING_PATTERN         | 5     | 5            | 5           | 1.0000       |
| MISSING_PROPERTY_FILTER | 2     | 2            | 2           | 1.0000       |
| TIME_RANGE_ERROR        | 3     | 3            | 3           | 1.0000       |
| WRONG_DIRECTION         | 2     | 2            | 2           | 1.0000       |
| WRONG_ENTITY_SCOPE      | 2     | 2            | 2           | 1.0000       |
| WRONG_RELATION_TYPE     | 5     | 5            | 5           | 1.0000       |

## By Primary Repair Edit
| Primary Edit                    | Cases | Static Valid | Exact Match |
| ------------------------------- | ----- | ------------ | ----------- |
| fallback_simpler_template       | 2     | 2            | 2           |
| flip_relation_direction         | 7     | 7            | 7           |
| repair_aggregation              | 1     | 1            | 1           |
| repair_entity_scope             | 5     | 5            | 5           |
| repair_relation_scoped_property | 2     | 2            | 2           |
| repair_time_range               | 1     | 1            | 1           |
| replace_relation_type           | 2     | 2            | 2           |
| restore_missing_pattern         | 2     | 2            | 2           |
| restore_property_filters        | 2     | 2            | 2           |

## By Applied Edit Participation
| Applied Edit                    | Cases Using Edit | Static Valid | Exact Match |
| ------------------------------- | ---------------- | ------------ | ----------- |
| fallback_simpler_template       | 2                | 2            | 2           |
| flip_relation_direction         | 7                | 7            | 7           |
| repair_aggregation              | 1                | 1            | 1           |
| repair_entity_scope             | 5                | 5            | 5           |
| repair_relation_scoped_property | 2                | 2            | 2           |
| repair_time_range               | 1                | 1            | 1           |
| replace_relation_type           | 2                | 2            | 2           |
| restore_missing_pattern         | 4                | 4            | 4           |
| restore_property_filters        | 2                | 2            | 2           |

## Failed Cases
None
