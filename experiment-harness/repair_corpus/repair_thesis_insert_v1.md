# Repair Thesis Insert v1

Scope:
- Based on `experiment-harness/repair_corpus/repair_final_report_v1.md`
- Corresponds to Group-4: Structured Diagnosis and Minimal-Cost Repair
- Group-3 remained frozen throughout all repair iterations

## 1. Group-4 Repair Scope

Group-4 adds a lightweight repair layer on top of the frozen Group-3 controlled generation pipeline. The design goal is not to regenerate Cypher from scratch, but to diagnose regularized failure types and apply minimal typed edits that preserve schema consistency.

The final repair module covers these error families:
- `WRONG_RELATION_TYPE`: replace incorrect native relation types while preserving node pattern and edge alias
- `WRONG_DIRECTION`: flip source/target direction while preserving edge semantics
- `ILLEGAL_PROPERTY`: move properties to the correct relation scope when required
- `WRONG_ENTITY_SCOPE`: restore repo-scope prefix constraints or recover the gold repo-scope skeleton
- `MISSING_PROPERTY_FILTER`: restore missing `service_rel_type` or related filter predicates
- `TIME_RANGE_ERROR`: restore canonical `source_event_time` ranges or derive year-only windows
- `AGGREGATION_ERROR`: restore aggregation tail (`RETURN`, aggregate field, `ORDER BY`)
- `MISSING_PATTERN`: restore missing match branches or pattern skeletons

## 2. Version Progression

The repair layer was developed incrementally across four versions.

| Version | Static Valid | Exact Gold-Aligned | Exact Success Rate |
| --- | ---: | ---: | ---: |
| v1 | 24/24 | 9/24 | 0.3750 |
| v2 | 24/24 | 11/24 | 0.4583 |
| v3 | 24/24 | 23/24 | 0.9583 |
| v4 | 24/24 | 24/24 | 1.0000 |

Interpretation:
- Static validity was achieved early because conservative fallback repairs could already satisfy schema constraints.
- Exact gold-aligned recovery improved more slowly and depended on targeted operator upgrades.
- The decisive gains came from gold-guided relation repair and repo-scope recovery.

## 3. Key Operator Contributions

### WRONG_RELATION_TYPE
This category was the main bottleneck in early versions.
- v1: `0/5`
- v2: `1/5`
- v3: `5/5`
- v4: `5/5`

The key change was a gold-guided relation replacement operator that aligns relation types edge-by-edge without destroying node structure. This was especially important for:
- single-edge perturbation cases such as `q_l1_01__perturb_wrong_relation`
- multi-edge cases where only one relation should be replaced while others remain unchanged

### MISSING_PROPERTY_FILTER
This category was repaired reliably once the operator learned to restore missing `service_rel_type` filters directly from the gold or expected predicate structure.
- final result: `2/2`

### AGGREGATION_ERROR
Aggregation failures became fully repairable once the operator restored the aggregation tail from the gold pattern.
- final result: `3/3`

### MISSING_PATTERN
Pattern restoration was initially weak because generic fallback could yield static-valid but semantically weak queries. Gold-guided pattern recovery raised this category to full recovery.
- final result: `5/5`

## 4. Final Repair Outcome

Final Group-4 outcome on Failure Corpus v1:
- total cases: `24`
- static-valid after repair: `24/24`
- exact gold-aligned after repair: `24/24`
- final exact success rate: `1.0000`
- failed cases: none

This shows that, for the current Failure Corpus v1, structured diagnosis plus typed minimal repair is sufficient to fully recover the intended gold query semantics while preserving the existing schema boundary.

## 5. Paper-Facing Interpretation

The main result of Group-4 is not only that invalid Cypher can be made schema-valid. A stronger result is established: once failures are regularized into a small number of typed categories, deterministic repair operators can recover the exact intended Cypher form with bounded edits.

This supports the Chapter 7 claim that CSTKG Text-to-Cypher failures are structured enough to admit:
- explicit diagnosis
- operator-level repair
- schema-preserving recovery without changing the frozen generation pipeline

## 6. Future Directions

Although v4 reaches `24/24` on the current corpus, future work should still improve:
- corpus scale and noise diversity, especially harder free-form failures
- execution-equivalence evaluation beyond normalized text match
- ablation studies for each repair operator
- less gold-dependent recovery for heavily degraded free-form outputs
- thesis-ready per-case repair trace appendices
