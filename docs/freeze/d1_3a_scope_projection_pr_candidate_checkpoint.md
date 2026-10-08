# D1.3a Scope / Projection Candidate Checkpoint

Status: `LOCAL_QA_PASS_PENDING_REVIEW`

## Base

- `BASE_TAG`: `ch7-d1-2c-v1-diagnostic-freeze`
- `BASE_SHA`: `98d860a6601ed24fa8d6e8f017bf98ddca79548b`
- `V1_ROLE`: `DEVELOPMENT_DIAGNOSTIC`
- `V2_REQUIRED_AFTER_TUNING`: `YES`
- `V2_CONSTRUCTED`: `NO`

## Bounded implementation

- Added typed `EntityScope(label, property, operator, value, provenance, source_span)`.
- Added ordered `ProjectionItem(role, label, property, distinct, nullable, alias, provenance, source_span)`.
- Added candidate-level scope and projection consumption audit with typed unconsumed reasons.
- Preserved the v4 template pack unchanged and added `independent_template_pack_v5.yaml` as a generic contract extension.
- Added one generic Issue prefix listing contract and one ordered ExternalResource ID-then-domain contract.
- Added bounded fail-closed handling for unsupported cardinality wording when it conflicts with a contract default; full limit normalization remains deferred.

## Changed production files

- `graph-migration/runners/independent_controlled_pipeline.py`
- `data_real/pilot_queries/independent_template_pack_v5.yaml`

## Tests and regression harness

- `graph-migration/tests/test_d1_3a_scope_projection.py`: 18 D1.3a synthetic positive/negative and anti-overfitting tests.
- `graph-migration/tests/test_d1_independent_path.py`: 113 passed.
- `graph-migration/tests` full suite: 136 passed.
- `experiment-harness/d1_2c/test_d1_2c_harness.py`: 3 passed.
- `experiment-harness/tests`: 10 passed.
- D1.3a v1 development runner/evaluator:
  - `experiment-harness/d1_3a/generate_v1_dev_regression.py`
  - `experiment-harness/d1_3a/evaluate_v1_dev_regression.py`

## Typed scope examples

- `PR_900001` -> `PullRequest.entity_id STARTS_WITH PR_900001`.
- `I_880002` -> `Issue.entity_id STARTS_WITH I_880002`.
- `PR_900001#12` and `I_880002#77` remain canonical equality anchors.
- Bare numeric fragments do not become typed scopes.

## Role-aware projection examples

- Issue anchor -> Actor `entity_id`.
- PullRequest anchor -> UnknownObject `entity_id`.
- PullRequest anchor -> ExternalResource `entity_id` plus `url_domain_etld1` in requested order.
- Issue prefix -> Issue `entity_id` plus Commit `entity_id`.

## Development regression

Artifact namespace: `experiment-harness/results/d1_3a_v1_dev_regression/`.

| metric | frozen v1 baseline | D1.3a development | delta |
|---|---:|---:|---:|
| executable semantic success | 4/39 | 12/39 | +8 |
| known-boundary abstention | 6/6 | 6/6 | 0 |
| false abstention | 35/39 | 27/39 | -8 |
| undetected semantic error | 0 | 0 | 0 |

- RC1 scope-prefix rows moved past the old failure layer: 5.
- RC3 target-role rows moved past the old failure layer: 8.
- Previously successful executable regressions: 0.
- `NEO4J_RUN`: `NO`.
- `HELDOUT_V1_ROLE`: `DEVELOPMENT_DIAGNOSTIC`.

## Anti-overfitting and immutability

- `QUERY_ID_ROUTING`: `NO`.
- `HELDOUT_SPECIFIC_PRODUCTION_LITERAL`: `NO` (synthetic literals appear only in tests/fixtures).
- `V1_HISTORICAL_ARTIFACT_CHANGED`: `NO`.
- No files under `data_real/heldout_v1/`, `experiment-harness/results/d1_2c_heldout_v1/`, or the D1.2c freeze checkpoint were modified.
- The v1 development runner loaded only `id`/`nl_query`; gold/reference annotations were not loaded.

## Interpretation

The metric delta is development-regression evidence only. It does not establish post-tuning generalization, runtime executability, or result correctness. A separately authored, contamination-audited independent v2 is required after tuning.
