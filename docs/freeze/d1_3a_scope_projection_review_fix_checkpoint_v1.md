# D1.3a PR #6 Review-Fix Checkpoint

Status: `LOCAL_QA_PASS`

## Review adjudication

- P1 is valid: multiple distinct `EntityScope` values could be reported as consumed by one singular slot, after which materialization silently retained only the last value.
- P2 is valid: a noun directly introducing a canonical source ID could be mistaken for a requested output when preceded by a list/display cue.
- Scope is limited to singular scope-slot safety and canonical-source/projection separation. No relation, time, limit, aggregation, optional-branch, LLM, or Neo4j work was added.

## Implementation

- `graph-migration/runners/independent_controlled_pipeline.py`
  - Shared typed-scope slot analysis groups constraints by compatible singular slot and semantic identity `(label, property, operator, value)`.
  - Repeated equivalent scopes deduplicate during materialization; distinct values produce `MULTIPLE_DISTINCT_VALUES_FOR_SINGULAR_SCOPE_SLOT` and fail closed in both coverage audit and `_slot_values`.
  - Projection extraction uses canonical entity mention spans to exclude a directly introduced source noun from generic output-noun cues. Explicit `ID` / `identifier` cues remain eligible projection evidence.
- `graph-migration/tests/test_d1_3a_scope_projection.py`
  - Added 7 tests for conflicting Issue/PR prefixes, repeated-scope deduplication, lower-level materialization rejection, source-anchored Issue/PR examples, and explicit source-label ID projection.
- `experiment-harness/d1_3a/generate_v1_dev_regression.py`
- `experiment-harness/d1_3a/evaluate_v1_dev_regression.py`
  - Added artifact versioning so post-fix outputs can be written as `_v2` without overwriting pre-fix `_v1` evidence.

## Tests

| Suite | Result |
|---|---:|
| D1.3a scope/projection | 25 passed |
| Independent path | 113 passed |
| Full graph-migration | 143 passed |
| D1.2c harness | 3 passed |
| experiment-harness | 10 passed |

## Development regression

Artifacts: `experiment-harness/results/d1_3a_v1_dev_regression/*_v2.*`.

Role: `DEVELOPMENT_REGRESSION / NOT_HELDOUT`.

| Metric | Frozen D1.2c baseline | Pre-fix D1.3a | Post-fix D1.3a | Delta vs pre-fix |
|---|---:|---:|---:|---:|
| Executable semantic success | 4/39 | 12/39 | 12/39 | 0 |
| Known boundary abstention | 6/6 | 6/6 | 6/6 | 0 |
| False abstention | 35/39 | 27/39 | 27/39 | 0 |
| Undetected semantic error | 0 | 0 | 0 | 0 |

- Previously successful D1.3a executables regressed: `0`.
- RC1 rows moved past the old failure layer: `5`.
- RC3 rows moved past the old failure layer: `8`.
- No metric regression was needed to close the review findings; results remain development-only and do not support generalization.
- Generation trace SHA-256: `4877420b8330fc81c8b7ead39b8da8b5cba5fd8937ef8a6d4c0aff91aab5b7a4`.
- Evaluation rows SHA-256: `db26ff5cc8e295fe2adf2e29df80ff9e2c2dbd87feb1fc2466c74e0b55d30f8c`.

## Safety and immutability

- `P1_MULTI_SCOPE_SILENT_DROP`: `FIXED`.
- `P2_SOURCE_NOUN_PROJECTION_LEAKAGE`: `FIXED`.
- `NO_EXPLICIT_SCOPE_CONSTRAINT_SILENTLY_DROPPED`: `PASS`.
- `SOURCE_ANCHOR_OUTPUT_ROLE_SEPARATION`: `PASS`.
- `QUERY_ID_ROUTING`: `NO`.
- `HELDOUT_SPECIFIC_PRODUCTION_LITERAL`: `NO` (production scan returned no matches).
- `HISTORICAL_D1_2C_V1_CHANGED`: `NO` (no diff under frozen v1 paths).
- `PRE_FIX_D1_3A_REGRESSION_ARTIFACTS_CHANGED`: `NO` (no diff under the existing v1 regression namespace).
- `V2_CONSTRUCTED`: `NO`.
- `NEO4J_RUN`: `NO`.

## Git / next actions

- PR: `https://github.com/birdflyi/OSDB_CSTKG_Text2Cypher/pull/6`.
- Current reviewed head before this follow-up: `56b14e0c0b51068e175306968fb0d361903ec3dc`.
- After this checkpoint is committed and local QA is rechecked, push once to the existing branch; reply to and resolve the P1/P2 review threads; trigger one fresh `@codex review`.
- Do not merge, construct held-out v2, or begin D1.3b in this follow-up.
