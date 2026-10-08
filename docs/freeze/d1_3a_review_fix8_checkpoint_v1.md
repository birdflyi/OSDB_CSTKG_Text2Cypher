# D1.3a PR #6 Review-Fix 8 Checkpoint

Date: 2026-10-08
PR: #6
Branch: `feat/ch7-d1-3a-scope-projection-ir`
Pre-fix8 head: `cba8933b11ca9fe49a472ba808ee4164fda81754`

## Review findings and fixes

- Finding A: `VALID_FIXED_IMPERATIVE_LIMIT_FAIL_CLOSED`. The bounded parser now
  recognizes `limit to N`, `limit to N rows/results/entries`, and `stop at N`
  as unnormalized cardinality requests. These values are retained in IR and
  coverage audit. They are not promoted to the general D1.3b limit grammar.
  A value different from the selected template default remains unconsumed, so
  selection fails closed rather than silently rendering the default. A value
  equal to the default may be accepted through the existing contract-entailment
  path. Existing `limit N`, prior bounded phrases, and no-limit default behavior
  remain unchanged; no duplicate explicit-limit entry is created.
- Finding B: `VALID_FIXED_PRE_FIX_ROW_DERIVED_METRICS`. The evaluator now
  derives all reported pre-fix metrics from the actual `--pre-fix-rows` file.
  It validates row schema, expected-behavior values, classification values, and
  non-empty input before computing success, executable/abstention denominators,
  false abstentions, correct boundary abstentions, and undetected errors.
  Provenance and reported deltas therefore refer to the same selected artifact.

## Commits

```text
COMMIT_CODE = d29423bf262007e5809fd643b42ee7dc3f39228e
COMMIT_EVIDENCE_PARENT = d29423bf262007e5809fd643b42ee7dc3f39228e
COMMIT_EVIDENCE = this checkpoint's containing commit (recorded in Git history)
```

The code commit contains only production code and tests. It contains no v9
evidence artifacts. The evidence commit contains only v9 artifacts and this
checkpoint; no production code changes occur between the two commits.

## New tests and bounded behavior

- `limit to 10 results` and `stop at 10` are captured as `[10]` and fail closed
  against a default-25 contract; no `LIMIT 25` is rendered.
- `limit to 25 results` is accepted only through default-25 entailment and
  renders `LIMIT 25`; `limit 10` continues to render `LIMIT 10`; no cardinality
  phrase continues to use the template default.
- The row-derived metric helper rejects empty, invalid expected-behavior, and
  missing/invalid classification artifacts.
- A synthetic alternate pre-fix artifact changes both authenticated provenance
  and reported pre-fix metrics/delta, proving that runtime comparison does not
  use the former hard-coded values.

Targeted fix8 tests passed. Full `graph-migration/tests`: 186 passed. Full
`experiment-harness/tests`: 31 passed. `git diff --check` and the production
anti-overfitting scan passed.

## Canonical Git-byte v9 development regression

Generation and evaluation ran in a detached worktree with
`core.autocrlf=false`, checked out at exactly `COMMIT_CODE`.

```text
canonical_source_commit = d29423bf262007e5809fd643b42ee7dc3f39228e
canonical_git_byte_verification = PASS
all tracked direct inputs bytes_match_git_blob = true
evaluation_role = DEVELOPMENT_REGRESSION / NOT_HELDOUT
evaluation_annotations_loaded_for_generation = false
gold_or_reference_cypher_loaded_for_generation = false
NEO4J_RUN = NO
```

## v8 baseline, v9 metrics, and selected pre-fix comparison

| Metric | v8 | v9 | Delta vs v8 |
|---|---:|---:|---:|
| Executable semantic success | 12/39 | 12/39 | 0 |
| Known boundary abstention | 6/6 | 6/6 | 0 |
| False abstention | 27/39 | 27/39 | 0 |
| Undetected semantic error | 0 | 0 | 0 |
| Previous-success regressions | 0 | 0 | 0 |

The selected default pre-fix artifact is
`experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_evaluation_rows_v1.jsonl`
with SHA-256
`db26ff5cc8e295fe2adf2e29df80ff9e2c2dbd87feb1fc2466c74e0b55d30f8c`.
Its row-derived metrics are:

```text
PRE_FIX_D1_3A_METRICS =
{EXECUTABLE_SEMANTIC_SUCCESS: 12, N_EXECUTABLE: 39,
 KNOWN_BOUNDARY_ABSTENTION: 6, N_ABSTENTION: 6,
 FALSE_ABSTENTION: 27, UNDETECTED_SEMANTIC_ERROR: 0}

DELTA_VS_PRE_FIX_D1_3A =
{EXECUTABLE_SEMANTIC_SUCCESS: 0, KNOWN_BOUNDARY_ABSTENTION: 0,
 FALSE_ABSTENTION: 0, UNDETECTED_SEMANTIC_ERROR: 0}
```

The v9 development set has no classification changes from v8. These fixes add
synthetic fail-closed cardinality and provenance-metric coverage but do not
claim a development-set accuracy increase. Interpretation remains static-only.

## v9 artifacts

| Artifact | SHA-256 |
|---|---|
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_generation_traces_v9.jsonl` | `0c9fcc25c5a880fb309864c0126629ec548acbdb7732dc2467d8979679496f69` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_generation_receipt_v9.json` | `13e6e00d8aa62d0d6deda371309756a3c72189eea3195b6cf8a433196795f4b8` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_evaluation_rows_v9.jsonl` | `2c611a74d3d513d762a86011ca9f828d101514931c1be2ea64986cef4ebacde5` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_summary_v9.json` | `8dc618079e74829b036d75da3ead7d022d9fd4bb4659c59d90321f8dd181d6d8` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_delta_review_fix_v9.md` | `dc94243f32185d9755e37fa3d0d535ebfb7351402a02b1c1e1f5a9d7e5845f32` |

## Historical immutability and safety

Before v9 generation, SHA-256 snapshots were taken for all 40 existing D1.3a
v1-v8 artifacts and all 16 historical D1.2c-v1 result artifacts. After v9
generation and copying, all 56 hashes matched; no historical artifact was
modified or removed.

```text
V1_V8_ARTIFACTS_UNCHANGED = YES
HISTORICAL_D1_2C_V1_CHANGED = NO
KNOWN_BOUNDARY_ABSTENTION = 6 / 6
UNDETECTED_SEMANTIC_ERROR = 0
PREVIOUS_SUCCESS_REGRESSION = 0
QUERY_ID_ROUTING = NO
HELDOUT_SPECIFIC_PRODUCTION_LITERAL = NO
FINAL_FULL_PR_DIFF_CHECK = PASS
V1_ROLE = DEVELOPMENT_DIAGNOSTIC
V2_CONSTRUCTED = NO
NEO4J_RUN = NO
```

The complete PR-range whitespace check was run against base
`98d860a6601ed24fa8d6e8f017bf98ddca79548b` after code and evidence changes.
This checkpoint does not promote v1 to held-out evidence and does not claim
Neo4j runtime correctness.
