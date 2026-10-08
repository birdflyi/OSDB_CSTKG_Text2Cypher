# D1.3a PR #6 Review-Fix 5 Checkpoint

Date: 2026-10-08
PR: #6
Branch: `feat/ch7-d1-3a-scope-projection-ir`
Pre-fix5 head: `80ff463b7bff8e7cfe92e05a1912c90a3801fd6b`

## Findings and fixes

- Finding A: `VALID_FIXED_CRLF_EXACT_BYTE_REBUILD`. v6 was generated from
  exact committed Git blob bytes; no semantic input-content divergence was
  found in the old v4/v5 records.
- Finding B: `VALID_FIXED_AGGREGATION_NO_DROP`. Ordinary count and distinct
  count requirements are parsed independently and every explicit requirement
  must be consumed or cause abstention.
- Finding C: `VALID_FIXED_SINGLE_COLUMN_DISTINCT_EQUIVALENCE`. A top-level
  `RETURN DISTINCT` proves item uniqueness only when the RETURN has one
  expression corresponding to the requested item; multi-column DISTINCT does
  not receive this equivalence.

## Commits

- `COMMIT_CODE = cf8f90ec33186163054b1b14ca5592b68bada0cd`
  (`fix(ch7): close D1.3a aggregation and provenance gaps`)
- `COMMIT_EVIDENCE` is the child commit recorded after this checkpoint.
- `COMMIT_EVIDENCE_PARENT = cf8f90ec33186163054b1b14ca5592b68bada0cd`

## Canonical Git-byte v6 run

The run used a detached `core.autocrlf=false` worktree at `COMMIT_CODE`.
Generation and evaluation both recorded:

```text
canonical_source_commit = cf8f90ec33186163054b1b14ca5592b68bada0cd
canonical_git_byte_verification = PASS
```

The generation receipt authenticated queries, schema, v5 template pack, and
the v4 dependency. The evaluation summary authenticated gold, frozen baseline
rows, pre-fix rows, and the frozen evaluator.

## v6 development metrics

| Metric | v6 |
|---|---:|
| Executable semantic success | 12/39 |
| Known boundary abstention | 6/6 |
| False abstention | 27/39 |
| Undetected semantic error | 0 |
| Previous-success regressions | 0 |
| Neo4j run | NO |

Delta vs v5: all reported metrics are unchanged (0, 0, 0, 0 respectively).
No held-out v2 was constructed.

## v6 artifacts

| Artifact | SHA-256 |
|---|---|
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_generation_traces_v6.jsonl` | `c20a7a76db6666971150eee6beabee13dbf51f49b03668748e515a33ae1c1471` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_generation_receipt_v6.json` | `b525c4a97c56ab3d77ee6b1efe7dc5241aa5b13888c9716ec249ec32a2237582` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_evaluation_rows_v6.jsonl` | `2c611a74d3d513d762a86011ca9f828d101514931c1be2ea64986cef4ebacde5` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_summary_v6.json` | `e6319b4a601fe996b8d2bd628460e826ef01d4a96f67be11528f97698f5f6ddc` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_delta_review_fix_v6.md` | `56a0cae66735f85f2ced4b7bba4d20c652660665a01a5350c6599da6dfa3de20` |

## Historical immutability

All existing D1.3a v1-v5 artifacts were hashed before/after the v6 run and
were unchanged. The v5 hashes remain those recorded in the fix4 checkpoint;
the v1-v4 inventory there also remains unchanged. All 16 historical
`experiment-harness/results/d1_2c_heldout_v1/` artifacts were unchanged.

```text
V1_V5_ARTIFACTS_UNCHANGED = YES
HISTORICAL_D1_2C_V1_CHANGED = NO
```

## QA

```text
graph-migration/tests = 162 passed
experiment-harness/tests = 27 passed
targeted fix5 tests = 55 passed
git diff --check = PASS
QUERY_ID_ROUTING = NO
HELDOUT_SPECIFIC_PRODUCTION_LITERAL = NO
V2_CONSTRUCTED = NO
NEO4J_RUN = NO
```

The role of v1 remains `DEVELOPMENT_DIAGNOSTIC`; this checkpoint does not
promote it to held-out evidence.
