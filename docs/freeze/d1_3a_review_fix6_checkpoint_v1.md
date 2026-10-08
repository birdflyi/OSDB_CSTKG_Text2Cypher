# D1.3a PR #6 Review-Fix 6 Checkpoint

Date: 2026-10-08  
PR: #6  
Branch: `feat/ch7-d1-3a-scope-projection-ir`  
Pre-fix6 head: `66ea47724486275b14dd4dfccfa91774a335e44f`

## Review findings and fixes

- Finding A: `VALID_FIXED_DISTINCT_IDS_OF_ENTITY`. Item DISTINCT now binds
  through bounded `distinct/unique IDs|identifiers of <entity>` phrases. It
  remains local to that item, does not become tuple DISTINCT, and aggregate
  argument spans do not become projections through the generic-ID fallback.
- Finding B: `VALID_FIXED_COMPOSITE_SOURCE_ANCHOR_SUBSPAN`. The full typed noun
  occurrence directly introducing a canonical source ID is identified first;
  noun occurrences wholly contained in that source phrase are suppressed as
  projections. Later explicit output phrases outside the source span remain
  eligible.

## Commits

- `COMMIT_CODE = 9be7907de2e39540a7e69ebb146517a6e0357d64`
  (`fix(ch7): harden projection cues for distinct IDs and composite anchors`)
- `COMMIT_EVIDENCE` is the child commit recorded after this checkpoint.
- `COMMIT_EVIDENCE_PARENT = 9be7907de2e39540a7e69ebb146517a6e0357d64`
- The code commit contains production code and tests only. No production code
  changed between `COMMIT_CODE` and `COMMIT_EVIDENCE`.

## New regression cases

Six new test functions cover 11 input cases, including:

- `distinct IDs of actors`, `unique identifiers of actors`, ordinary `IDs of
  actors`, and noun-first `distinct actor IDs`;
- local item DISTINCT in a multi-column request, without tuple DISTINCT;
- aggregate-only `count distinct actor IDs`, without an accidental projection;
- IssueComment, PullRequestReview, and PullRequestReviewComment source-span
  containment;
- a later explicit Issue-ID output outside the IssueComment source span; and
- the existing simple Issue source / Actor output case.

Results: targeted `test_d1_3a_scope_projection.py` = 44 passed; full
`graph-migration/tests` = 168 passed; full `experiment-harness/tests` = 27
passed.

## Canonical Git-byte v7 run

Generation and evaluation ran in a detached worktree with
`core.autocrlf=false`, checked out at exactly `COMMIT_CODE`.

```text
canonical_source_commit = 9be7907de2e39540a7e69ebb146517a6e0357d64
canonical_git_byte_verification = PASS
tracked direct inputs bytes_match_git_blob = true
evaluation role = DEVELOPMENT_REGRESSION / NOT_HELDOUT
```

## v7 development metrics

| Metric | v6 | v7 | Delta vs v6 |
|---|---:|---:|---:|
| Executable semantic success | 12/39 | 12/39 | 0 |
| Known boundary abstention | 6/6 | 6/6 | 0 |
| False abstention | 27/39 | 27/39 | 0 |
| Undetected semantic error | 0 | 0 | 0 |
| Previous-success regressions | 0 | 0 | 0 |
| Neo4j run | NO | NO | — |

These two review fixes are covered by synthetic parser regressions; the frozen
39-row development set contains no row that changes classification. The v7
run therefore confirms no metric regression but does not demonstrate a
development-set accuracy increase. The interpretation remains static-only.

## v7 artifacts

| Artifact | SHA-256 |
|---|---|
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_generation_traces_v7.jsonl` | `c20a7a76db6666971150eee6beabee13dbf51f49b03668748e515a33ae1c1471` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_generation_receipt_v7.json` | `f25f69fe0cceffacf39f0314fd004412ebe56ec5cead4f0baeac822400e8b82c` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_evaluation_rows_v7.jsonl` | `2c611a74d3d513d762a86011ca9f828d101514931c1be2ea64986cef4ebacde5` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_summary_v7.json` | `38518be0d8bd81c15399005113cdae79b50ad81383fb4f1f1c0774605487f943` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_delta_review_fix_v7.md` | `56a0cae66735f85f2ced4b7bba4d20c652660665a01a5350c6599da6dfa3de20` |

All copied v7 files were verified byte-identical to the canonical detached
worktree outputs.

## Historical immutability and safety

All 30 existing D1.3a v1-v6 artifacts and all 16 historical D1.2c-v1 result
artifacts were hashed before and after generation and were unchanged.

```text
V1_V6_ARTIFACTS_UNCHANGED = YES
HISTORICAL_D1_2C_V1_CHANGED = NO
QUERY_ID_ROUTING = NO
HELDOUT_SPECIFIC_PRODUCTION_LITERAL = NO
V2_CONSTRUCTED = NO
NEO4J_RUN = NO
```

`git diff --check = PASS`. This checkpoint does not promote the v1 development
diagnostic to held-out evidence.
