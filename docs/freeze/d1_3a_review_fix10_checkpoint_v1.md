# D1.3a PR #6 Review-Fix 10 Checkpoint

Date: 2026-10-09  
PR: #6  
Pre-fix10 head: `63448cf09ba2a81788b32edd5e68544958980670`

## Review findings and bounded fixes

- Finding A (`discussion_r4221894816`): fixed the item-local DISTINCT
  silent-drop. `ControlledQueryIR.projection_distinct` remains tuple/row
  DISTINCT, while `ProjectionItem.distinct` remains an independent
  per-column requirement. A leading tuple DISTINCT no longer clears later
  `unique/distinct` cues. A tuple-only template cannot consume an additional
  item-local uniqueness requirement and therefore abstains with a typed
  unconsumed projection reason.
- Finding B (`discussion_r4221894821`): fixed bounded inline negation for
  `excluding`, `omit/omitting`, and `without` phrases governing the same ID
  prefix. These forms produce `NOT_STARTS_WITH` with provenance
  `negated_typed_prefix_scope_from_nl`, reason
  `UNSUPPORTED_NEGATED_TYPED_SCOPE_OPERATOR`, and no executable negative
  template. Positive-prefix and unrelated `without/omit` controls remain
  `STARTS_WITH`.

No general Boolean logic, negative execution template, Neo4j run, held-out v2,
D1.3b work, or historical artifact rewrite was performed.

## Commits

```text
COMMIT_CODE = 0c607e3d49547b328d71ed669ed0500975067fd2
COMMIT_EVIDENCE_PARENT = 0c607e3d49547b328d71ed669ed0500975067fd2
COMMIT_EVIDENCE = this checkpoint's containing commit (recorded in Git history)
```

The code commit contains only production code and fix10 tests. The evidence
commit contains the five v11 development artifacts and this checkpoint, with
no production-code changes between the two commits.

## Tests and safety

- New fix10 tests: 6 collected and passed; the combined targeted fix9/fix10/
  projection suite passed 69 tests.
- Full `graph-migration/tests`: 197 passed.
- Full `experiment-harness/tests`: 36 passed.
- `git diff --check` passed against base
  `98d860a6601ed24fa8d6e8f017bf98ddca79548b`.
- Production anti-overfitting scan passed:
  `QUERY_ID_ROUTING = NO`; `HELDOUT_SPECIFIC_PRODUCTION_LITERAL = NO`.

Required bounded behavior passed:

```text
TUPLE_ITEM_DISTINCT_SEPARATION = PASS
LOCAL_ITEM_UNIQUENESS_NOT_DROPPED = PASS
SINGLE_COLUMN_DISTINCT_REGRESSION = PASS
AGGREGATE_DISTINCT_REGRESSION = PASS
INLINE_EXCLUSION_NEGATED_SCOPE = PASS
NEGATED_SCOPE_FAIL_CLOSED = PASS
NEGATION_FALSE_POSITIVE_CONTROLS = PASS
POSITIVE_PREFIX_SCOPE_REGRESSION = PASS
```

## Canonical exact-byte v11 run

Generation and evaluation ran in a separate detached worktree with
`core.autocrlf=false`, checked out exactly at `COMMIT_CODE`, with a clean
tracked worktree.

```text
canonical_source_commit = 0c607e3d49547b328d71ed669ed0500975067fd2
canonical_git_byte_verification = PASS
canonical_tracked_worktree_clean = true
all tracked direct-input bytes_match_git_blob = true
all generation implementation bytes_match_git_blob = true
all evaluation implementation bytes_match_git_blob = true
evaluation_role = DEVELOPMENT_REGRESSION / NOT_HELDOUT
evaluation_annotations_loaded_for_generation = false
gold_or_reference_cypher_loaded_for_generation = false
NEO4J_RUN = NO
```

The canonical v11 receipt records five tracked direct inputs and eight
generation runtime implementation files; the v11 evaluation summary records
four evaluation runtime implementation files. Every record has
`bytes_match_git_blob = true`.

## Metrics

| Metric | v10 | v11 | Delta v11-v10 |
|---|---:|---:|---:|
| Executable semantic success | 12/39 | 12/39 | 0 |
| Known boundary abstention | 6/6 | 6/6 | 0 |
| False abstention | 27/39 | 27/39 | 0 |
| Undetected semantic error | 0 | 0 | 0 |
| Previous-success regressions | 0 | 0 | 0 |

No classification or selected-template IDs changed between v10 and v11. The
fix10 changes close parser/audit safety holes not exercised by the frozen v1
development regression set; they do not justify an accuracy increase claim.

```text
V11_DEV_METRICS =
{EXECUTABLE_SEMANTIC_SUCCESS: 12, N_EXECUTABLE: 39,
 KNOWN_BOUNDARY_ABSTENTION: 6, N_ABSTENTION: 6,
 FALSE_ABSTENTION: 27, UNDETECTED_SEMANTIC_ERROR: 0}

DELTA_VS_V10 =
{EXECUTABLE_SEMANTIC_SUCCESS: 0, KNOWN_BOUNDARY_ABSTENTION: 0,
 FALSE_ABSTENTION: 0, UNDETECTED_SEMANTIC_ERROR: 0}

BOUNDARY_ABSTENTION = 6 / 6
UNDETECTED_SEMANTIC_ERROR = 0
REGRESSED_PREVIOUS_SUCCESSES = 0
V1_ROLE = DEVELOPMENT_DIAGNOSTIC
V2_CONSTRUCTED = NO
NEO4J_RUN = NO
```

## v11 artifacts

| Artifact | SHA-256 |
|---|---|
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_generation_traces_v11.jsonl` | `0c9fcc25c5a880fb309864c0126629ec548acbdb7732dc2467d8979679496f69` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_generation_receipt_v11.json` | `322493ef77635faf493ad22e00edd78a6b47c4dc5958615593b8cc53519bd4e7` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_evaluation_rows_v11.jsonl` | `2c611a74d3d513d762a86011ca9f828d101514931c1be2ea64986cef4ebacde5` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_summary_v11.json` | `fddd865b692242de5798055dd54c1b8a2714e259a8d02383d4f822f7e641f655` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_delta_review_fix_v11.md` | `dc94243f32185d9755e37fa3d0d535ebfb7351402a02b1c1e1f5a9d7e5845f32` |

## Historical immutability and interpretation

All D1.3a v1-v10 artifacts and all historical D1.2c-v1 artifacts were checked
before and after v11 copying; no tracked historical file changed.

```text
V1_V10_ARTIFACTS_UNCHANGED = YES
HISTORICAL_D1_2C_V1_CHANGED = NO
QUERY_ID_ROUTING = NO
HELDOUT_SPECIFIC_PRODUCTION_LITERAL = NO
```

This checkpoint records development-only static evidence. It does not promote
v1 to held-out evidence, construct held-out v2, claim Neo4j runtime correctness,
or broaden the bounded semantic contract.
