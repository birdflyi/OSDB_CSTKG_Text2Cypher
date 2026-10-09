# D1.3a PR #6 Review-Fix 9 Checkpoint

Date: 2026-10-09
PR: #6
Pre-fix9 head: `b099e72b8ddf9cd8f6f9dcd830a98020ad4cdd4f`

## Review findings and bounded fixes

- Finding A: `VALID_FIXED_NEGATED_PREFIX_FAIL_CLOSED`. Typed prefix scopes now
  represent bounded negation as `NOT_STARTS_WITH` with provenance
  `negated_typed_prefix_scope_from_nl`. No current template consumes that
  operator, so the constraint remains in the IR/audit trace with reason code
  `UNSUPPORTED_NEGATED_TYPED_SCOPE_OPERATOR` and generation abstains. Positive
  `STARTS_WITH` behavior is unchanged. No negative Cypher operator was added.
- Finding B: `VALID_FIXED_RUNTIME_IMPLEMENTATION_BINDING`. Canonical runs now
  require `HEAD == canonical_source_commit` and a clean tracked worktree before
  generation/evaluation. Direct implementation files are recorded with working
  SHA-256, Git blob SHA, Git-blob SHA-256, source commit, and
  `bytes_match_git_blob`. Untracked output files are allowed after the gate.
- Finding C: `VALID_FIXED_PUNCTUATED_SOURCE_INTRODUCTION`. Source anchors now
  accept only bounded structural gaps (whitespace plus `:`, `(`, `[`, `{`, or
  `-`). Lexical wording such as `issue ID I_...` remains distinct from a source
  introduction. Composite source-anchor suppression remains active.

## Commits

```text
COMMIT_CODE = 6a2dacd1a42f2f1e2d09863019b931b1ad5193cf
COMMIT_EVIDENCE_PARENT = 6a2dacd1a42f2f1e2d09863019b931b1ad5193cf
COMMIT_EVIDENCE = this checkpoint's containing commit (recorded in Git history)
```

The code commit contains production code and tests only; it contains no v10
evidence. The evidence commit contains v10 artifacts and this checkpoint, with
no production code changes between the two commits.

## Tests and safety

- New fix9 tests: 10 collected and passed.
- Full `graph-migration/tests`: 191 passed.
- Full `experiment-harness/tests`: 36 passed.
- `git diff --check` passed against base `98d860a6601ed24fa8d6e8f017bf98ddca79548b`.
- Production anti-overfitting scan passed:
  `QUERY_ID_ROUTING = NO`; `HELDOUT_SPECIFIC_PRODUCTION_LITERAL = NO`.

Required bounded behavior passed:

```text
NEGATED_PREFIX_SCOPE_FAIL_CLOSED = PASS
POSITIVE_PREFIX_SCOPE_REGRESSION = PASS
CANONICAL_TRACKED_WORKTREE_GATE = PASS
DIRTY_PIPELINE_CANONICAL_RUN_REJECTED = PASS
DIRTY_GENERATOR_CANONICAL_RUN_REJECTED = PASS
RUNTIME_IMPLEMENTATION_PROVENANCE = PASS
PUNCTUATED_SOURCE_INTRODUCTION = PASS
LEXICAL_GAP_NOT_SOURCE_INTRODUCTION = PASS
COMPOSITE_SOURCE_ANCHOR_REGRESSION = PASS
```

## Canonical exact-byte v10 run

Generation and evaluation ran in a detached worktree with
`core.autocrlf=false`, checked out at exactly `COMMIT_CODE`.

```text
canonical_source_commit = 6a2dacd1a42f2f1e2d09863019b931b1ad5193cf
canonical_git_byte_verification = PASS
canonical_tracked_worktree_clean = true
all tracked direct-input bytes_match_git_blob = true
all recorded runtime/evaluator implementation bytes_match_git_blob = true
evaluation_role = DEVELOPMENT_REGRESSION / NOT_HELDOUT
evaluation_annotations_loaded_for_generation = false
gold_or_reference_cypher_loaded_for_generation = false
NEO4J_RUN = NO
```

Generation implementation provenance covers:

```text
experiment-harness/d1_3a/generate_v1_dev_regression.py
experiment-harness/d1_3a/input_provenance.py
experiment-harness/d1_3a/template_provenance.py
experiment-harness/d1_3a/artifact_safety.py
graph-migration/runners/independent_controlled_pipeline.py
graph-migration/repair/gold_blind_repair.py
graph-migration/validators/pilot_cypher_validator.py
graph-migration/normalizers/derived_slot_builder.py
```

Evaluation implementation provenance covers:

```text
experiment-harness/d1_3a/evaluate_v1_dev_regression.py
experiment-harness/d1_3a/input_provenance.py
experiment-harness/d1_3a/artifact_safety.py
experiment-harness/d1_2c/evaluate_heldout_v1.py
```

Every file listed above has `bytes_match_git_blob = true`; the complete
per-file records are in the v10 generation receipt and evaluation summary.

## Metrics

| Metric | v9 | v10 | Delta vs v9 |
|---|---:|---:|---:|
| Executable semantic success | 12/39 | 12/39 | 0 |
| Known boundary abstention | 6/6 | 6/6 | 0 |
| False abstention | 27/39 | 27/39 | 0 |
| Undetected semantic error | 0 | 0 | 0 |
| Previous-success regressions | 0 | 0 | 0 |

The fix9 changes are safety/provenance closures and do not claim a development
set accuracy increase. v10 remains static-only development evidence.

```text
V10_DEV_METRICS =
{EXECUTABLE_SEMANTIC_SUCCESS: 12, N_EXECUTABLE: 39,
 KNOWN_BOUNDARY_ABSTENTION: 6, N_ABSTENTION: 6,
 FALSE_ABSTENTION: 27, UNDETECTED_SEMANTIC_ERROR: 0}

DELTA_VS_V9 =
{EXECUTABLE_SEMANTIC_SUCCESS: 0, KNOWN_BOUNDARY_ABSTENTION: 0,
 FALSE_ABSTENTION: 0, UNDETECTED_SEMANTIC_ERROR: 0}

BOUNDARY_ABSTENTION = 6 / 6
UNDETECTED_SEMANTIC_ERROR = 0
REGRESSED_PREVIOUS_SUCCESSES = 0
```

## v10 artifacts

| Artifact | SHA-256 |
|---|---|
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_generation_traces_v10.jsonl` | `0c9fcc25c5a880fb309864c0126629ec548acbdb7732dc2467d8979679496f69` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_generation_receipt_v10.json` | `fff946bbb270f8838c79ae9705759b6305e4758e311b030947f09936c62c86b4` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_evaluation_rows_v10.jsonl` | `2c611a74d3d513d762a86011ca9f828d101514931c1be2ea64986cef4ebacde5` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_summary_v10.json` | `cb11c17c3ec2650cc54c000304052c1fef34a13ed80579a00d5b85b2bebd9cfd` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_delta_review_fix_v10.md` | `dc94243f32185d9755e37fa3d0d535ebfb7351402a02b1c1e1f5a9d7e5845f32` |

The v10 receipt and summary bind both direct inputs and runtime implementation
to the canonical source commit. The v9 disposition is unchanged: direct input
byte provenance was PASS, implementation binding was INCOMPLETE, and no
semantic divergence is claimed for v9. v10 supersedes v9 as the first complete
commit-bound input-plus-implementation evidence.

## Historical immutability and interpretation

All D1.3a v1-v9 artifacts and all historical D1.2c-v1 artifacts were checked
before and after v10 copying; hashes are unchanged.

```text
V1_V9_ARTIFACTS_UNCHANGED = YES
HISTORICAL_D1_2C_V1_CHANGED = NO
V1_ROLE = DEVELOPMENT_DIAGNOSTIC
V2_CONSTRUCTED = NO
NEO4J_RUN = NO
QUERY_ID_ROUTING = NO
HELDOUT_SPECIFIC_PRODUCTION_LITERAL = NO
```

This checkpoint does not promote v1 to held-out evidence, does not construct
held-out v2, and does not claim Neo4j runtime correctness.
