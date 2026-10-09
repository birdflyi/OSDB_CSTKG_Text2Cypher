# D1.3a PR #6 Review-Fix 14 Checkpoint

Date: 2026-10-09
PR: #6

## Commits

```text
PRE_FIX14_HEAD = e9cdc9cf63318ec3619db7d57dc140320f1543ea
COMMIT_CODE = 615691d3927e541158aaa63933ba58f6845c2eb5
COMMIT_EVIDENCE = this checkpoint's containing commit (recorded in Git history)
COMMIT_EVIDENCE_PARENT = 615691d3927e541158aaa63933ba58f6845c2eb5
```

`COMMIT_CODE` contains only the production implementation and Fix14 targeted tests. The evidence commit contains no production-code changes.

## Findings fixed

```text
Finding A = VALID_FIXED_REPO_SCOPE_TYPED_PREFIX_CONFLICT
Finding B = VALID_FIXED_POST_TOKEN_NEGATED_PREFIX
Evidence defect C = VALID_CORRECTED_BY_ADDENDUM
```

### Repository scope / typed prefix consistency

For a canonical or deterministically derived repository `R_<n>`, an explicit positive typed scope for `PullRequest`, `Issue`, or another configured label must equal the corresponding derived base prefix (`PR_<n>`, `I_<n>`, etc.). A mismatch is recorded as `REPO_SCOPE_TYPED_PREFIX_CONFLICT` with repository ID, scope label, expected prefix, explicit value, operator, and source span. The candidate coverage is rejected before template selection, so no Cypher is rendered.

The same-value case remains legal and maps to one effective slot value. `_slot_values(...)` repeats the guard and raises `RepoScopeTypedPrefixConflictError` before a conflicting value can overwrite a repository-derived slot.

### Post-token prefix negation

Bounded forms such as `excluding I_900001 prefix`, `without I_900001 prefix`, `except PR_900001 prefix`, and `but not ... prefix` now preserve `NOT_STARTS_WITH` with provenance `negated_typed_prefix_scope_from_nl`. The current execution contract does not support a negative typed scope, so coverage fails closed with `UNSUPPORTED_NEGATED_TYPED_SCOPE_OPERATOR`; no negative Cypher template was added.

Positive post-token forms and unrelated negation controls remain positive. Existing pre-token negation forms remain unchanged.

## Tests

```text
NEW_TARGETED_TESTS = 32 passed
FULL_GRAPH_MIGRATION_TESTS = 247 passed
FULL_EXPERIMENT_HARNESS_TESTS = 46 passed
NEW_FIX14_DIFF_CHECK = PASS
QUERY_ID_ROUTING = NO
HELDOUT_SPECIFIC_PRODUCTION_LITERAL = NO
```

## Fix13 provenance correction

The historical checkpoint `docs/freeze/d1_3a_review_fix13_checkpoint_v1.md` is unchanged. The correction is recorded in:

```text
docs/freeze/d1_3a_review_fix13_checkpoint_hash_correction_v1.md
```

```text
FIX13_DELTA_OLD_RECORDED_SHA = e33c289b82d80df67998142a9bebfbb35e125bc384956826d0abd616ca402d6a
FIX13_DELTA_AUTHORITATIVE_SHA = 50450e019ce63235082c2e4ac0a96ae130b7d7bec52c5d38dd437db42e384599
```

The four other v14 artifact hashes match their historical checkpoint. The addendum supersedes only the incorrect v14 delta-Markdown field; no v14 artifact was regenerated, and no semantic or metric result changed.

## Canonical v15 development regression

The run was generated and evaluated in a detached worktree at exact `COMMIT_CODE`, with `core.autocrlf=false`, clean tracked worktree, direct-input Git-byte verification, implementation provenance, and exact trace-to-receipt verification.

```text
CANONICAL_SOURCE_COMMIT = 615691d3927e541158aaa63933ba58f6845c2eb5
CANONICAL_GIT_BYTE_VERIFICATION = PASS
CANONICAL_TRACKED_WORKTREE_CLEAN = true
GENERATION_IMPLEMENTATION_PROVENANCE = PASS (8/8)
EVALUATION_IMPLEMENTATION_PROVENANCE = PASS (5/5)
GENERATION_TRACE_RECEIPT_VERIFICATION = PASS
```

Exactly five v15 artifacts are committed under:

```text
experiment-harness/results/d1_3a_v1_dev_regression_fix14_v15/
```

| Artifact | SHA-256 |
|---|---|
| `d1_3a_v1_dev_generation_traces_v15.jsonl` | `df4ef1e8fd1d8cfae3ad4518341e74287c4ce8390cbe4156ed6edbeda0b90ae8` |
| `d1_3a_v1_dev_generation_receipt_v15.json` | `373ef38eb2ba80de7e49f337181da076c770e82b25e3dfde4c044ef5d6647c5f` |
| `d1_3a_v1_dev_evaluation_rows_v15.jsonl` | `0b99ab37049981fc7df13dcf69d5c2d601843038c3cc78aac85db7e5f40fd8e1` |
| `d1_3a_v1_dev_summary_v15.json` | `53ea44757943f0a172ffc26978c8ad8b43f5cb6dbe5b4b17f4d988df533cd935` |
| `d1_3a_v1_dev_delta_review_fix_v15.md` | `ea6e603287d9c33aae442d75b12663916575593786454e8afeb754a8fca9962b` |

## Metrics and interpretation

```text
V14_EXECUTABLE_SEMANTIC_SUCCESS = 13 / 39
V15_EXECUTABLE_SEMANTIC_SUCCESS = 13 / 39
DELTA_V15_V14 = 0
BOUNDARY_ABSTENTION = 6 / 6
UNDETECTED_SEMANTIC_ERROR = 0
REGRESSED_PREVIOUS_SUCCESSES = 0
CLASSIFICATION_CHANGED_IDS = []
```

The v15 run is development-only diagnostic evidence. It is not held-out evidence, does not construct v2, and does not run Neo4j.

```text
V1_V14_ACCEPTED_RESULT_ARTIFACTS_UNCHANGED = YES
FIX13_CHECKPOINT_FILE_CHANGED = NO
HISTORICAL_D1_2C_V1_CHANGED = NO
V2_CONSTRUCTED = NO
NEO4J_RUN = NO
```
