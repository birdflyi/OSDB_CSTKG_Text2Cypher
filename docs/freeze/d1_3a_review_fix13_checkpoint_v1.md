# D1.3a PR #6 Review-Fix 13 Checkpoint

Date: 2026-10-09
PR: #6
Pre-fix13 head: `50ea705610ccf97cda86beb888baedc08e5238eb`

## Findings and disposition

### Finding A — who/whoever-derived ID uniqueness

Finding A was valid and is fixed. The bounded `distinct|unique IDs|identifiers
of ... who/whoever` construction now propagates item-local uniqueness to the
`Actor.entity_id` projection item. Ordinary `IDs of whoever opened it` remains
`distinct=false` and keeps the existing `indv4_issue_opened_by` behavior.
Because that contract renders ordinary `RETURN a.entity_id` and does not
formally guarantee item-local uniqueness, the explicit unique/distinct request
is preserved in IR and audit and fails closed. Tuple DISTINCT is not inferred.

### Finding B — domain-property item uniqueness

Finding B was valid and is fixed. Bounded `unique|distinct domains`,
`registrable domains`, and `site domains` cues now propagate to the
`ExternalResource.url_domain_etld1` projection item. Ordinary domains remain
`distinct=false`; a unique resource-ID cue does not leak to the domain item.
The ordinary `indv5_reference_external_id_domain` contract is therefore
rejected for an explicitly unique domain request rather than silently
weakening it. `count distinct domains` remains aggregate-local and does not
create a standalone domain projection item.

## Commits

```text
COMMIT_CODE = ed2f3cafde3e3abe52b02f7759967b8c84476090
COMMIT_EVIDENCE_PARENT = ed2f3cafde3e3abe52b02f7759967b8c84476090
COMMIT_EVIDENCE = this checkpoint's containing commit (recorded in Git history)
```

The code commit contains only production code and tests. The evidence commit
adds v14 artifacts and this checkpoint, with no production-code changes.

## Tests and acceptance

- New Fix13 targeted tests: 7 passed.
- D1.3a scope/projection file: 65 passed.
- Full `graph-migration/tests`: 216 passed.
- Full `experiment-harness/tests`: 46 passed.
- `git diff --check 50ea705610ccf97cda86beb888baedc08e5238eb..COMMIT_CODE`:
  PASS. Fix13 introduces no whitespace warnings.
- Production anti-overfitting scan: PASS; no query-ID routing or
  heldout-specific production literal was added.

```text
WHO_ID_LOCAL_UNIQUENESS_PRESERVED = PASS
WHO_ID_ORDINARY_REGRESSION = PASS
WHO_PRONOUN_SOURCE_ANCHOR_REGRESSION = PASS
DOMAIN_ITEM_UNIQUENESS_PRESERVED = PASS
DOMAIN_ORDINARY_REGRESSION = PASS
DOMAIN_AGGREGATE_DISTINCT_SEPARATION = PASS
TUPLE_ITEM_DISTINCT_SEPARATION = PASS
AGGREGATE_DISTINCT_SEPARATION = PASS
CANONICAL_TRACE_RECEIPT_VERIFICATION = PASS
CANONICAL_IMPLEMENTATION_PROVENANCE = PASS
QUERY_ID_ROUTING = NO
HELDOUT_SPECIFIC_PRODUCTION_LITERAL = NO
```

## Canonical v14 development regression

The accepted run used a detached worktree at the exact `COMMIT_CODE`, with
`core.autocrlf=false`, a clean tracked worktree, direct-input Git-byte
verification, generation/evaluation implementation provenance, and exact
generation-trace-to-receipt verification. Runtime provenance records match
the same source commit and committed Git blob bytes (generation: 8/8;
evaluation: 5/5).

```text
canonical_source_commit = ed2f3cafde3e3abe52b02f7759967b8c84476090
canonical_git_byte_verification = PASS
canonical_tracked_worktree_clean = true
generation_implementation_provenance = PASS (8/8)
evaluation_implementation_provenance = PASS (5/5)
generation_trace_receipt_verification = PASS
evaluation_role = DEVELOPMENT_REGRESSION
heldout_role = NOT_HELDOUT
evaluation_annotations_loaded = false
gold_or_reference_cypher_loaded = false
V2_CONSTRUCTED = NO
NEO4J_RUN = NO
```

## Metrics and classification delta

| Metric | v13 | accepted v14 | Delta v14-v13 |
|---|---:|---:|---:|
| Executable semantic success | 13/39 | 13/39 | 0 |
| Known boundary abstention | 6/6 | 6/6 | 0 |
| False abstention | 26/39 | 26/39 | 0 |
| Undetected semantic error | 0 | 0 | 0 |
| Previous-success regressions | 0 | 0 | 0 |

```text
CLASSIFICATION_CHANGED_IDS = []
BOUNDARY_ABSTENTION = 6 / 6
UNDETECTED_SEMANTIC_ERROR = 0
REGRESSED_PREVIOUS_SUCCESSES = 0
V1_ROLE = DEVELOPMENT_DIAGNOSTIC
V2_CONSTRUCTED = NO
NEO4J_RUN = NO
```

## Accepted v14 artifacts and SHA-256

| Artifact | SHA-256 |
|---|---|
| `experiment-harness/results/d1_3a_v1_dev_regression_fix13_v14/d1_3a_v1_dev_generation_receipt_v14.json` | `c61b338617eabf41e49925d485d74868894092b18ba34bbb5fd4549a5dd20207` |
| `experiment-harness/results/d1_3a_v1_dev_regression_fix13_v14/d1_3a_v1_dev_generation_traces_v14.jsonl` | `df4ef1e8fd1d8cfae3ad4518341e74287c4ce8390cbe4156ed6edbeda0b90ae8` |
| `experiment-harness/results/d1_3a_v1_dev_regression_fix13_v14/d1_3a_v1_dev_evaluation_rows_v14.jsonl` | `0b99ab37049981fc7df13dcf69d5c2d601843038c3cc78aac85db7e5f40fd8e1` |
| `experiment-harness/results/d1_3a_v1_dev_regression_fix13_v14/d1_3a_v1_dev_summary_v14.json` | `57fbcef8e9a73f4f6be83c1450c83c37fb7ca7457fa85bd59a2315f5eba36d14` |
| `experiment-harness/results/d1_3a_v1_dev_regression_fix13_v14/d1_3a_v1_dev_delta_review_fix_v14.md` | `e33c289b82d80df67998142a9bebfbb35e125bc384956826d0abd616ca402d6a` |

The generation receipt records the same trace hash:

```text
generation_trace_path = experiment-harness/results/d1_3a_v1_dev_regression_fix13_v14/d1_3a_v1_dev_generation_traces_v14.jsonl
generation_trace_sha256 = df4ef1e8fd1d8cfae3ad4518341e74287c4ce8390cbe4156ed6edbeda0b90ae8
generation_receipt_sha256 = c61b338617eabf41e49925d485d74868894092b18ba34bbb5fd4549a5dd20207
```

## Historical immutability

All accepted D1.3a v1-v13 artifacts remain unchanged. Historical D1.2c-v1
artifacts remain unchanged. Fix13 adds only the v14 namespace and this
checkpoint; it does not rewrite v1-v13 or any prior checkpoint.

```text
V1_V13_ARTIFACTS_UNCHANGED = YES
HISTORICAL_D1_2C_V1_CHANGED = NO
QUERY_ID_ROUTING = NO
HELDOUT_SPECIFIC_PRODUCTION_LITERAL = NO
```
