# D1.3a PR #6 Review-Fix 12 Checkpoint

Date: 2026-10-09
PR: #6
Pre-fix12 head: `dcd65b3c811a7e08e3c45b1f0afe3625ae8a9cdf`

## Findings and disposition

### Finding A — typed scope operator binding

Finding A was valid and is fixed. `STARTS_WITH` is now emitted only for a
bounded, explicit prefix operator: `prefix`, `starts with`, `begins with`,
`starting with`, `beginning with`, typed-ID start/begin forms, and the bounded
post-token form `... <typed ID> prefix` (for example, “IDs that fall under
PR_156018 prefix”). A bare `whose IDs` phrase is not sufficient. An adjacent
typed-ID clause with `ends/ending with` is retained as `ENDS_WITH`; `contain/
containing` is retained as `CONTAINS`; and an explicit `IDs are <typed ID>`
request is retained as `UNSPECIFIED`. These unsupported scopes use provenance
`unsupported_typed_scope_operator_from_nl`, remain visible in the IR/audit, and
fail closed with `UNSUPPORTED_TYPED_SCOPE_OPERATOR`. No ENDS_WITH, CONTAINS,
or negative execution template was added. Unrelated clause operators are not
inherited; repeated typed tokens joined by `and/or` retain the explicitly
stated operator so singular-slot conflicts still fail closed.

Previously supported negative prefix forms (`do not/don't`, `not starting`,
`excluding`, `omit/omitting`, `without`, `except`, and `but not`) remain
`NOT_STARTS_WITH` and fail closed. Positive-prefix and negation false-positive
controls remain covered.

### Finding B — generation receipt to trace chain

Finding B was valid and is fixed. Canonical evaluator mode requires a generation
receipt (explicit `--generation-receipt`, or the deterministic versioned
default). Before trace rows are loaded/classified it verifies the selected
trace's SHA-256, receipt source commit, artifact version, normalized trace path,
receipt Git-byte PASS and clean tracked-worktree status, and every runtime
implementation record's source commit/path/`bytes_match_git_blob=true`.
Receipt role fields must remain `DEVELOPMENT_REGRESSION / NOT_HELDOUT`, with
annotations and gold/reference Cypher absent. Any mismatch aborts with a
deterministic `GENERATION_TRACE_RECEIPT_*` error before evaluation output is
created. The summary records receipt path/hash, trace path/hash, receipt source
commit, evaluator source commit, and
`generation_trace_receipt_verification=PASS`.

This is provenance-chain validation, not cryptographic signing or an
adversarial tamper-proof attestation.

## Commits

```text
COMMIT_CODE = fb1d4bca3bf93102b91a3d2bf53ac353618687ec
COMMIT_EVIDENCE_PARENT = fb1d4bca3bf93102b91a3d2bf53ac353618687ec
COMMIT_EVIDENCE = this checkpoint's containing commit (recorded in Git history)
```

The code commit contains production code and tests only. The evidence commit
adds the accepted v13 development artifacts and this checkpoint, without
production-code changes.

## Tests and safety

- New typed-scope operator tests: 5 passed.
- New receipt/trace chain tests: 10 passed (including trace mutation rejection
  before row evaluation and summary hash-chain assertions).
- Combined Fix9/Fix10/Fix11/Fix12 scope/projection suite: 81 passed.
- Combined receipt and existing provenance targeted suite: 22 passed.
- Full `graph-migration/tests`: 209 passed.
- Full `experiment-harness/tests`: 46 passed.
- `git diff --check dcd65b3c811a7e08e3c45b1f0afe3625ae8a9cdf..COMMIT_CODE`:
  PASS. The broader base-to-COMMIT_CODE check reports only the two previously
  recorded trailing-space Markdown hard breaks in the immutable Fix10
  checkpoint; Fix12 introduces no new whitespace warnings.
- Production anti-overfitting scan: PASS; `QUERY_ID_ROUTING = NO` and
  `HELDOUT_SPECIFIC_PRODUCTION_LITERAL = NO`.

```text
STARTS_WITH_REQUIRES_REAL_PREFIX_OPERATOR = PASS
UNSUPPORTED_ENDS_WITH_FAIL_CLOSED = PASS
UNSUPPORTED_CONTAINS_FAIL_CLOSED = PASS
NO_OPERATOR_NO_PREFIX_FABRICATION = PASS
NEGATED_SCOPE_REGRESSIONS = PASS
CANONICAL_TRACE_RECEIPT_VERIFICATION = PASS
TRACE_MUTATION_REJECTED = PASS
TRACE_SUBSTITUTION_REJECTED = PASS
RECEIPT_SOURCE_COMMIT_MISMATCH_REJECTED = PASS
RECEIPT_ARTIFACT_VERSION_MISMATCH_REJECTED = PASS
QUERY_ID_ROUTING = NO
HELDOUT_SPECIFIC_PRODUCTION_LITERAL = NO
```

## Canonical v13 run and retry-path note

The accepted generation and evaluation used a detached worktree at the exact
`COMMIT_CODE`, with `core.autocrlf=false`, exact direct-input Git-byte matches,
generation/evaluation implementation provenance, and a clean tracked
worktree. All generation and evaluation runtime implementation records match
the same source commit and committed Git blob bytes (generation: 8 records;
evaluation: 5 records).

The first v13 attempt used an interim, superseded local commit and failed the
previous-success safety gate: 12/39 success with
`ho_q_l1_02_V3: SUCCESS -> FALSE_ABSTENTION` because the explicit “falls under
the ... prefix” wording places `prefix` immediately after the typed ID. That
bounded prefix wording was restored and regression-tested before the final
code commit. Those unaccepted intermediate files remain untracked in the
detached worktree and are not included in this evidence commit.

The environment refused removal of those failed, agent-created intermediate
files. To preserve them without overwriting and to keep the artifact chain
append-only, the accepted v13 generation/evaluation artifacts are stored under:

```text
experiment-harness/results/d1_3a_v1_dev_regression_fix12_v13_retry/
```

An initial retry evaluator invocation omitted `--traces`, so receipt validation
rejected the default-path trace before row evaluation; it wrote no evaluation
artifacts. The successful invocation passed the exact retry trace and receipt
paths. Only the successful five v13 artifacts from the retry directory are
included in COMMIT_EVIDENCE; the failed attempt files in the standard result
directory are excluded.

```text
canonical_source_commit = fb1d4bca3bf93102b91a3d2bf53ac353618687ec
canonical_git_byte_verification = PASS
canonical_tracked_worktree_clean = true
generation_implementation_provenance = PASS (8/8)
evaluation_implementation_provenance = PASS (5/5)
generation_trace_receipt_verification = PASS
evaluation_role = DEVELOPMENT_REGRESSION
heldout_role = NOT_HELDOUT
evaluation_annotations_loaded = false
gold_or_reference_cypher_loaded = false
```

## Metrics and classification delta

| Metric | v12 | accepted v13 | Delta v13-v12 |
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

## Accepted v13 artifacts and SHA-256

| Artifact | SHA-256 |
|---|---|
| `experiment-harness/results/d1_3a_v1_dev_regression_fix12_v13_retry/d1_3a_v1_dev_generation_receipt_v13.json` | `4f2ca98e734b6c386f392d8993f5c9806fa527e1b9c7bbd293eb2cf7c8bc0689` |
| `experiment-harness/results/d1_3a_v1_dev_regression_fix12_v13_retry/d1_3a_v1_dev_generation_traces_v13.jsonl` | `df4ef1e8fd1d8cfae3ad4518341e74287c4ce8390cbe4156ed6edbeda0b90ae8` |
| `experiment-harness/results/d1_3a_v1_dev_regression_fix12_v13_retry/d1_3a_v1_dev_evaluation_rows_v13.jsonl` | `0b99ab37049981fc7df13dcf69d5c2d601843038c3cc78aac85db7e5f40fd8e1` |
| `experiment-harness/results/d1_3a_v1_dev_regression_fix12_v13_retry/d1_3a_v1_dev_summary_v13.json` | `6292ad21b64d4905573ff172b3ff90f6f7ae9f83e2e115eb06dafc5d21f8bf1` |
| `experiment-harness/results/d1_3a_v1_dev_regression_fix12_v13_retry/d1_3a_v1_dev_delta_review_fix_v13.md` | `ea6e603287d9c33aae442d75b12663916575593786454e8afeb754a8fca9962b` |

## Historical immutability

No tracked D1.3a v1-v12 artifacts or historical D1.2c-v1 artifacts changed in
the Fix12 code commit. The evidence commit adds only the accepted v13 retry
artifacts and this checkpoint. Historical v12 remains development-only and
was not rewritten.

```text
V1_V12_ARTIFACTS_UNCHANGED = YES
HISTORICAL_D1_2C_V1_CHANGED = NO
QUERY_ID_ROUTING = NO
HELDOUT_SPECIFIC_PRODUCTION_LITERAL = NO
```
