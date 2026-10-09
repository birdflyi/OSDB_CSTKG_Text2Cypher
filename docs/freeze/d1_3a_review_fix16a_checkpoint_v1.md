# D1.3a PR #6 Fix16a Checkpoint

## Disposition

Fix16a closes a compound entity-cardinality firewall bypass discovered by a
read-only post-Fix16 probe. The wording remains outside the frozen controlled
language grammar; the correction strengthens fail-closed behavior and does not
add general entity-noun cardinality support.

```text
PRE_FIX16A_HEAD = 841659ae85f9ac59d99d0f8c7a904f70716eb08f
POST_FIX16_PROBE = For issue I_1#2, show at most 10 people who opened it and their IDs
PROBE_RESULT_BEFORE_FIX16A = unsafe executable weakening to LIMIT 25
CLASSIFICATION = CATEGORY_C_OUT_OF_GRAMMAR_WEAKENED_EXECUTION_BLOCKER
ROOT_CAUSE = broad downstream IDs/identifiers exemption in unsupported-cardinality firewall
FIX16_CHECKPOINT_REWRITTEN = NO
V17_ARTIFACTS_REWRITTEN = NO
V17_METRICS_CHANGED_RETROACTIVELY = NO
```

Fix16a supersedes only the claim that Fix16's cardinality firewall was complete
for compound out-of-grammar surfaces containing later ID-projection wording.
The earlier checkpoint and evidence remain immutable.

## Correction

The broad `IDs/identifiers later` exemption was removed. An unsupported
cap remains explicit unless it belongs to the narrow already-frozen
PullRequest-list structure: a PullRequest cap, explicit PullRequest
`STARTS_WITH` scope, and a sole `PullRequest.entity_id` projection. In that
case the numeric cap is routed through the existing limit contract audit:
the template-default value may be entailed, but a conflicting value is
rejected. Later ID wording alone grants no exemption. Actor, people, and user
caps remain fail-closed, including when numerically equal to the template
default.

```text
COMMIT_CODE_FIX16A = 18dcfe902e493cf7e436e431ddf253d955e38a41
COMMIT_EVIDENCE_FIX16A = containing checkpoint commit (exact OID reported at handoff)
COMMIT_EVIDENCE_PARENT = 18dcfe902e493cf7e436e431ddf253d955e38a41
```

## Verification

The focused Fix16a, Fix16 boundary, and scope/projection tests pass (90 total).
The full `graph-migration/tests` suite passes (279); the full
`experiment-harness/tests` suite passes (52). Production anti-overfitting scan
found no held-out IDs/literals or request-ID routing in the controlled parser
or template pack. `git diff --check` passes.

The compound probes for people/actors/users fail closed without Cypher,
including the default-equal cap. The frozen PullRequest ID-list request with
cap 25 remains executable and is audited as entailed by the template's 25
default. Its cap-10 counterpart has no rendered Cypher and cannot inherit
`LIMIT 25`. Existing `top 10` behavior remains executable with `LIMIT 10`.

## Canonical v18 development regression

The canonical run was generated and evaluated in a detached worktree at exact
`COMMIT_CODE_FIX16A`, with `core.autocrlf=false`, a clean tracked worktree,
direct-input Git-byte verification, implementation provenance, and exact
trace-to-receipt verification.

```text
CANONICAL_SOURCE_COMMIT = 18dcfe902e493cf7e436e431ddf253d955e38a41
CANONICAL_GIT_BYTE_VERIFICATION = PASS
CANONICAL_TRACKED_WORKTREE_CLEAN = PASS
GENERATION_IMPLEMENTATION_PROVENANCE = PASS
EVALUATION_IMPLEMENTATION_PROVENANCE = PASS
GENERATION_TRACE_RECEIPT_VERIFICATION = PASS
```

Canonical output directory:

```text
experiment-harness/results/d1_3a_v1_dev_regression_fix16a_v18/
```

| Artifact | SHA-256 |
|---|---|
| `d1_3a_v1_dev_generation_traces_v18.jsonl` | `4b026b0feffc546870e7b35b59ee2070c0ca8584b52a88733cc239e137a9509b` |
| `d1_3a_v1_dev_generation_receipt_v18.json` | `dbf8cca2c7df39ecb3f7985d2c436337642ed4e5c4a71ce4d1faf9c4be3b930f` |
| `d1_3a_v1_dev_evaluation_rows_v18.jsonl` | `4ca5a4a05f3dc44e879d125a4579135093a504e04d09f35666256970af093729` |
| `d1_3a_v1_dev_summary_v18.json` | `4bb91f3fbed3d88e2d9a086a3cbce843751b94a1e40c01521664dee542625413` |
| `d1_3a_v1_dev_delta_review_fix_v18.md` | `ea6e603287d9c33aae442d75b12663916575593786454e8afeb754a8fca9962b` |

| Metric | v17 | v18 | Delta |
|---|---:|---:|---:|
| Executable semantic success | 13/39 | 13/39 | 0 |
| Known boundary abstention | 6/6 | 6/6 | 0 |
| False abstention | 26 | 26 | 0 |
| Undetected semantic error | 0 | 0 | 0 |
| Previous-success regressions | 0 | 0 | 0 |

The 45 sample IDs, classifications, and selected templates are identical to
v17; `CLASSIFICATION_CHANGED_IDS = []`. The role remains development-only:
`V2_CONSTRUCTED = NO`, `NEO4J_RUN = NO`, `LLM_IMPLEMENTED = NO`.

## Historical immutability

All v1-v17 evidence, the Fix16 checkpoint, the controlled-language boundary,
Fix13/Fix14/Fix15 chronology, and historical D1.2c-v1 artifacts are unchanged.
The Fix16 checkpoint and v17 metrics were not rewritten.

```text
V1_V17_ARTIFACTS_UNCHANGED = YES
FIX16_CHECKPOINT_CHANGED = NO
CONTROLLED_LANGUAGE_BOUNDARY_CHANGED = NO
HISTORICAL_D1_2C_V1_CHANGED = NO
```

## PR handling

Fix16a authorizes one push for this separately scoped correction. After that
push, a corrective reply will be posted to the cardinality review thread; the
three earlier Fix16 replies remain valid. All four threads will be resolved
only after the v18 gates pass, followed by exactly one `@codex review` trigger.
No merge, D1.3b work, held-out v2 construction, Neo4j run, or LLM implementation
is in scope.
