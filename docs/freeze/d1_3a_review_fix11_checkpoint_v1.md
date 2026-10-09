# D1.3a PR #6 Review-Fix 11 Checkpoint

Date: 2026-10-09
PR: #6
Pre-fix11 head: `53cdeec01f441e29666e4c44b2a9926dc58e51b8`

## Review findings and bounded fixes

- Finding A (`discussion_r4222352153`): except-style inline prefix exclusions
  now produce `NOT_STARTS_WITH` with provenance
  `negated_typed_prefix_scope_from_nl`. Audit exposes
  `UNSUPPORTED_NEGATED_TYPED_SCOPE_OPERATOR`; generation fails closed because
  no negative execution template exists. The bounded grammar recognizes
  `except [those|issues|pull requests] whose IDs/identifiers start/begin with`
  and equivalent `but not` forms. It does not use global `except/not` tests;
  unrelated excluded comments and their positive prefix clauses remain
  positive `STARTS_WITH`.
- Finding B (`discussion_r4222352169`): ID pronouns no longer bind to the
  nearest arbitrary entity noun. Resolution first honors an already-present
  explicit output projection role; otherwise it may use one unique
  output-cued noun that is not a source-anchor noun. Source nouns and embedded
  nouns inside composite anchors are never implicit antecedents. With no
  unique eligible role, no role is invented. `who/whoever` establishes Actor
  before pronoun resolution. An explicitly requested source projection such
  as `return the issue ID` remains represented.

No general coreference or Boolean logic was implemented. No negative Cypher
template, Neo4j run, held-out v2, D1.3b work, or historical artifact rewrite was
performed.

## Commits

```text
COMMIT_CODE = 6a1d9894f82b4e0457574a51c9fd407274fa567d
COMMIT_EVIDENCE_PARENT = 6a1d9894f82b4e0457574a51c9fd407274fa567d
COMMIT_EVIDENCE = this checkpoint's containing commit (recorded in Git history)
```

The code commit contains production code and Fix11 tests only. The evidence
commit contains five v12 development artifacts and this checkpoint, with no
production-code changes between the two commits.

## Tests and safety

- New Fix11 tests: 7 passed.
- Combined D1.3a scope/projection + Fix9/Fix10/Fix11 targeted suite: 76 passed.
- Full `graph-migration/tests`: 204 passed.
- Full `experiment-harness/tests`: 36 passed.
- `git diff --check` passed for the Fix11 code/test paths against base
  `98d860a6601ed24fa8d6e8f017bf98ddca79548b` and for the complete Fix11
  changeset. The full historical base-to-HEAD check also reports two
  pre-existing Markdown hard-break trailing spaces in the immutable
  `d1_3a_review_fix10_checkpoint_v1.md`; that prior checkpoint was not edited.
- Production anti-overfitting scan passed:
  `QUERY_ID_ROUTING = NO`; `HELDOUT_SPECIFIC_PRODUCTION_LITERAL = NO`.

Required bounded behavior passed:

```text
EXCEPT_STYLE_NEGATED_SCOPE = PASS
BUT_NOT_STYLE_NEGATED_SCOPE = PASS
NEGATION_FALSE_POSITIVE_CONTROLS = PASS
NEGATED_SCOPE_FAIL_CLOSED = PASS
POSITIVE_PREFIX_SCOPE_REGRESSION = PASS
SOURCE_ANCHOR_PRONOUN_EXCLUSION = PASS
EXPLICIT_OUTPUT_ROLE_PRONOUN_PRIORITY = PASS
WHO_ACTOR_PRONOUN_BINDING = PASS
COMPOSITE_SOURCE_PRONOUN_REGRESSION = PASS
AMBIGUOUS_PRONOUN_NO_GUESS = PASS
EXPLICIT_SOURCE_PROJECTION_STILL_VISIBLE = PASS
```

## Canonical exact-byte v12 run

Generation and evaluation ran in a detached worktree with `core.autocrlf=false`
at exactly `COMMIT_CODE`. The first attempted invocation from the main worktree
was rejected by the canonical Git-byte gate and produced no accepted run; the
successful generation/evaluation were run from the exact detached worktree.

```text
canonical_source_commit = 6a1d9894f82b4e0457574a51c9fd407274fa567d
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

The v12 generation receipt records five tracked direct inputs and eight
generation runtime implementation files; the v12 evaluation summary records
four evaluation runtime implementation files. Every provenance record has
`bytes_match_git_blob = true`.

## Metrics and classification delta

| Metric | v11 | v12 | Delta v12-v11 |
|---|---:|---:|---:|
| Executable semantic success | 12/39 | 13/39 | +1 |
| Known boundary abstention | 6/6 | 6/6 | 0 |
| False abstention | 27/39 | 26/39 | -1 |
| Undetected semantic error | 0 | 0 | 0 |
| Previous-success regressions | 0 | 0 | 0 |

Exactly one classification changed:

```text
ho_q_l1_03_V1: FALSE_ABSTENTION -> SUCCESS
```

Reason: the query asks for unknown-type referenced objects and then says
“Return their IDs.” In v11 the pronoun reused the source PullRequest noun,
adding an unrequested PullRequest projection beside UnknownObject and causing
template abstention. v12 keeps the explicit UnknownObject output role and
excludes the source-anchor PullRequest from implicit pronoun resolution; the
existing `indv4_reference_object` template then renders the requested
UnknownObject IDs. This was an incidental bounded source-leakage correction,
not development-sentence tuning. The result remains development-only and
static-only.

```text
V12_DEV_METRICS =
{EXECUTABLE_SEMANTIC_SUCCESS: 13, N_EXECUTABLE: 39,
 KNOWN_BOUNDARY_ABSTENTION: 6, N_ABSTENTION: 6,
 FALSE_ABSTENTION: 26, UNDETECTED_SEMANTIC_ERROR: 0}

DELTA_VS_V11 =
{EXECUTABLE_SEMANTIC_SUCCESS: +1, KNOWN_BOUNDARY_ABSTENTION: 0,
 FALSE_ABSTENTION: -1, UNDETECTED_SEMANTIC_ERROR: 0}

BOUNDARY_ABSTENTION = 6 / 6
UNDETECTED_SEMANTIC_ERROR = 0
REGRESSED_PREVIOUS_SUCCESSES = 0
V1_ROLE = DEVELOPMENT_DIAGNOSTIC
V2_CONSTRUCTED = NO
NEO4J_RUN = NO
```

## v12 artifacts

| Artifact | SHA-256 |
|---|---|
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_generation_traces_v12.jsonl` | `df4ef1e8fd1d8cfae3ad4518341e74287c4ce8390cbe4156ed6edbeda0b90ae8` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_generation_receipt_v12.json` | `d1210a1c491a7f41ee42eef597b1400551fbdff13df0fe961476b33f3ad1d00b` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_evaluation_rows_v12.jsonl` | `0b99ab37049981fc7df13dcf69d5c2d601843038c3cc78aac85db7e5f40fd8e1` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_summary_v12.json` | `08b5d8f2487a4185793affa37ef08326b72721c08fb4586a821ebcb6878d7759` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_delta_review_fix_v12.md` | `ea6e603287d9c33aae442d75b12663916575593786454e8afeb754a8fca9962b` |

## Historical immutability and interpretation

All D1.3a v1-v11 artifacts and all historical D1.2c-v1 artifacts remained
unchanged. The evidence commit adds only the v12 artifacts and this checkpoint.

```text
V1_V11_ARTIFACTS_UNCHANGED = YES
HISTORICAL_D1_2C_V1_CHANGED = NO
QUERY_ID_ROUTING = NO
HELDOUT_SPECIFIC_PRODUCTION_LITERAL = NO
```

This checkpoint records development-only static evidence. It does not promote
v1 to held-out evidence, construct held-out v2, or claim Neo4j runtime
correctness.
