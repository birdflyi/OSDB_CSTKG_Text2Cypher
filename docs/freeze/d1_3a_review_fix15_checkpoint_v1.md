# D1.3a PR #6 Review-Fix 15 Checkpoint

Date: 2026-10-09
PR: #6

## Commits

```text
PRE_FIX15_HEAD = b604f80684a9b2b423e9b41f1cbb4846c81f70e6
COMMIT_CODE = c142a4dd424e8ee09d21417123f0ab19907e2b1c
COMMIT_EVIDENCE = this checkpoint's containing commit (recorded in Git history)
COMMIT_EVIDENCE_PARENT = c142a4dd424e8ee09d21417123f0ab19907e2b1c
```

`COMMIT_CODE` contains only the production implementation and Fix15 tests. The evidence commit contains the five canonical v16 artifacts and this checkpoint, with no production-code changes.

## Findings fixed

### Finding A — DISTINCT cue attribution

Tuple/row uniqueness (`ControlledQueryIR.projection_distinct`) and item-local uniqueness (`ProjectionItem.distinct`) remain separate. Each item now retains the source spans of all bounded lexical `distinct`/`unique` cues. A leading tuple cue is removed from the first item's local flags only when it is the sole cue and its exact source span is the same leading `distinct` token reinterpreted as tuple DISTINCT. Positional proximity alone is not used.

The regression suite covers tuple-only DISTINCT, independent uniqueness on either tuple item, unique repo IDs coexisting with a leading tuple cue, local IDs-of-entity DISTINCT, single-column DISTINCT, and aggregate-local DISTINCT. An item-local requirement not consumable by the selected tuple-only contract remains visible as unconsumed and fails closed.

### Finding B — compact pre-token negation

Bounded prefix phrases `without|excluding|exclude|omitting|omit|except|but not prefix <typed token>` now produce `NOT_STARTS_WITH` with provenance `negated_typed_prefix_scope_from_nl`. Both Issue and PullRequest typed prefixes are covered. Since negative prefix execution is unsupported, coverage fails closed with `UNSUPPORTED_NEGATED_TYPED_SCOPE_OPERATOR`; no executable negative Cypher was introduced.

Previously supported post-token negation forms remain negative. Negations of unrelated nouns such as comments remain positive controls, as do positive `with prefix` forms.

### Finding C — attributive property noun suppression

For the bounded property family `domain(s)`, `registrable domain(s)`, and `site domain(s)`, directly attributive `resource(s)` / `external resource(s)` no longer causes generic entity-ID inference in a property-only request. Explicit ID/identifier cues and coordination remain independent: `external resource IDs and domains` projects both columns, as does coordinated `external resources and domains`. `unique external resource domains` marks only the domain item unique. `count distinct domains` remains aggregate-local.

## Tests and acceptance

```text
NEW_FIX15_TARGETED_TESTS = 7 passed
FIX15_DISTINCT_NEGATION_SCOPE_PROJECTION_TARGETED_SUITE = 109 passed
FULL_GRAPH_MIGRATION_TESTS = 254 passed
FULL_EXPERIMENT_HARNESS_TESTS = 46 passed
NEW_FIX15_DIFF_CHECK = PASS
QUERY_ID_ROUTING = NO
HELDOUT_SPECIFIC_PRODUCTION_LITERAL = NO
```

The production anti-overfitting scan found no held-out IDs, held-out dataset routing, or review-thread identifiers in the production runner, repair, or validator paths.

## Canonical v16 development regression

The run was generated and evaluated in a detached worktree at exact `COMMIT_CODE`, with `core.autocrlf=false`, a clean tracked worktree, direct-input Git-byte verification, generation/evaluation implementation provenance, and exact trace-to-receipt verification.

```text
CANONICAL_SOURCE_COMMIT = c142a4dd424e8ee09d21417123f0ab19907e2b1c
CANONICAL_GIT_BYTE_VERIFICATION = PASS
CANONICAL_TRACKED_WORKTREE_CLEAN = true
GENERATION_IMPLEMENTATION_PROVENANCE = PASS (8/8)
EVALUATION_IMPLEMENTATION_PROVENANCE = PASS (5/5)
GENERATION_TRACE_RECEIPT_VERIFICATION = PASS
EVALUATION_ROLE = DEVELOPMENT_REGRESSION
HELDOUT_ROLE = NOT_HELDOUT
EVALUATION_ANNOTATIONS_LOADED = false
GOLD_OR_REFERENCE_CYPHER_LOADED = false
V2_CONSTRUCTED = NO
NEO4J_RUN = NO
```

Exactly five v16 artifacts are committed under:

```text
experiment-harness/results/d1_3a_v1_dev_regression_fix15_v16/
```

| Artifact | SHA-256 |
|---|---|
| `d1_3a_v1_dev_generation_traces_v16.jsonl` | `9506d37742231653cdcaef42ebcabd85cb924eb03aa43f7562be2bfc533343f8` |
| `d1_3a_v1_dev_generation_receipt_v16.json` | `0221a76ac1c60f12e2c9e79df80218739dcf74a08b70d82ebf33a6493ab65430` |
| `d1_3a_v1_dev_evaluation_rows_v16.jsonl` | `4ca5a4a05f3dc44e879d125a4579135093a504e04d09f35666256970af093729` |
| `d1_3a_v1_dev_summary_v16.json` | `bf6a0b722dbf3ff88d61115f083d0b2cccf031a0b2a2682c4dd3ceb09ea233ae` |
| `d1_3a_v1_dev_delta_review_fix_v16.md` | `9706153f4059b9fb64414b6a227c8f5004ac24ba1518fccd777cb168dc1a0ae9` |

The receipt authenticates the trace hash above. The evaluation summary records the same canonical source commit, raw-byte provenance for direct inputs and runtime implementation, a clean tracked worktree, and `generation_trace_receipt_verification = PASS`.

## Metrics and interpretation

| Metric | v15 | accepted v16 | Delta v16-v15 |
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

The unchanged development metrics are expected: the Fix15 behaviors are parser invariants not represented by new held-out-v1 cases. The run is static-only development diagnostic evidence, not generalization or runtime evidence.

## Historical immutability

The code commit changes only the production runner and the new Fix15 test file. The evidence commit adds only the five v16 artifacts and this checkpoint. No historical D1.3a v1-v15 accepted result artifact is rewritten. The Fix13 original checkpoint, Fix13 hash-correction addendum, Fix14 checkpoint, and historical D1.2c-v1 artifacts remain unchanged.

```text
V1_V15_ARTIFACTS_UNCHANGED = YES
FIX13_CHECKPOINT_FILE_CHANGED = NO
FIX13_HASH_CORRECTION_ADDENDUM_CHANGED = NO
FIX14_CHECKPOINT_CHANGED = NO
HISTORICAL_D1_2C_V1_CHANGED = NO
```
