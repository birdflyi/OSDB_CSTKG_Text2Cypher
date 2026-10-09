# D1.3a Review Fix16 Checkpoint

## Scope and disposition

- `PRE_FIX16_HEAD`: `2d7c053121b02a30e61e2f4246bc2dfa5a6a5969`
- Fresh review: `5466862039`
- `COMMIT_CODE`: `344751963bf315654bf518c84926f16c02286938`
- `COMMIT_EVIDENCE_PARENT`: `344751963bf315654bf518c84926f16c02286938`
- `COMMIT_EVIDENCE`: this evidence commit (hash returned in the handoff)

Finding A, additional exclusion paraphrases, is fixed by recording an
`unsupported_exclusion_surface` and failing closed. Finding B, postfix
duplicate-removal wording, is fixed by recording an
`unsupported_postfix_uniqueness_surface` and failing closed. Finding C,
entity-noun cardinality wording, is fixed by recording an
`unsupported_cardinality_surface` when the supported limit grammar does not
consume it. None of these changes expands the frozen executable paraphrase
grammar. Finding D is fixed by making every existing
`d1_3a_v1_dev_regression(?:_|$)` result namespace append-only.

The IR field is `unsupported_explicit_constraints`. Every record contains
`kind`, `reason_code`, `surface_text`, `source_span`, and `provenance`; the
reason code is `UNSUPPORTED_EXPLICIT_NL_CONSTRAINT`. The audit exposes
`requested`, `consumed`, and `unconsumed` entries, and any unconsumed entry
rejects candidate selection with the deterministic reason
`unsupported explicit NL constraint`.

## Regression and safety results

- OUT_OF_GRAMMAR_EXCLUSION_FAILS_CLOSED: PASS
- OUT_OF_GRAMMAR_POSTFIX_UNIQUENESS_FAILS_CLOSED: PASS
- OUT_OF_GRAMMAR_CARDINALITY_FAILS_CLOSED: PASS
- SUPPORTED_NEGATION_REGRESSION: PASS
- SUPPORTED_DISTINCT_REGRESSION: PASS
- SUPPORTED_LIMIT_REGRESSION: PASS
- UNSUPPORTED_EXPLICIT_CONSTRAINT_AUDIT: PASS
- ALL_D1_3A_EVIDENCE_NAMESPACES_APPEND_ONLY: PASS
- ALLOW_OVERWRITE_CANNOT_REWRITE_PROTECTED_EVIDENCE: PASS
- SCRATCH_OVERWRITE_POLICY_REGRESSION: PASS
- QUERY_ID_ROUTING: NO
- HELDOUT_SPECIFIC_PRODUCTION_LITERAL: NO

Targeted Fix16 tests, Fix15 tests, and scope/projection tests: 100 passed in
the final focused run. Full suites passed: `graph-migration/tests` 271 and
`experiment-harness/tests` 52.

## Canonical v17 development regression

The canonical source commit is `344751963bf315654bf518c84926f16c02286938`.
The detached worktree had a clean tracked state, direct-input Git-byte
verification passed, generation and evaluation implementation byte
verification passed, and generation trace/receipt verification passed.

Canonical output directory:

```text
experiment-harness/results/d1_3a_v1_dev_regression_fix16_v17/
```

| Artifact | SHA-256 |
|---|---|
| `d1_3a_v1_dev_generation_traces_v17.jsonl` | `bc76821bc985ae86f2b3d20ab565074fffeeeb68230343402562da4fe08fdfda` |
| `d1_3a_v1_dev_generation_receipt_v17.json` | `a082d1e40eaff0c6580185a532c19a368ddd9e4a0afc9e166efb49339dcaebe7` |
| `d1_3a_v1_dev_evaluation_rows_v17.jsonl` | `4ca5a4a05f3dc44e879d125a4579135093a504e04d09f35666256970af093729` |
| `d1_3a_v1_dev_summary_v17.json` | `ccb6a4e521333d8b0a31c62d14dee9ebeefc27eceea74654d569e4c2412408a9` |
| `d1_3a_v1_dev_delta_review_fix_v17.md` | `9706153f4059b9fb64414b6a227c8f5004ac24ba1518fccd777cb168dc1a0ae9` |

The summary metrics are:

| Metric | v16 | v17 | Delta |
|---|---:|---:|---:|
| Executable semantic success | 13/39 | 13/39 | 0 |
| Known boundary abstention | 6/6 | 6/6 | 0 |
| False abstention | 26 | 26 | 0 |
| Undetected semantic error | 0 | 0 | 0 |
| Previous-success regressions | 0 | 0 | 0 |

`BOUNDARY_ABSTENTION=6/6`, `UNDETECTED_SEMANTIC_ERROR=0`, and
`REGRESSED_PREVIOUS_SUCCESSES=0`. The role remains
`V1_ROLE=DEVELOPMENT_DIAGNOSTIC`; `V2_CONSTRUCTED=NO`, `NEO4J_RUN=NO`, and
`LLM_IMPLEMENTED=NO`.

All v1-v16 artifacts are unchanged. The Fix13 original checkpoint, Fix13
hash-correction addendum, Fix14 checkpoint, Fix15 checkpoint, and historical
D1.2c-v1 artifacts are unchanged. This checkpoint and the controlled-language
boundary document are evidence-only additions; no production code is added
to the evidence commit.

## Post-freeze policy

After Fix16, supported-grammar semantic drops/inversions and provenance or
evidence-integrity defects remain blockers. An out-of-grammar wording that
safely abstains is a non-blocking coverage suggestion and does not trigger a
new D1.3a paraphrase-expansion fix. An out-of-grammar wording that still
executes with weakened or inverted semantics is a firewall blocker. General
arbitrary-English coverage is outside D1.3a.
