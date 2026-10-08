# D1.3a PR #6 Review-Fix 2 Checkpoint

Status: `LOCAL_QA_PASS`

## Scope and adjudication

- PR: `#6` (`feat(ch7): add typed prefix scope and role-aware projection`).
- Branch: `feat/ch7-d1-3a-scope-projection-ir`.
- Pre-fix2 head: `73602ab771ed03d5cc6465a11730f806fdfe8137`.
- Finding 1 (artifact overwrite/default): `VALID_FIXED`.
- Finding 2a (tuple-level DISTINCT): `VALID_FIXED`.
- Finding 2b (transitive template dependency hash): `VALID_FIXED`.
- No merge, held-out v2 construction, or Neo4j run was performed.

## Implementation

Changed source and tests:

- `experiment-harness/d1_3a/artifact_safety.py` (new): defaults artifact
  namespace to v2, validates version tokens, refuses existing output paths,
  and protects v1/v2 from overwrite even when the development override is
  supplied.
- `experiment-harness/d1_3a/generate_v1_dev_regression.py` and
  `experiment-harness/d1_3a/evaluate_v1_dev_regression.py`: expose parser
  builders, default to v2, and apply no-clobber checks before writing.
- `graph-migration/runners/independent_controlled_pipeline.py`: represents
  whole-result DISTINCT as `ControlledQueryIR.projection_distinct`, separate
  from projection-item properties; audits requested/default tuple DISTINCT.
- `data_real/pilot_queries/independent_template_pack_v5.yaml`: declares
  tuple-level DISTINCT and its family-level entailment for
  `indv4_actor_multi_target_reference`, `indv4_issue_comment_actor`, and
  `indv4_review_reference`. The `collect(DISTINCT ...)` actor contract remains
  item-local for `indv4_comprehensive_external_actor_aggregation`.
- `graph-migration/tests/test_d1_3a_scope_projection.py`: synthetic positive
  and negative tuple-DISTINCT cases, including order and single-column cases.
- `experiment-harness/d1_3a/template_provenance.py` (new): recursively hashes
  `extends` dependencies, rejects cycles/missing/out-of-root dependencies,
  and computes a deterministic bundle hash.
- `experiment-harness/tests/test_d1_3a_review_fix2_provenance.py` (new): seven
  tests for defaults, no-clobber, dependency closure, cycles, and canonical
  hash construction. It loads CLI parser defaults in isolated subprocesses to
  avoid polluting the legacy harness package import paths.

The D1.3a v5 contract requires explicit tuple-DISTINCT entailment when a
template supplies top-level `RETURN DISTINCT` without a request. Pre-v5
skeleton-only packs retain their frozen implicit-skeleton compatibility path;
this keeps the historical independent-path and D1.1 closure regressions
unchanged. An explicit v5 projection contract without the entailment is
rejected.

## Artifact namespace and immutability

- Generator and evaluator default artifact version: `v2` (not v1).
- Existing output: fail closed unless a development-only override is given;
  v1 and v2 remain protected even with that override.
- This follow-up explicitly generated only v3 artifacts:
  `d1_3a_v1_dev_generation_traces_v3.jsonl`,
  `d1_3a_v1_dev_generation_receipt_v3.json`,
  `d1_3a_v1_dev_evaluation_rows_v3.jsonl`,
  `d1_3a_v1_dev_summary_v3.json`, and
  `d1_3a_v1_dev_delta_review_fix_v3.md`.
- All existing v1/v2 D1.3a development artifact hashes matched their
  pre-run values (`V1_ARTIFACTS_UNCHANGED=YES`,
  `V2_ARTIFACTS_UNCHANGED=YES`):

| Artifact | SHA-256 |
|---|---|
| `d1_3a_v1_dev_delta_review_fix_v2.md` | `56a0cae66735f85f2ced4b7bba4d20c652660665a01a5350c6599da6dfa3de20` |
| `d1_3a_v1_dev_delta_vs_frozen_baseline_v1.md` | `6d64d77f48b8db1ff01afc7babb5d08f872950856efa6d5eceb01a9b39a1630d` |
| `d1_3a_v1_dev_evaluation_rows_v1.jsonl` | `db26ff5cc8e295fe2adf2e29df80ff9e2c2dbd87feb1fc2466c74e0b55d30f8c` |
| `d1_3a_v1_dev_evaluation_rows_v2.jsonl` | `db26ff5cc8e295fe2adf2e29df80ff9e2c2dbd87feb1fc2466c74e0b55d30f8c` |
| `d1_3a_v1_dev_generation_receipt_v1.json` | `4aa1dc8745616f52969cbdba970c1c69fa5801ad687a9158290b1f4315a45f2` |
| `d1_3a_v1_dev_generation_receipt_v2.json` | `71bd7187b650f9c70c1f763dea7524500ff82cf60d4a1159345b79c39b061d23` |
| `d1_3a_v1_dev_generation_traces_v1.jsonl` | `2ffda642ad5963ceb10e4d2de86cf6a0f74043ebdb406437cd7e7775554e1c47` |
| `d1_3a_v1_dev_generation_traces_v2.jsonl` | `4877420b8330fc81c8b7ead39b8da8b5cba5fd8937ef8a6d4c0aff91aab5b7a4` |
| `d1_3a_v1_dev_summary_v1.json` | `ec7406e6f1aeeb716c144e48ba9fe37282b3691dc8f20d6c46a780dd162d2fc9` |
| `d1_3a_v1_dev_summary_v2.json` | `2b5753f4dc69ce0d8b504bc413f81d1d74ed55649acfca9482bc3359adc19179` |

## Template dependency receipt

Dependency records are ordered dependency-first and contain project-root-
relative POSIX paths plus raw-file SHA-256 values. The resolved bundle digest
is SHA-256 over the UTF-8 bytes of compact JSON (`ensure_ascii=false`, sorted
object keys, separators `,` and `:`) for that ordered record list. Thus a base
pack byte change changes the bundle hash even if the top-level facade is
unchanged.

- Top-level v5 facade SHA-256:
  `ff1872b26fabd87ee607d0b62ba1155138c4e5f5eee8698ace8aa341f822132b`.
- v4 dependency SHA-256:
  `7e38cb8ccac24cd93695b25b131cbe0e188b93dfba89316eb032288a6e5c47cd`.
- Resolved template bundle SHA-256:
  `63a4eadb23a9345bb9097c1f84c000375181a2dd104619930f6dc60fc3e05ddd`.
- Stable closure, transitive base-change sensitivity, cycle detection, missing
  dependency, and canonicalization tests: pass.

## Development regression

Role: `DEVELOPMENT_REGRESSION / NOT_HELDOUT` (`V1_ROLE=DEVELOPMENT_DIAGNOSTIC`).

| Metric | Pre-fix2 | v3 post-fix2 | Delta |
|---|---:|---:|---:|
| Executable semantic success | 12/39 | 12/39 | 0 |
| Known boundary abstention | 6/6 | 6/6 | 0 |
| False abstention | 27/39 | 27/39 | 0 |
| Undetected semantic error | 0 | 0 | 0 |
| Previously successful executable regressions | 0 | 0 | 0 |

- v3 generation trace SHA-256:
  `92ec9df37785cabef155f4efab16e022d6d12f9676f3768ac1e53030f8073e88`.
- v3 evaluation rows SHA-256:
  `49cb23e45766d33f610545fbeaf5ca7a723dd36f58da3739983015396fb4fcd5`.
- `indv4_actor_multi_target_reference`: tuple DISTINCT is represented at
  projection level, its explicit tuple contract accepts a matching distinct
  request, and removing its entailment rejects an unmarked distinct result.
- `V2_CONSTRUCTED=NO`; v2 remains outside scope.

## Verification

| Suite / gate | Result |
|---|---:|
| D1.3a scope/projection tests | 29 passed |
| Independent-path tests | 113 passed |
| Full graph-migration tests | 147 passed |
| D1.2c harness tests | 3 passed |
| experiment-harness tests (including 7 new provenance tests) | 17 passed |
| Production scan for held-out literals and request-ID routing | 1 passed |
| `git diff --check` | passed |

- `MULTI_SCOPE_SILENT_DROP=FIXED`.
- `SOURCE_NOUN_PROJECTION_LEAKAGE=FIXED`.
- `PREFIX_V1_ARTIFACTS_CHANGED=NO`; `PREFIX_V2_ARTIFACTS_CHANGED=NO`.
- `HISTORICAL_D1_2C_V1_CHANGED=NO`.
- `QUERY_ID_ROUTING=NO`.
- `HELDOUT_SPECIFIC_PRODUCTION_LITERAL=NO`.
- `V2_CONSTRUCTED=NO`.
- `NEO4J_RUN=NO`.

## PR follow-up boundary

Local acceptance is complete. The next authorized actions are one follow-up
commit and exactly one push to the existing PR #6 branch, then replies and
resolutions for the three review threads (P1 artifact overwrite, P2a tuple
DISTINCT, P2b dependency closure) and one fresh `@codex review`. Do not merge,
construct held-out v2, or start D1.3b.
