# D1.3a PR #6 Review-Fix 3 Checkpoint

Date: 2026-10-08
PR: #6
Branch: `feat/ch7-d1-3a-scope-projection-ir`
Pre-fix3 HEAD: `01c30289f6aa484d9e116c8fbd7c2b2aa4ba7cd0`

## Review findings

| Finding | Adjudication | Resolution |
|---|---|---|
| A — bare/local `distinct` was promoted to tuple DISTINCT | VALID | Fixed by representing tuple, projection-item, and aggregate-argument DISTINCT independently. |
| B — generation receipt omitted schema provenance | VALID | Fixed; receipt records the exact schema path and SHA-256 used by generation. |
| C — evaluation summary omitted `--pre-fix-rows` provenance | VALID | Fixed; summary records paths and SHA-256 for every direct evaluation input, including the dynamically loaded evaluator. |

## DISTINCT model and synthetic tests

- Tuple/result DISTINCT is inferred only from bounded whole-result cues or an unambiguous leading DISTINCT over a multi-item projection list. A local item cue or aggregate cue alone does not set `projection_distinct`.
- Item-level DISTINCT is attached only to the affected `ProjectionItem`. The mixed actor/resource test confirms the ordinary resource item is not marked distinct.
- Aggregate-argument DISTINCT is represented in `ir.aggregation` with its function, typed field, and `distinct: true`; aggregate coverage requires a matching distinct aggregate expression and field in the selected skeleton.
- Skeleton auditing reads a leading `DISTINCT` immediately after `RETURN` as tuple scope. `collect(DISTINCT x)` remains aggregate-local; `RETURN DISTINCT collect(DISTINCT x)` correctly reports both levels.
- The comprehensive actor aggregation test consumes `collect(DISTINCT a.entity_id)` and `count(DISTINCT pr.entity_id)` without requesting top-level tuple DISTINCT.
- Synthetic checks passed for multi-target tuple DISTINCT, item + aggregate DISTINCT, mixed local/ordinary items, unique combinations, local-scope ambiguity, and skeleton-level scope recognition.

## Provenance

Generation receipt direct inputs:

| Input | Path | SHA-256 |
|---|---|---|
| Queries | `data_real/heldout_v1/heldout_queries_v1.jsonl` | `433b55308edf7806775e855d5c3bfd4c40c4d29602a62291d18e523b8f05fe91` |
| Schema | `data_real/pilot_queries/schema_metadata.yaml` | `d213ff4981c1c93fd581c6095e0710cdb9cfd8dccba15b3bee363962b09c907d` |
| Template pack | `data_real/pilot_queries/independent_template_pack_v5.yaml` | `ff1872b26fabd87ee607d0b62ba1155138c4e5f5eee8698ace8aa341f822132b` |
| Resolved template dependency bundle | v4 + v5 template files | `63a4eadb23a9345bb9097c1f84c000375181a2dd104619930f6dc60fc3e05ddd` |

The generation provenance integration test ran twice with identical queries/templates and a changed synthetic schema copy; the two receipts identified the exact schema path and different content hashes. The checked-in production schema was not modified.

Evaluation summary direct inputs:

| Input | Path | SHA-256 |
|---|---|---|
| Generation traces | `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_generation_traces_v4.jsonl` | `45a25c183e00b4fee70dd4e4a18f6e045b3252556fe5ff41606379cb8c74539d` |
| Gold | `data_real/heldout_v1/heldout_gold_v1.jsonl` | `d04f2ce061327d25357d3b9f6479d54a76608071aba65e80a29aa760ac351be7` |
| Frozen baseline rows | `experiment-harness/results/d1_2c_heldout_v1/d1_2c_v1_recovered_evaluation_rows_v2.jsonl` | `cc15c59d9a35b1902acc4366b7aead1ca845b39168eb9effe06178a5faee9519` |
| Pre-fix rows | `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_evaluation_rows_v1.jsonl` | `db26ff5cc8e295fe2adf2e29df80ff9e2c2dbd87feb1fc2466c74e0b55d30f8c` |
| Dynamically loaded evaluator | `experiment-harness/d1_2c/evaluate_heldout_v1.py` | `e02639f85278819c895eea2426706cbfe82187c3b88f50791e65f5fb5db7ddba` |

The evaluation provenance integration test changed synthetic pre-fix rows and verified the recorded hash changed; it also ran the evaluator with synthetic inputs and checked all five recorded input paths/hashes against the exact files consumed.

## Development regression and artifacts

Role: `DEVELOPMENT_REGRESSION / NOT_HELDOUT`. No Neo4j run occurred; no held-out v2 was constructed.

| Metric | Pre-fix3 | v4 | Delta vs pre-fix3 |
|---|---:|---:|---:|
| Executable semantic success | 12/39 | 12/39 | 0 |
| Known boundary abstention | 6/6 | 6/6 | 0 |
| False abstention | 27/39 | 27/39 | 0 |
| Undetected semantic error | 0 | 0 | 0 |
| Previously successful executable regressions | — | 0 | 0 |

Delta vs frozen baseline: executable success `+8` (4 to 12), known boundary abstention `0`, false abstention `-8` (35 to 27), undetected semantic errors `0`.

v4 artifact hashes:

| Artifact | SHA-256 |
|---|---|
| `d1_3a_v1_dev_generation_traces_v4.jsonl` | `45a25c183e00b4fee70dd4e4a18f6e045b3252556fe5ff41606379cb8c74539d` |
| `d1_3a_v1_dev_generation_receipt_v4.json` | `ba848f727946dffb6acc8733cf0ed69a1941ac176757d5c8c51a33ed849c884b` |
| `d1_3a_v1_dev_evaluation_rows_v4.jsonl` | `2c611a74d3d513d762a86011ca9f828d101514931c1be2ea64986cef4ebacde5` |
| `d1_3a_v1_dev_summary_v4.json` | `59e1a0c8f9b53e0d2253428871d0351b331bba0a4bb83146e62aca40685bf3ce` |
| `d1_3a_v1_dev_delta_review_fix_v4.md` | `56a0cae66735f85f2ced4b7bba4d20c652660665a01a5350c6599da6dfa3de20` |

## Historical artifact integrity

Every v1/v2/v3 artifact below was hashed before v4 generation and re-hashed after evaluation; all before/after hashes are identical.

| Artifact | SHA-256 before = after |
|---|---|
| `d1_3a_v1_dev_delta_review_fix_v2.md` | `56a0cae66735f85f2ced4b7bba4d20c652660665a01a5350c6599da6dfa3de20` |
| `d1_3a_v1_dev_delta_review_fix_v3.md` | `56a0cae66735f85f2ced4b7bba4d20c652660665a01a5350c6599da6dfa3de20` |
| `d1_3a_v1_dev_delta_vs_frozen_baseline_v1.md` | `6d64d77f48b8db1ff01afc7babb5d08f872950856efa6d5eceb01a9b39a1630d` |
| `d1_3a_v1_dev_evaluation_rows_v1.jsonl` | `db26ff5cc8e295fe2adf2e29df80ff9e2c2dbd87feb1fc2466c74e0b55d30f8c` |
| `d1_3a_v1_dev_evaluation_rows_v2.jsonl` | `db26ff5cc8e295fe2adf2e29df80ff9e2c2dbd87feb1fc2466c74e0b55d30f8c` |
| `d1_3a_v1_dev_evaluation_rows_v3.jsonl` | `49cb23e45766d33f610545fbeaf5ca7a723dd36f58da3739983015396fb4fcd5` |
| `d1_3a_v1_dev_generation_receipt_v1.json` | `4aa1dc8745616f52969cbdbba970c1c69fa5801ad687a9158290b1f4315a45f2` |
| `d1_3a_v1_dev_generation_receipt_v2.json` | `71bd7187b650f9c70c1f763dea7524500ff82cf60d4a1159345b79c39b061d23` |
| `d1_3a_v1_dev_generation_receipt_v3.json` | `3325e7fbb2553218c9ad46748b9000dffd9d9ee0fa78e2ead01ea514505e6c6c` |
| `d1_3a_v1_dev_generation_traces_v1.jsonl` | `2ffda642ad5963ceb10e4d2de86cf6a0f74043ebdb406437cd7e7775554e1c47` |
| `d1_3a_v1_dev_generation_traces_v2.jsonl` | `4877420b8330fc81c8b7ead39b8da8b5cba5fd8937ef8a6d4c0aff91aab5b7a4` |
| `d1_3a_v1_dev_generation_traces_v3.jsonl` | `92ec9df37785cabef155f4efab16e022d6d12f9676f3768ac1e53030f8073e88` |
| `d1_3a_v1_dev_summary_v1.json` | `ec7406e6f1aeeb716c144e48ba9fe37282b3691dc8f20d6c46a780dd162d2fc9` |
| `d1_3a_v1_dev_summary_v2.json` | `2b5753f4dc69ce0d8b504bc413f81d1d74ed55649acfca9482bc3359adc19179` |
| `d1_3a_v1_dev_summary_v3.json` | `dcf71b256f9ac30b9142ba33a2a57225739e56b6fe770768533554ccfccd68d2` |

All 17 files in `experiment-harness/results/d1_2c_heldout_v1/` were also SHA-256 inventoried before and after the run; no file changed. `HISTORICAL_D1_2C_V1_CHANGED=NO`.

## Acceptance gates

- D1.3a scope/projection and fix2/fix3 provenance tests: `46 passed`.
- Full `graph-migration/tests`: `152 passed`.
- Full `experiment-harness/tests` (including D1.2c harness tests): `22 passed`.
- `git diff --check`: PASS.
- Production scan for held-out literals and request-ID routing: PASS; `QUERY_ID_ROUTING=NO`; `HELDOUT_SPECIFIC_PRODUCTION_LITERAL=NO`.
- `V1_ROLE=DEVELOPMENT_DIAGNOSTIC`; `V2_CONSTRUCTED=NO`; `NEO4J_RUN=NO`.
- `V1_ARTIFACTS_CHANGED=NO`; `V2_ARTIFACTS_CHANGED=NO`; `V3_ARTIFACTS_CHANGED=NO`.

## GitHub follow-up

Local implementation, v4 artifacts, and QA are complete. PR thread reply/resolution and the single allowed push are still pending at checkpoint creation; no merge is authorized in this task.
