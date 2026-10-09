# D1.3a PR #6 Review-Fix 4 Checkpoint

Date: 2026-10-08
PR: #6
Branch: `feat/ch7-d1-3a-scope-projection-ir`
Pre-fix4 HEAD: `cd16858748352e4ae60901250a74dfc6c08c4256`

## Review findings

| Finding | Adjudication | Resolution |
|---|---|---|
| A — checked-in v4 evidence could be overwritten with the development override | VALID_FIXED | Canonical D1.3a result paths are append-only for every version token. |
| B — child template packs erased inherited contracts when fields were absent | VALID_FIXED | Layered loading now inherits absent fields and applies explicit overrides without weakening inherited semantics. |

## Append-only evidence policy

`experiment-harness/results/d1_3a_v1_dev_regression/` is the canonical evidence directory. If any target already exists beneath this directory, generation/evaluation refuses to write it, regardless of version number or `--allow-overwrite-development-artifact`. The override remains available only for existing outputs outside the canonical evidence directory (e.g. synthetic/temp tests). Allocate a new version token for each new canonical result.

Temporary-directory tests proved that existing canonical v1, v4, and v99 targets cannot be overwritten even with the override; a new canonical v5 target can be written once, then is protected; a non-canonical synthetic path can use the explicit override.

An end-to-end generator invocation targeting the existing canonical v4 namespace with `--allow-overwrite-development-artifact` was also refused before writing either output.

## Layered template contract semantics

- For `selection` and `projection_options`, child mappings merge key-wise over the inherited mapping.
- For `scope_slots` and `projection_contract`, an absent child field inherits the parent list; a present list replaces it. An explicit empty list therefore intentionally clears that contract.
- An absent child template-contract entry preserves all inherited fields exactly.
- Layered packs reject duplicate template IDs rather than returning ambiguous candidate contracts.
- Synthetic v6 → v5 → v4 tests confirmed v5 tuple distinct options, projection contracts, and typed scope slots survive a v6 that adds only a new template.
- The three-layer dependency closure is dependency-first and deterministic. Changing only v5 changes its record and the resolved bundle hash while v4 and v6 records remain unchanged.

No production v6 pack was created. Existing production v4/v5 packs and their hashes were not modified.

## Test and acceptance results

- Targeted D1.3a scope/projection, provenance, append-only, layered inheritance, and dependency tests: **55 passed**.
- Full `graph-migration/tests`: **158 passed**.
- Full `experiment-harness/tests`: **25 passed**.
- `git diff --check`: PASS.
- Production scan: `QUERY_ID_ROUTING=NO`; `HELDOUT_SPECIFIC_PRODUCTION_LITERAL=NO`.
- Preserved earlier invariants: multi-scope silent drop fixed; source-noun projection leakage fixed; tuple/item/aggregate DISTINCT separated; transitive template hash fixed; schema provenance fixed; pre-fix rows provenance fixed.

## v5 development regression

Role: `DEVELOPMENT_REGRESSION / NOT_HELDOUT`; v1 is only a development diagnostic. No Neo4j run occurred; held-out v2 was not constructed.

| Metric | Pre-fix4 | v5 | Delta vs pre-fix4 |
|---|---:|---:|---:|
| Executable semantic success | 12/39 | 12/39 | 0 |
| Known boundary abstention | 6/6 | 6/6 | 0 |
| False abstention | 27/39 | 27/39 | 0 |
| Undetected semantic error | 0 | 0 | 0 |
| Previous-success regressions | — | 0 | 0 |

v5 artifact hashes:

| Artifact | SHA-256 |
|---|---|
| `d1_3a_v1_dev_generation_traces_v5.jsonl` | 45a25c183e00b4fee70dd4e4a18f6e045b3252556fe5ff41606379cb8c74539d |
| `d1_3a_v1_dev_generation_receipt_v5.json` | 522ae751da0892ff517ad82bfe1d01ad23d766c57be1c594d160074a9cd68efa |
| `d1_3a_v1_dev_evaluation_rows_v5.jsonl` | 2c611a74d3d513d762a86011ca9f828d101514931c1be2ea64986cef4ebacde5 |
| `d1_3a_v1_dev_summary_v5.json` | aef64614c5eb071ba819376ddde21f5bbdcab3cbc67db8cf8eee45ab7a46dadf |
| `d1_3a_v1_dev_delta_review_fix_v5.md` | 56a0cae66735f85f2ced4b7bba4d20c652660665a01a5350c6599da6dfa3de20 |

Receipt path: `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_generation_receipt_v5.json`
Evaluation summary path: `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_summary_v5.json`

## v1-v4 append-only artifact inventory

These 20 existing artifacts were hashed before the v5 run and re-hashed after it. Every before/after hash matches.

| Artifact | SHA-256 before = after |
|---|---|
+| `d1_3a_v1_dev_delta_review_fix_v2.md` | 56a0cae66735f85f2ced4b7bba4d20c652660665a01a5350c6599da6dfa3de20 |
| `d1_3a_v1_dev_delta_review_fix_v3.md` | 56a0cae66735f85f2ced4b7bba4d20c652660665a01a5350c6599da6dfa3de20 |
| `d1_3a_v1_dev_delta_review_fix_v4.md` | 56a0cae66735f85f2ced4b7bba4d20c652660665a01a5350c6599da6dfa3de20 |
| `d1_3a_v1_dev_delta_vs_frozen_baseline_v1.md` | 6d64d77f48b8db1ff01afc7babb5d08f872950856efa6d5eceb01a9b39a1630d |
| `d1_3a_v1_dev_evaluation_rows_v1.jsonl` | db26ff5cc8e295fe2adf2e29df80ff9e2c2dbd87feb1fc2466c74e0b55d30f8c |
| `d1_3a_v1_dev_evaluation_rows_v2.jsonl` | db26ff5cc8e295fe2adf2e29df80ff9e2c2dbd87feb1fc2466c74e0b55d30f8c |
| `d1_3a_v1_dev_evaluation_rows_v3.jsonl` | 49cb23e45766d33f610545fbeaf5ca7a723dd36f58da3739983015396fb4fcd5 |
| `d1_3a_v1_dev_evaluation_rows_v4.jsonl` | 2c611a74d3d513d762a86011ca9f828d101514931c1be2ea64986cef4ebacde5 |
| `d1_3a_v1_dev_generation_receipt_v1.json` | 4aa1dc8745616f52969cbdbba970c1c69fa5801ad687a9158290b1f4315a45f2 |
| `d1_3a_v1_dev_generation_receipt_v2.json` | 71bd7187b650f9c70c1f763dea7524500ff82cf60d4a1159345b79c39b061d23 |
| `d1_3a_v1_dev_generation_receipt_v3.json` | 3325e7fbb2553218c9ad46748b9000dffd9d9ee0fa78e2ead01ea514505e6c6c |
| `d1_3a_v1_dev_generation_receipt_v4.json` | ba848f727946dffb6acc8733cf0ed69a1941ac176757d5c8c51a33ed849c884b |
| `d1_3a_v1_dev_generation_traces_v1.jsonl` | 2ffda642ad5963ceb10e4d2de86cf6a0f74043ebdb406437cd7e7775554e1c47 |
| `d1_3a_v1_dev_generation_traces_v2.jsonl` | 4877420b8330fc81c8b7ead39b8da8b5cba5fd8937ef8a6d4c0aff91aab5b7a4 |
| `d1_3a_v1_dev_generation_traces_v3.jsonl` | 92ec9df37785cabef155f4efab16e022d6d12f9676f3768ac1e53030f8073e88 |
| `d1_3a_v1_dev_generation_traces_v4.jsonl` | 45a25c183e00b4fee70dd4e4a18f6e045b3252556fe5ff41606379cb8c74539d |
| `d1_3a_v1_dev_summary_v1.json` | ec7406e6f1aeeb716c144e48ba9fe37282b3691dc8f20d6c46a780dd162d2fc9 |
| `d1_3a_v1_dev_summary_v2.json` | 2b5753f4dc69ce0d8b504bc413f81d1d74ed55649acfca9482bc3359adc19179 |
| `d1_3a_v1_dev_summary_v3.json` | dcf71b256f9ac30b9142ba33a2a57225739e56b6fe770768533554ccfccd68d2 |
| `d1_3a_v1_dev_summary_v4.json` | 59e1a0c8f9b53e0d2253428871d0351b331bba0a4bb83146e62aca40685bf3ce |

## Historical D1.2c-v1 inventory

All 16 files in `experiment-harness/results/d1_2c_heldout_v1/` were hashed before and after the v5 run. No path was missing or changed; `HISTORICAL_D1_2C_V1_CHANGED=NO`.

| Artifact | SHA-256 before = after |
|---|---|
+| `d1_2c_first_run_start_receipt_v1.json` | 5c7e4db308849f2f22c95b722176de430669d5b55b0920d3f35ac20c264c45c0 |
| `d1_2c_generation_recovery_receipt_v1.json` | 317f9abefe75e7a2652e7d77210f10aa4bf37cf02db46329a420184a9c306cbf |
| `d1_2c_generation_traces_v1.jsonl` | 9df9e576460f77d4c8d6fa9bd9a6b65b9eece83cd6837ec1e828027ba2604334 |
| `d1_2c_harness_freeze_manifest_v1.json` | ed289db665a89bfbf369b949e7bac723b5ce1ad57706ddec6e171bdcb9aa3070 |
| `d1_2c_trace_id_reconciliation_v1.jsonl` | 30e7e8d0d965d8d645b50b78ce8429275479d3a2ac8380ac330f444c2d3a01a8 |
| `d1_2c_v1_bounded_parser_development_spec_v1.md` | c0f17e5c4a519a853cbc582e0e93187dcef84b7d3675a0acc30139ba77967c2d |
| `d1_2c_v1_false_abstention_root_cause_audit_v1.jsonl` | 7aaff65865cf04117ae52d80d653a3eea27975df13cdec5fa8cb5676111f34e5 |
| `d1_2c_v1_false_abstention_root_cause_summary_v1.md` | 44b068070c034c5c0c08f74ba6b80e0ad362662b60e2433a4a7249726c58cdf5 |
| `d1_2c_v1_incident_adjudication_checkpoint_v1.md` | 410537a12a8a351e7c76ed5ec7cd212610d9bed83c2ccdc3091d2c50ea270a68 |
| `d1_2c_v1_protocol_deviation_v1.md` | 3138d1066289f08492bd8161be194a20a276b1d60e0b9bd6743d5a22773e1fba |
| `d1_2c_v1_recovered_evaluation_reporting_correction_v1.md` | 8d0b67cd8deb399ff8252b52542b4f68951834ce760ae1b70f12448d92a53f67 |
| `d1_2c_v1_recovered_evaluation_rows_v1.jsonl` | 3a9e3e4d0b5a5d1b8fbf9230d987c2713647f6e2d03552335d83d913a6d10ec5 |
| `d1_2c_v1_recovered_evaluation_rows_v2.jsonl` | cc15c59d9a35b1902acc4366b7aead1ca845b39168eb9effe06178a5faee9519 |
| `d1_2c_v1_recovered_evaluation_summary_v1.json` | e719f0f3a5f4b7bde089ed71a026bd935d14411c7885ca2d056b1276c4aa299f |
| `d1_2c_v1_recovered_evaluation_summary_v2.json` | e4b7f27e2a00fb26eeff3124c19cc54a14ed532a18808bb81c3f4663e8c52695 |
| `d1_2c_v1_recovered_failure_analysis_v1.md` | 2fb85a17f1975903e40544090210cc8c180fedf22161364d22eadcab27acfd84 |

## GitHub follow-up

Local QA and the v5 development regression are complete. This checkpoint is written before the follow-up commit and single push; no PR merge is authorized in this task.
