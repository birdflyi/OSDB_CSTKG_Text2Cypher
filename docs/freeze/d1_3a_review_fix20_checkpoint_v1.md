# D1.3a Review Fix20 Checkpoint v1

Date: 2026-10-10
PR: #6
PRE_FIX20_HEAD: `d70e8d5ac1266805597d72ba27850fef4ca23c00`
Findings addressed: `4231293176`, `4231293184`

## Commits and protocol

- `COMMIT_CODE_FIX20`: `607830cf2c0e0e9c4f7eb17bb8e1b0dd267756cb`
- `COMMIT_EVIDENCE_FIX20`: created after this checkpoint is staged
- evidence parent must equal `607830cf2c0e0e9c4f7eb17bb8e1b0dd267756cb`
- one non-force push is authorized after both commits; no intermediate push
- no historical v1-v21 evidence, checkpoint, or erratum was edited

## Fix20 implementation

The generation receipt verifier now binds canonical schema and template identities,
including the ordered transitive template dependency closure and resolved bundle
SHA-256. Canonical evaluation derives these identities from stable repository
paths, checks exact Git bytes and implementation provenance, and fails closed on
path, hash, dependency, or bundle mismatches before classification/output.

The controlled front-end now recognizes local typed-prefix negation with optional
determiners (`the`, `this`, `that`, `these`, `those`) and abstains fail-closed;
it does not render a negative Cypher operator or invert the constraint into a
positive `STARTS_WITH` query. Existing positive forms and unrelated-clause
controls remain covered.

## Canonical v22 evidence

Source commit: `607830cf2c0e0e9c4f7eb17bb8e1b0dd267756cb`.
Generation/evaluation ran in a detached clean worktree with
`core.autocrlf=false`. Git-byte input and runtime implementation provenance:
PASS. Role is `DEVELOPMENT_REGRESSION / NOT_HELDOUT`; no Neo4j runtime or held-out
v2 was run.

| Artifact | SHA-256 |
|---|---|
| `d1_3a_v1_dev_generation_traces_v22.jsonl` | `4b026b0feffc546870e7b35b59ee2070c0ca8584b52a88733cc239e137a9509b` |
| `d1_3a_v1_dev_generation_receipt_v22.json` | `d6015874e8e2d6fa45b0467a1f1c850b9995ee1c4343e840c4ba083b3238f593` |
| `d1_3a_v1_dev_evaluation_rows_v22.jsonl` | `4ca5a4a05f3dc44e879d125a4579135093a504e04d09f35666256970af093729` |
| `d1_3a_v1_dev_summary_v22.json` | `2a396a4b2fc8d944ed0f411283dc6d80cf1d3cd4af268f82a3bf32b0346c8b08` |
| `d1_3a_v1_dev_delta_review_fix_v22.md` | `ea6e603287d9c33aae442d75b12663916575593786454e8afeb754a8fca9962b` |

## Canonical contract and receipt gates

- queries: `data_real/heldout_v1/heldout_queries_v1.jsonl`, SHA-256
  `433b55308edf7806775e855d5c3bfd4c40c4d29602a62291d18e523b8f05fe91`
- schema: `data_real/pilot_queries/schema_metadata.yaml`, SHA-256
  `d3e0ee543e603a3b545c406c4fd3b7275c779c9d4c64617b2edaa0c7a2f2d201`
- template pack: `data_real/pilot_queries/independent_template_pack_v5.yaml`,
  SHA-256 `ff1872b26fabd87ee607d0b62ba1155138c4e5f5eee8698ace8aa341f822132b`
- ordered dependency closure:
  1. `data_real/pilot_queries/independent_template_pack_v4.yaml` —
     `c57d05cb42989f0a406a3e48a4fb6f3dda822121b0db17bb1f72e5be91f66d8f`
  2. `data_real/pilot_queries/independent_template_pack_v5.yaml` —
     `ff1872b26fabd87ee607d0b62ba1155138c4e5f5eee8698ace8aa341f822132b`
- resolved template bundle SHA-256:
  `a587c26385db6ae4d75621281126425b1b339bbe67c45e6a059c9c9349fde6dd`
- canonical generation contract binding: PASS
- trace receipt verification: PASS
- expected query text binding: `45/45`, PASS
- source commit/path/SHA and Git-byte provenance: PASS

## Metrics and classification invariants

- `N=45`, `N_EXECUTABLE=39`, `N_ABSTENTION=6`
- executable semantic success: `13/39`
- known-boundary abstention: `6/6`
- false abstention: `26`
- undetected semantic error: `0`
- v21→v22 changed classification IDs: `[]`
- previous successful executable regressions: `0`

## Tests and environmental exception

- Fix20 focused tests: `4 passed`
- graph-migration full suite: `293 passed`, `1 failed`; report status
  `PASS_WITH_PREEXISTING_FIXTURE_BLOCKER`
- exact failure: `graph-migration/tests/test_real_csv_preprocess.py::test_preprocess_osdb_csv_adds_augmented_columns_and_fine_grained`
  raises `FileNotFoundError` for
  `graph-migration/fixtures/real_pilot_redis/mini_sample.csv`
- the fixture exists only as a local ignored file (`.gitignore:276`) and is not
  tracked; the same failure was reproduced in a clean `d70e8d5` detached
  worktree. It was not added or used as repository evidence.
- experiment-harness full suite: `69 passed, 3 skipped`
- Windows symlink tests: `3 skipped`
- `git diff --check`: PASS
- anti-overfitting scan: no reviewer example IDs or review-thread literals in
  production code

## Non-scope

No LLM/DeepSeek, Neo4j runtime, held-out v2, D1.3b, manuscript edit, tag,
merge, or query-ID routing was performed. The ignored fixture was not committed.
