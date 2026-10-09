# D1.3a Review Fix18 Checkpoint v1

Date: 2026-10-09
PR: #6
PRE_FIX18_HEAD: `94144cda5d16c3826f7ed322c07ace3cfc840b8d`
Fresh review: `5469907382`

## Findings and dispositions

1. Trailing typed exclusion (`but not`, `except`, `excluding`, and bounded
   variants) is now represented as `unsupported_exclusion_surface` and fails
   closed. No general Boolean negation or `NOT_STARTS_WITH` execution was
   added.
2. The canonical evaluator authority is
   `data_real/heldout_v1/heldout_queries_v1.jsonl`. v20 authenticates receipt
   path/hash and compares every exact `(heldout_id, nl_query)` pair before
   evaluation or artifact writing. Same-ID substitutions, swaps, blank values,
   and duplicates are rejected.
3. v20 distinguishes generation gold-blind declarations from evaluation
   gold-consuming declarations. Historical metadata is preserved and the
   affected range is documented in
   `d1_3a_evaluation_gold_access_semantics_erratum_v1.md`.

## Commits and provenance

- `COMMIT_CODE_FIX18`: `ad332b729b4abad5b722358f9e2294f30b5d5b85`
- `COMMIT_EVIDENCE_FIX18`: created after this checkpoint is staged
- canonical v20 source commit: `ad332b729b4abad5b722358f9e2294f30b5d5b85`
- detached canonical worktree used with `core.autocrlf=false`
- tracked worktree gate: PASS; Git-byte input and implementation provenance:
  PASS

Canonical expected-query source:

- path: `data_real/heldout_v1/heldout_queries_v1.jsonl`
- Git blob SHA-256: `433b55308edf7806775e855d5c3bfd4c40c4d29602a62291d18e523b8f05fe91`
- 45 unique non-empty IDs and 45 exact NL-text pairs verified
- receipt path/hash verification: PASS

## v20 artifacts

Directory: `experiment-harness/results/d1_3a_v1_dev_regression_fix18_v20/`

| Artifact | SHA-256 |
|---|---|
| `d1_3a_v1_dev_generation_traces_v20.jsonl` | `4b026b0feffc546870e7b35b59ee2070c0ca8584b52a88733cc239e137a9509b` |
| `d1_3a_v1_dev_generation_receipt_v20.json` | `314c879e81b01f04450bcf1388a29a9a01fe11e11848c7d9d5ceb7b226f827c9` |
| `d1_3a_v1_dev_evaluation_rows_v20.jsonl` | `4ca5a4a05f3dc44e879d125a4579135093a504e04d09f35666256970af093729` |
| `d1_3a_v1_dev_summary_v20.json` | `395ef48c74b46b1877da438d97ca2f96317a1c49b8a705bf3b7bc2565a18699a` |
| `d1_3a_v1_dev_delta_review_fix_v20.md` | `ea6e603287d9c33aae442d75b12663916575593786454e8afeb754a8fca9962b` |

## Gates and tests

- executable semantic success: `13/39`
- known boundary abstention: `6/6`
- false abstention: `26`
- undetected semantic error: `0`
- previous-success regressions: `0`
- trace receipt verification: PASS
- expected-query binding verification: PASS (`45/45`)
- evaluator gold-consuming: `true`
- generator gold-blind: `true`
- graph-migration tests: `288 passed`
- experiment-harness tests with the repository's required
  `PYTHONPATH=experiment-harness`: `64 passed, 3 skipped`
- the unconfigured full harness invocation has one pre-existing collection
  import failure (`repair.lightweight_repair`); no Fix18 test failed
- `git diff --check`: PASS
- anti-overfitting scan: no fixed reviewer example IDs in production code

The three skipped tests are the existing Windows symlink-protection skips from
the Fix17 test suite.

## Explicit non-scope

No LLM/DeepSeek, Neo4j run, held-out v2, D1.3b, query-ID routing, manuscript
change, merge, or tag was performed. Historical v1–v19 artifacts remain
unchanged. Exactly one final non-force push is required after the evidence
commit; no push has been made in this checkpoint.
