# D1.3a Review Fix19 Checkpoint v1

Date: 2026-10-09
PR: #6
PRE_FIX19_HEAD: `2ee5d095ae110cefb705cfd811d9e2fb1fd7beee`
Fresh review: `5470938420`
Finding: `4230815227`

## Commits and protocol

- `COMMIT_CODE_FIX19`: `266336df6691dabd522c1c782faafce615bc3306`
- evidence commit: this append-only evidence commit, whose exact SHA is
  recorded in the final handoff after creation
- evidence parent: `266336df6691dabd522c1c782faafce615bc3306`
- one non-force push: pending at checkpoint creation
- no historical v1–v20 artifact was edited

## Minimal patch

`generation_receipt.py` now keeps independent `expected_trace_project_path`
and `expected_queries_project_path` variables. Focused tests assert trace-only
behavior, distinct trace/query paths and hashes, and fail-closed path/byte
mutations. No grammar, routing, model, runtime, or manuscript changes were
made.

## Canonical v21 evidence

Source commit: `266336df6691dabd522c1c782faafce615bc3306`
Detached worktree: clean; `core.autocrlf=false` explicitly set.
Canonical Git-byte input and implementation provenance: PASS.

| Artifact | SHA-256 |
|---|---|
| `d1_3a_v1_dev_generation_traces_v21.jsonl` | `4b026b0feffc546870e7b35b59ee2070c0ca8584b52a88733cc239e137a9509b` |
| `d1_3a_v1_dev_generation_receipt_v21.json` | `45e28c2990407031b9c11ba22e444167d5919220af266d4684194ba7f43fa27a` |
| `d1_3a_v1_dev_evaluation_rows_v21.jsonl` | `4ca5a4a05f3dc44e879d125a4579135093a504e04d09f35666256970af093729` |
| `d1_3a_v1_dev_summary_v21.json` | `f34e9949b9ae1a1295a7a1d5f66d2b42b156721f144da087207b516f297118f5` |
| `d1_3a_v1_dev_delta_review_fix_v21.md` | `ea6e603287d9c33aae442d75b12663916575593786454e8afeb754a8fca9962b` |

## Cross-artifact invariants

- trace path: `experiment-harness/results/d1_3a_v1_dev_regression_fix19_v21/d1_3a_v1_dev_generation_traces_v21.jsonl`
- trace SHA: `4b026b0feffc546870e7b35b59ee2070c0ca8584b52a88733cc239e137a9509b`
- receipt trace path/SHA: identical to the above
- summary trace path/SHA: identical to the above
- expected query path: `data_real/heldout_v1/heldout_queries_v1.jsonl`
- expected query SHA: `433b55308edf7806775e855d5c3bfd4c40c4d29602a62291d18e523b8f05fe91`
- receipt query path/SHA and summary authority path/SHA: identical
- path/hash cross-artifact consistency: PASS
- exact expected-query binding: `45/45`, PASS
- trace→receipt verification: PASS

## Gates and tests

- v21: 13/39 executable successes; 6/6 known-boundary abstentions; 26 false
  abstentions; 0 undetected semantic errors; 0 previous-success regressions
- changed classifications relative to v20: `[]`
- focused Fix19 tests: `14 passed`
- graph-migration: `287 passed`, `1 failed` because the existing fixture
  `graph-migration/fixtures/real_pilot_redis/mini_sample.csv` is absent
- experiment-harness with `PYTHONPATH=experiment-harness`: `65 passed, 3 skipped`
- Windows symlink skips: `3`, unchanged from Fix17
- `git diff --check`: PASS
- anti-overfitting scan: no fixed reviewer example IDs in production code

The graph-migration fixture failure is environmental/pre-existing and outside
Fix19; no production code was changed to mask it.

## Non-scope

No LLM/DeepSeek, Neo4j, held-out v2, D1.3b, manuscript edit, tag, merge, or
query-ID routing was performed. Fix18 checkpoint and gold-access erratum remain
unchanged. The three older Fix18 threads were already resolved.
