# D1.3a Review Fix21 Checkpoint v1

Date: 2026-10-10
PR: #6
PRE_FIX21_HEAD: `4d27f159f27c8b15299bfec9974499fa29cc85ef`
Fresh review: `5472580003`
Finding addressed: `4232179715`

## Commits and protocol

- `COMMIT_CODE_FIX21`: `27175a7849f605a65754b4ae48c5066ca69d0928`
- `COMMIT_EVIDENCE_FIX21`: created after this checkpoint is staged
- evidence parent must equal `27175a7849f605a65754b4ae48c5066ca69d0928`
- one non-force push is authorized after both commits; no intermediate push
- Fix20 and all historical v1-v22 evidence/checkpoints/errata remain immutable

## Bounded local polarity firewall

Finding `4232179715` exposed `except for` before a typed postfix prefix. The
previous parser recognized `except` but not the bounded preposition, so the
request incorrectly became executable positive `STARTS_WITH`.

Fix21 centralizes the bounded typed-prefix exclusion cue surface and adds
`except for` to the existing local polarity checks. The rule is reused by
postfix token detection, pre-token typed-prefix negation, unsupported-constraint
auditing, and trailing typed exclusion detection. It is local to the relevant
typed token/clause; sentence barriers remain hard barriers. Negative or
unsupported typed-prefix forms preserve visible `NOT_STARTS_WITH`/source-span
evidence, abstain, select no template, and render no Cypher. No negative Cypher,
general Boolean parser, query-ID routing, or arbitrary English grammar was added.

## Safety matrix

- `except for` with and without determiner: fail-closed for Issue and
  PullRequest typed prefixes
- existing `except`, `excluding`, `without`, `omitting`, and `but not` forms:
  fail-closed
- positive `whose IDs start with`, `with ... prefix`, and PullRequest forms:
  remain executable
- unrelated negation across a sentence barrier or on an unrelated object:
  does not poison the positive prefix
- Fix18 trailing exclusions remain fail-closed
- local matrix and regression tests: `73 passed`; Fix21-specific tests:
  `14 passed`

## Canonical v23 evidence

Source commit: `27175a7849f605a65754b4ae48c5066ca69d0928`.
Generation/evaluation ran in a clean detached worktree with
`core.autocrlf=false`. Role is `DEVELOPMENT_REGRESSION / NOT_HELDOUT`; no Neo4j
runtime or held-out v2 was run.

| Artifact | SHA-256 |
|---|---|
| `d1_3a_v1_dev_generation_traces_v23.jsonl` | `4b026b0feffc546870e7b35b59ee2070c0ca8584b52a88733cc239e137a9509b` |
| `d1_3a_v1_dev_generation_receipt_v23.json` | `5b28e21d354abf1409dc156388240d0cba3fe2a2124c16d5b569aba4cfd35051` |
| `d1_3a_v1_dev_evaluation_rows_v23.jsonl` | `4ca5a4a05f3dc44e879d125a4579135093a504e04d09f35666256970af093729` |
| `d1_3a_v1_dev_summary_v23.json` | `b29cff406db82e486be9bbcd3fb68209463dae285753ae9acde60490fa839d5b` |
| `d1_3a_v1_dev_delta_review_fix_v23.md` | `ea6e603287d9c33aae442d75b12663916575593786454e8afeb754a8fca9962b` |

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
- resolved bundle SHA-256:
  `a587c26385db6ae4d75621281126425b1b339bbe67c45e6a059c9c9349fde6dd`
- canonical generation contract binding: PASS
- trace receipt verification: PASS
- expected query binding: `45/45`, PASS
- Git-byte input and implementation provenance: PASS
- tracked worktree clean: PASS

## Metrics and invariants

- `N=45`, `N_EXECUTABLE=39`, `N_ABSTENTION=6`
- executable semantic success: `13/39`
- known-boundary abstention: `6/6`
- false abstention: `26`
- undetected semantic error: `0`
- previous-success regressions: `0`
- v22→v23 changed classification IDs: `[]`
- v23 trace/evaluation artifacts preserve the same development-only metrics as
  accepted v22; this does not establish held-out, runtime, or generalization
  correctness

## Tests and fixture exception

- graph-migration full suite: `306 passed`, `1 failed`; status
  `PASS_WITH_PREEXISTING_FIXTURE_BLOCKER`
- exact failure: `graph-migration/tests/test_real_csv_preprocess.py::test_preprocess_osdb_csv_adds_augmented_columns_and_fine_grained`
  raises `FileNotFoundError` for ignored/untracked
  `graph-migration/fixtures/real_pilot_redis/mini_sample.csv`
- the same missing-fixture failure was present on the clean pre-Fix20
  `d70e8d5` baseline; no replacement fixture was fabricated or committed
- experiment-harness full suite: `69 passed, 3 skipped`
- Windows symlink tests: `3 skipped`
- `git diff --check`: PASS
- anti-overfitting scan: no reviewer-specific IDs or review-thread literals in
  production code; sample IDs occur only in tests

## Non-scope

No LLM/DeepSeek, Neo4j runtime, held-out v2, D1.3b, manuscript edit, tag,
merge, or query-ID routing was performed. Historical v1-v22 evidence is
unchanged and the local ignored fixture was not committed.
