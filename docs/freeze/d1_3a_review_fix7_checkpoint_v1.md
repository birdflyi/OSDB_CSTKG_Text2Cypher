# D1.3a PR #6 Review-Fix 7 Checkpoint

Date: 2026-10-08
PR: #6
Branch: `feat/ch7-d1-3a-scope-projection-ir`
Pre-fix7 head: `ff9c308426a426db7eb5b6517ff7b58293fcd77d`

## Review findings and fixes

- Finding A: `VALID_FIXED_POSSESSIVE_ITEM_DISTINCT`. Bounded noun-first ID
  projection cues now accept singular and plural possessives and explicit
  `distinct`/`unique` before IDs or identifiers. The cue remains local to its
  projection item; it does not set tuple DISTINCT.
- Finding B: `VALID_FIXED_ANCHORED_GENERIC_ID_FALLBACK`. Generic ID fallback
  excludes simple and composite source-introduction nouns, including nouns
  embedded in composite anchors. The result cue includes `whoever`, allowing
  an anchored Issue to remain source context while projecting the opened-by
  Actor. With no safe target, generation fails closed.
- Finding C: `VALID_FIXED_PLURAL_DOMAIN`. Bounded `domain`/`domains`, including
  `registrable domain(s)` and `site domain(s)`, map to
  `ExternalResource.url_domain_etld1`. “External domains” as a link target is
  not treated as a requested domain property.
- Finding D: `VALID_FIXED_SINGLE_INHERITANCE_EXTENDS_POLICY`. Runtime loading
  and provenance closure now share the single-base pack policy. List-valued
  `extends` fails closed with `MULTIPLE_TEMPLATE_BASES_NOT_SUPPORTED`;
  string-valued inheritance, standalone packs, three-layer chains, cycle
  detection, and missing-base failure remain supported.

## Fix6 checkpoint QA correction

```text
POST_COMMIT_DIFF_CHECK_PRE_CORRECTION =
3 trailing-whitespace warnings in this checkpoint only
PRE_CORRECTION_FIX6_CHECKPOINT_BLOB_SHA = 873db92dfc700eb79bc5d2ab08d94559ec798a0a
SEMANTIC_OR_EVIDENCE_CONTENT_CHANGED = NO
FINAL_DIFF_CHECK_AFTER_CORRECTION = PASS
```

The three formatting-only trailing spaces were removed. No semantic code,
metrics, or evidence artifact content changed.

## Commits

```text
COMMIT_CODE = 18ae57970e2d4dc1d8e0fd3452b39360bc2ab1ea
COMMIT_EVIDENCE_PARENT = 18ae57970e2d4dc1d8e0fd3452b39360bc2ab1ea
COMMIT_EVIDENCE = this checkpoint's containing commit (recorded in Git history)
```

The code commit contains production code, tests, and the Fix6 checkpoint QA
correction; it contains no v8 evidence artifacts. No production code changes
are included in the evidence commit. The evidence commit ID is reported from
Git history because a commit cannot embed its own final object ID.

## New synthetic regression cases

- Possessive item DISTINCT: `actors' distinct IDs`, `actors' unique
  identifiers`, and `actor's distinct IDs` set item DISTINCT. Ordinary
  `actors' IDs` remains non-distinct; noun-first DISTINCT and `distinct IDs of
  actors` remain supported.
- DISTINCT locality and aggregation: a second ordinary projection remains
  non-distinct and tuple DISTINCT remains false; `count distinct actor IDs`
  remains aggregate-only.
- Anchored generic-ID safety: “show the ID of whoever opened it” selects the
  Actor while preserving the Issue source and renders the opened-by template;
  “show the ID” after a simple or composite source anchor invents no target;
  a later non-source Actor noun remains eligible.
- Plural domain projection: resource IDs and plural domains preserve requested
  order; plural registrable domains map to the bounded domain property; an
  “external domains” link target is not misread as that property.
- Pack policy: runtime and provenance both reject list-valued `extends` with
  the same error; standalone and string-valued packs load; a three-layer
  single-base chain works; cycle detection remains active.

Targeted tests covering the new cases: 17 passed. Full
`graph-migration/tests`: 182 passed. Full `experiment-harness/tests`: 29
passed. `git diff --check` and the production anti-overfitting scan passed.

## Canonical Git-byte v8 development regression

Generation and evaluation ran in a detached worktree with
`core.autocrlf=false`, checked out at exactly `COMMIT_CODE`.

```text
canonical_source_commit = 18ae57970e2d4dc1d8e0fd3452b39360bc2ab1ea
canonical_git_byte_verification = PASS
all tracked direct inputs bytes_match_git_blob = true
evaluation_role = DEVELOPMENT_REGRESSION / NOT_HELDOUT
generation_input_fields = id, nl_query
evaluation_annotations_loaded_for_generation = false
gold_or_reference_cypher_loaded_for_generation = false
NEO4J_RUN = NO
```

## v7 baseline and v8 metrics

| Metric | v7 | v8 | Delta vs v7 |
|---|---:|---:|---:|
| Executable semantic success | 12/39 | 12/39 | 0 |
| Known boundary abstention | 6/6 | 6/6 | 0 |
| False abstention | 27/39 | 27/39 | 0 |
| Undetected semantic error | 0 | 0 | 0 |
| Previous-success regressions | 0 | 0 | 0 |

All five v8 artifacts were copied byte-for-byte from the canonical worktree.
The generation trace, evaluation rows, and delta report are unchanged from v7;
the receipt and summary record the v8 version and canonical source commit.
The frozen 39-row development set has no classification changes from v7, so
these fixes add synthetic edge-case coverage but no development-set metric
gain. The interpretation remains static-only.

## v8 artifacts

| Artifact | SHA-256 |
|---|---|
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_generation_traces_v8.jsonl` | `c20a7a76db6666971150eee6beabee13dbf51f49b03668748e515a33ae1c1471` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_generation_receipt_v8.json` | `fd84579a0cfe203b4f433438ef9ad8ad903929dd47cc65195d37fcae3f5fd525` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_evaluation_rows_v8.jsonl` | `2c611a74d3d513d762a86011ca9f828d101514931c1be2ea64986cef4ebacde5` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_summary_v8.json` | `a023f696cb3f89a9171f504eea3ea143f0381affe99ec415057ef2d1ca3d5d34` |
| `experiment-harness/results/d1_3a_v1_dev_regression/d1_3a_v1_dev_delta_review_fix_v8.md` | `56a0cae66735f85f2ced4b7bba4d20c652660665a01a5350c6599da6dfa3de20` |

## Historical immutability and safety

Before v8 generation, SHA-256 snapshots were taken for all 35 existing D1.3a
v1-v7 artifacts and all 16 historical D1.2c-v1 result artifacts. After
generation and copying v8, all 51 hashes matched; no historical artifact was
modified or removed. The Fix6 checkpoint changed only for the documented QA
correction.

```text
V1_V7_ARTIFACTS_UNCHANGED = YES
HISTORICAL_D1_2C_V1_CHANGED = NO
KNOWN_BOUNDARY_ABSTENTION = 6 / 6
UNDETECTED_SEMANTIC_ERROR = 0
PREVIOUS_SUCCESS_REGRESSION = 0
QUERY_ID_ROUTING = NO
HELDOUT_SPECIFIC_PRODUCTION_LITERAL = NO
FINAL_FULL_PR_DIFF_CHECK = PASS
V1_ROLE = DEVELOPMENT_DIAGNOSTIC
V2_CONSTRUCTED = NO
NEO4J_RUN = NO
```

The full PR-range whitespace check was run against base
`98d860a6601ed24fa8d6e8f017bf98ddca79548b` after all changes were committed.
This checkpoint does not promote the v1 development diagnostic to held-out
evidence and does not claim runtime correctness.
