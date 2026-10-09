# D1.3a Review Fix17 Checkpoint

## Scope and disposition

- `PRE_FIX17_PR_HEAD`: `b4a9dee84d675873aebbf0471a7100a04fafc047`
- `REVIEWED_HEAD_CONFIRMED`: `b4a9dee84d675873aebbf0471a7100a04fafc047`
- Finding A (`4228699307`): valid evaluation-integrity blocker. Every direct trace, gold, frozen-baseline, pre-fix, and current evaluation row artifact is validated for a non-empty string `heldout_id` and uniqueness before any ID-keyed mapping. Invalid input fails closed with source, ID/value, and row information.
- Finding B (`4228699316`): valid append-only evidence-integrity blocker. Artifact paths now retain and classify both normalized lexical absolute paths and fully resolved targets; either namespace is protected, including symlink aliases in either direction.
- Finding C (`4228699324`): valid but non-blocking bounded-frontend coverage observation. `For issue with ID I_880002#77, show who opened it` safely abstains outside the frozen controlled grammar. It is recorded as `NONBLOCKING_NLU_COVERAGE_DEFERRED`; no grammar expansion was made and the behavior is not claimed as fixed.

## Code and evidence commits

- `COMMIT_CODE_FIX17`: `95f6a8bf7883203258ed532a97dbeab7472b3472`
- `COMMIT_EVIDENCE_FIX17`: recorded after this checkpoint is committed
- `EVIDENCE_PARENT_EQUALS_CODE`: `YES` (required commit structure)
- `ONE_FINAL_PUSH`: pending until evidence commit is created and verified

## Verification

- Focused Fix17 integrity tests: `34 passed, 3 skipped` (the three symlink-creation cases were skipped because this Windows session cannot create symlinks; no symlink test is counted as passing evidence).
- Full `experiment-harness/tests`: `61 passed, 3 skipped`.
- Full `graph-migration/tests`: `279 passed`.
- `git diff --check`: `PASS`.
- Python compilation check: `PASS`.
- Anti-overfitting scan for held-out/request-specific production literals: `PASS`.
- `CONTROLLED_LANGUAGE_BOUNDARY_UNCHANGED`: `YES`.

## Canonical v19 development regression

Canonical source commit: `95f6a8bf7883203258ed532a97dbeab7472b3472`.

The run was generated and evaluated in a detached worktree at that exact commit with `core.autocrlf=false`-equivalent canonical Git-byte checks. Direct inputs and runtime implementation bytes matched Git blobs; the generation trace receipt verified; the tracked worktree gate passed.

Output directory:

```text
experiment-harness/results/d1_3a_v1_dev_regression_fix17_v19/
```

Acceptance gates:

| Gate | Result |
|---|---|
| `V19_EVALUATION_ROLE=DEVELOPMENT_REGRESSION` | PASS |
| `V19_TRACE_RECEIPT_VERIFICATION` | PASS |
| `V19_INPUT_GIT_BYTE_VERIFICATION` | PASS |
| `V19_IMPLEMENTATION_GIT_BYTE_PROVENANCE` | PASS |
| `V19_TRACKED_WORKTREE_CLEAN` | PASS |
| 45 unique/non-empty IDs | PASS |
| Executable semantic success | `13/39` |
| Known boundary abstention | `6/6` |
| False abstention | `26` |
| Undetected semantic error | `0` |
| Previous-success regressions | `0` |
| Changed classification IDs vs v18 | `[]` |
| Changed selected-template IDs vs v18 | `[]` |

| Artifact | SHA-256 |
|---|---|
| `d1_3a_v1_dev_generation_traces_v19.jsonl` | `4b026b0feffc546870e7b35b59ee2070c0ca8584b52a88733cc239e137a9509b` |
| `d1_3a_v1_dev_generation_receipt_v19.json` | `f6e95449bda19eacf69cd6d87e9e58f0af7e0f54d06243a18b7b083677d833dc` |
| `d1_3a_v1_dev_evaluation_rows_v19.jsonl` | `4ca5a4a05f3dc44e879d125a4579135093a504e04d09f35666256970af093729` |
| `d1_3a_v1_dev_summary_v19.json` | `e19ccc547446987eccb22a19c1a64117f56bde309d665fc03eb56f8506123453` |
| `d1_3a_v1_dev_delta_review_fix_v19.md` | `ea6e603287d9c33aae442d75b12663916575593786454e8afeb754a8fca9962b` |

## Historical and scope preservation

- `V1_V18_ARTIFACTS_UNCHANGED`: `YES` (v19 is append-only; v1-v18 were not rewritten).
- `FIX16A_CHECKPOINT_UNCHANGED`: `YES`.
- `D1_2C_V1_UNCHANGED`: `YES`.
- No Neo4j run, LLM/DeepSeek call, held-out v2 construction, manuscript revision, D1.3b work, or PR merge was performed.
