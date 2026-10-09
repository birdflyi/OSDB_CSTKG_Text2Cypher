# D1.3a Fix13 Checkpoint Hash Correction Addendum

Date: 2026-10-09

This addendum corrects one provenance field in the historical Fix13 checkpoint. The original checkpoint is preserved for chronology; this addendum supersedes only its v14 delta-Markdown SHA-256 field.

## Source record

```text
SOURCE_FIX13_HEAD = e9cdc9cf63318ec3619db7d57dc140320f1543ea
SOURCE_CHECKPOINT = docs/freeze/d1_3a_review_fix13_checkpoint_v1.md
```

The original checkpoint recorded the committed v14 delta artifact as:

```text
INCORRECT_RECORDED_DELTA_SHA256 = e33c289b82d80df67998142a9bebfbb35e125bc384956826d0abd616ca402d6a
```

The authoritative SHA-256 of the exact committed Git bytes is:

```text
AUTHORITATIVE_COMMITTED_DELTA_SHA256 = 50450e019ce63235082c2e4ac0a96ae130b7d7bec52c5d38dd437db42e384599
```

Cause: the delta Markdown was hashed before trailing-space cleanup, and the checkpoint field was not refreshed afterward.

## Exact committed-byte re-hash audit

| v14 artifact | authoritative committed SHA-256 | checkpoint status |
|---|---|---|
| `experiment-harness/results/d1_3a_v1_dev_regression_fix13_v14/d1_3a_v1_dev_generation_receipt_v14.json` | `c61b338617eabf41e49925d485d74868894092b18ba34bbb5fd4549a5dd20207` | MATCH |
| `experiment-harness/results/d1_3a_v1_dev_regression_fix13_v14/d1_3a_v1_dev_generation_traces_v14.jsonl` | `df4ef1e8fd1d8cfae3ad4518341e74287c4ce8390cbe4156ed6edbeda0b90ae8` | MATCH |
| `experiment-harness/results/d1_3a_v1_dev_regression_fix13_v14/d1_3a_v1_dev_evaluation_rows_v14.jsonl` | `0b99ab37049981fc7df13dcf69d5c2d601843038c3cc78aac85db7e5f40fd8e1` | MATCH |
| `experiment-harness/results/d1_3a_v1_dev_regression_fix13_v14/d1_3a_v1_dev_summary_v14.json` | `57fbcef8e9a73f4f6be83c1450c83c37fb7ca7457fa85bd59a2315f5eba36d14` | MATCH |
| `experiment-harness/results/d1_3a_v1_dev_regression_fix13_v14/d1_3a_v1_dev_delta_review_fix_v14.md` | `50450e019ce63235082c2e4ac0a96ae130b7d7bec52c5d38dd437db42e384599` | CHECKPOINT FIELD MISMATCH; CORRECTED HERE |

This is the only hash-field discrepancy found in the five accepted v14 artifacts.

```text
SEMANTIC_CONTENT_CHANGED_BY_CORRECTION = NO
V14_METRICS_CHANGED = NO
V14_CLASSIFICATION_CHANGED = NO
V14_TRACE_RECEIPT_CHAIN_CHANGED = NO
V14_ARTIFACT_REGENERATED = NO
```

The original `docs/freeze/d1_3a_review_fix13_checkpoint_v1.md` remains unchanged.
