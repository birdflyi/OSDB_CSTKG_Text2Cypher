# D1.2c-v1 Historical Harness Status

This directory contains the harness that produced the historical D1.2c-v1
first run from pre-run harness commit
`69eb9b818ccab7b4965c1b6fdc5f45ff50885c65`.

The generation wrapper used for that run contained a metadata bug: it did not
carry the frozen `heldout_id`/request ID into the raw trace. Generation still
processed all 45 frozen rows. Exact-NL reconciliation established a 45/45
bijection in the original frozen order, and the missing IDs had no behavioral
effect on parsing, template selection, rendering, validation, or repair.

The D1.2c-v1 generation must not be rerun. The raw trace is immutable:

```text
RAW_TRACE_SHA256 = 9df9e576460f77d4c8d6fa9bd9a6b65b9eece83cd6837ec1e828027ba2604334
GENERATION_RERUN = NO
RAW_TRACE_IMMUTABLE = YES
```

The versioned recovered v2 evaluation files are authoritative for diagnostic
metrics. The v1 evaluation files remain only as historical evidence of the
later-corrected reference-provenance reporting defect.

The explicit no-preview rule was violated during harness preparation. There is
no evidence that the preview changed system behavior, but the run is not a
pristine blind held-out evaluation:

```text
NO_PREVIEW_RULE_VIOLATED = YES
PREVIEW_INFLUENCE_CLASSIFICATION = NO_EVIDENCE_OF_CHANGE
PRISTINE_BLIND_HELDOUT_PROTOCOL = NO
```

After inspection and root-cause diagnosis, v1 is development evidence only:

```text
HELDOUT_V1_ROLE = DEVELOPMENT_DIAGNOSTIC
V1_MUST_NOT_BE_USED_AS_POST_TUNING_HELDOUT = YES
NEW_INDEPENDENT_HELDOUT_V2_REQUIRED_AFTER_TUNING = YES
```

Do not interpret the four semantically correct rendered executable outputs as
4/4 overall accuracy. The recovered first-run result is 4/39 executable
semantic success, 6/6 correct known-boundary abstention, and 35/39 executable
false abstention. All 35 executable failures occurred before render.

This evidence freeze does not modify `generate_heldout_v1.py`,
`evaluate_heldout_v1.py`, or any parser, generator, template, repair, evaluator,
dataset, or database state.
