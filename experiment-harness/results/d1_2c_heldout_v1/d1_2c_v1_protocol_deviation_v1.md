# D1.2c v1 Protocol Deviation

`NO_PREVIEW_RULE_VIOLATED = YES`

During preparation before COMMIT_A, the first few frozen held-out query rows
were inspected to understand the input shape. This occurred before the
generation start marker and before COMMIT_A; no held-out rows were inspected
after COMMIT_A and before generation based on the recorded command sequence.

```text
HELDOUT_NL_PREVIEW_OCCURRED_BEFORE_COMMIT_A = YES
HELDOUT_NL_PREVIEW_OCCURRED_AFTER_COMMIT_A = NO_EVIDENCE
PREVIEW_INFLUENCED_SYSTEM_BEHAVIOR = NO_EVIDENCE_OF_CHANGE
PRISTINE_BLIND_HELDOUT_PROTOCOL = NO
```

The preview is a protocol deviation, not evidence that the frozen parser,
generator, templates, repair, or evaluator were changed. The implementation
and dataset remained unchanged after COMMIT_A and no generation rerun is
authorized by this incident task.
