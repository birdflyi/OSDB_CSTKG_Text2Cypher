# D1.3a v14 development regression delta (Fix13)

Artifact version: `v14`
Role: `DEVELOPMENT_REGRESSION` (`NOT_HELDOUT`)

## Comparison with v13

| Metric | v13 | v14 | Delta |
|---|---:|---:|---:|
| Executable semantic success | 13/39 | 13/39 | 0 |
| Known boundary abstention | 6/6 | 6/6 | 0 |
| False abstention | 26/39 | 26/39 | 0 |
| Undetected semantic error | 0 | 0 | 0 |
| Previous-success regressions | 0 | 0 | 0 |

`CLASSIFICATION_CHANGED_IDS = []`. The two reviewed uniqueness constraints now remain explicit in IR and fail closed against ordinary projection contracts. No template Cypher was changed to add DISTINCT.

Canonical source commit: `ed2f3cafde3e3abe52b02f7759967b8c84476090`.
Git-byte verification: PASS. Trace/receipt chain: PASS.
`V2_CONSTRUCTED = NO`; `NEO4J_RUN = NO`.
