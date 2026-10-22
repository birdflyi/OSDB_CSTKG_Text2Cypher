# Grounded DuckDB with declared ten-second retries

22 October 2026. Status: STOPPED_INFRASTRUCTURE. Frozen E10 runner f1359a0 and original E9 engine ea21869. Seven original requests comprise six phrasings of three adapted answerable goals plus one separate literal-direction control. These are not seven independent goals or human requests. P/I cannot express the answerable output goals in their frozen single-call language; this is coverage evidence, not matched-capability mapping accuracy.

| Arm | Shared view | Main cases evaluated / planned | Main correct | Main wrong returned | Main safe refusals | Main infrastructure | Direction control correct / evaluated |
|---|---|---:|---:|---:|---:|---:|---:|
| G | FIRST_PHYSICAL | 3/6 | 3 | 0 | 0 | 0 | 1/1 |
| G | AFTER_TRANSPORT | 3/6 | 3 | 0 | 0 | 0 | 1/1 |
| G | AFTER_STRUCTURAL | 3/6 | 3 | 0 | 0 | 0 | 1/1 |
| P | FIRST_PHYSICAL | 3/6 | 0 | 0 | 3 | 0 | 0/1 |
| P | AFTER_TRANSPORT | 3/6 | 0 | 0 | 3 | 0 | 0/1 |
| P | AFTER_STRUCTURAL | 3/6 | 0 | 0 | 3 | 0 | 0/1 |
| I | FIRST_PHYSICAL | 3/6 | 0 | 0 | 2 | 1 | 0/1 |
| I | AFTER_TRANSPORT | 3/6 | 0 | 0 | 2 | 1 | 0/1 |
| I | AFTER_STRUCTURAL | 3/6 | 0 | 0 | 2 | 1 | 0/1 |

Views share actual requests: first physical; selected response after infrastructure retries; final after at most one structural repair. Wrong/refused/truncated or non-infrastructure responses are never transport-rerolled. The two phrasings of a goal are not independent tasks. The direction-control empty answer is excluded from the main six-case result. Undispatched positions remain visible and are not scored as completed failures.

## Actual transport and cost

There are 14 unique physical requests, including 2 infrastructure retries, with 0 logical requests reaching HTTP200 after a failed first attempt. Status breakdown: {"200": 11, "500": 2, "ReadTimeout": 1}. All actual requests have one budget slot and immutable files. Every retry body matches its first logical request. Retry waits pass monotonic and wall-clock checks; total injected waiting is 20.016 seconds, separate from 398.718 network seconds. First/retry/structural views do not multiply cost.

This cohort reports 39,881 known tokens and 3 unknown-usage requests. Global ledger: 362/1200 attempts, 86 unknown usage, zero pending, 1,300,415 known tokens. Old global duration includes earlier pacing waits; this new cohort keeps waits separate. Currency fees remain unverified. Per-arm full-attempt usage is in the accompanying checkpoint, not success-only estimates.

All seven reference checks pass. Source/task/schema/model and saved engine hashes match. All 362 saved model inputs pass the protected-field audit (13 plaintext user messages), not a general contamination proof. The three-global-physical-failure stop is verified against budget order. This declared retry cohort is not pooled with historical zero-retry runs. No causal rate-limit or architecture advantage follows from recovery.

Raw records remain immutable under e10_grounded_duckdb_retry_v1. Shareable checkpoint: E9_E10_GROUNDED_DUCKDB_CHECKPOINT.json. No automatic next batch or retry is admitted. If stopped, routine probing resumes no earlier than 2026-10-22T15:12:34+08:00. The DASFAA contribution and remaining frozen tasks still require completion and evidence review.
