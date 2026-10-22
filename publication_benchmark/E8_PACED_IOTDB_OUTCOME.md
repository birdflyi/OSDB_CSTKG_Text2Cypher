# First paced IoTDB source batch: stopped, not completed

Checkpoint: 22 October 2026, after the batch ended at 08:59 local time. The method, eight source tasks, references, compact contexts, model settings, compiler, scoring and one structural-repair allowance remained frozen. Admission wrapper: `0a185eb`; execution manifest: `53ac4323a70bf9f88cc4b3f1016046f66e125222`. This is the first IoTDB source prediction batch, not a retry of Redis or DuckDB.

Probe07 returned HTTP 200. The separately disclosed transport wrapper then imposed at least 60 seconds between actual model-request starts, equally for all arms and repairs, with no transport retry. The batch stopped after 13 requests. Requests 11–13 all returned HTTP 500, triggering the existing three-consecutive-infrastructure stop. No second IoTDB batch or automatic next-project batch was started.

| IoTDB source view | Evaluated / planned | Exact snapshot answers | Safe catalog refusals | Infrastructure outcomes | Contract-error outcomes |
|---|---:|---:|---:|---:|---:|
| G1, repaired direct | 4 / 8 | 1 | 0 | 3 | 0 |
| P1, protocol-controlled old services | 4 / 8 | 0 | 3 | 1 | 0 |
| I0, extended services before repair | 4 / 8 | 0 | 2 | 1 | 1 |
| I1, extended services after allowed repair | 4 / 8 | 0 | 2 | 2 | 0 |

First/repaired views share requests. Do not add their counts or costs. All eight tasks are graph-answerable; a safe catalog refusal is not their successful completion. Their frozen coverage labels describe a single service call, not every composition of existing calls.

The direct branch-count answer matches the original snapshot, but its parameter omits the requested literal prefix colon (`B_158975124` instead of `B_158975124:`). Preserve the original snapshot grade. The already documented post-hoc prefix witness establishes a failure class, not a new prospective IoTDB error rate. The I0 PR-duration prediction attempts an `observed_events` call but omits required `explicit_decline`. It fails the contract check before execution; its single structural repair encounters infrastructure failure. It supplies no returned evidence and is not a successful upper-layer plan.

## Transport and cost

| Actual paced requests | Result |
|---|---:|
| Total requests, including one repair | 13 |
| HTTP 200 | 7 |
| HTTP 500 | 5 |
| Read timeout | 1 |
| Known reported tokens | 20,690 |
| Unknown-usage requests | 6 |
| Measured network time, seconds | 345.921 |
| Injected waiting, seconds | 493.845 |
| Frozen runner dispatch sum, seconds | 839.828 |

Network plus waiting reconciles to the runner within 0.062 seconds of local overhead. Runner dispatch time includes pacing and must not be called pure model latency. Receipt-based reconstruction gives a minimum start gap of 59.985 seconds, within filesystem/write timing tolerance; the pacer uses a monotonic clock to enforce its 60-second interval. All 13 receipt payload hashes match the actual saved requests, and their structured user inputs contain none of the protected gold/score fields. These checks do not prove absence of all textual contamination.

Pacing did not prevent interruption in this batch. It was not randomized against an identical unpaced batch, so these outcomes neither identify the underlying cause nor estimate the causal effect of cadence. There is no explicit rate-limit evidence and no architecture latency advantage.

Private receipts are preserved under `continuation_20261021/e7_source_iotdb_v1` and `iotdb_paced_integrity_audit_v1.json`. The E9 checkpoint contains the shareable aggregate and hashes. No raw provider error body or private connection configuration belongs in Git.

## Continuation boundary

No model job remains active. Preserve this batch and earlier valid outputs. The admission helper deliberately rejects an existing IoTDB output directory; do not reuse it as a probe-only tool. At most one later recorded health probe is permitted after at least one hour from the latest actual terminal, not before approximately 09:59:06 local. A short probe alone does not authorize another full-task batch. A recovered or replacement transport needs a separately recorded finite continuation, unchanged frozen evaluation rules and complete attempt accounting. Offline preparation is sufficient; do not add services or tasks while transport is the blocker.
