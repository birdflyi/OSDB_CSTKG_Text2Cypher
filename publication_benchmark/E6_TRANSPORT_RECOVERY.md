# E6 DuckDB recovery version, declared before dispatch

Original DuckDB v1 stopped after G/P/I's first goal each received HTTP 500 upstream-connection errors; no model content or query was returned. Other goals were not dispatched. Preserve that run, all three failures and their unknown usage. After offline result archiving and a separately recorded successful connectivity probe, allow one separately named full DuckDB v2 run.

V2 uses exactly the original 14 goals, ordering, model settings, prompts, compiler and one structural repair budget. This is infrastructure recovery, not response-quality-based selection: v1 has no available predictions to choose between. Do not retry any valid but wrong, refused, malformed or truncated result in v2 except the already defined one structural repair. Three consecutive infrastructure failures stop v2 as well. No further immediate full-run restart is authorized by this protocol.

Report 14 logical goals, not 15. Report original first-attempt interruption and recovery outcome separately; include all v1/v2 calls and unknown usage in complete costs. Do not average original infrastructure failure views with v2 scores as independent tasks, hide failures, or claim v2 was the original uninterrupted run. Frozen executable method is unchanged from 3d6bd22; subsequent commits only archive reports and declare this transport policy.

Once v2 completes, the already frozen E7 catalog-blind proposal generation may proceed sequentially. If v2 stops, do offline work and wait for a separately recorded low-frequency probe at a later heartbeat. The earlier background guard saw the stopped queue and is now awake-only; it will not independently start another generator.
