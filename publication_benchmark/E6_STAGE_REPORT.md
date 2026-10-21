# E6 information-interface development: Redis and DuckDB recovery complete

## Recovery outcome

The separately declared DuckDB v2 run completed with exactly the same methods, tasks and settings. G1 completed 10/12 answerable goals plus both boundaries, with one infrastructure failure and one wrong returned result. I1 completed 10/12 plus both boundaries, with two infrastructure failures and no wrong returned result. P1 completed 6/12 plus both boundaries, with one infrastructure failure. No repairs were made to valid but wrong predictions. Including Redis, G/I each completed 22/24 answerable cases and all four boundaries; this is no overall operational accuracy superiority.

The direct-query error on DuckDB's new-PR list used exact `entity_id=$entity_prefix` matching rather than STARTS WITH and returned an empty list. The query executed but lost the requested prefix semantics. The service's typed prefix parameter compiles to STARTS WITH, but this one developer-known example is not a general semantic guarantee or an independent main effect.

DuckDB v2 used 43 calls, known tokens253257, unknown usage4; all43 inputs passed isolation audit. Together with Redis43 calls and original DuckDB3 failed calls, E6 used89 actual attempts, known tokens509791, unknown usage9. The original interrupted run remains visible below and in E6_RESULTS.json. Count14 logical goals per project, not15 for DuckDB. Execution protocol and compiler were unchanged; later commits only record reports and the declared recovery policy.

Original stage checkpoint follows for provenance; its next-action paragraph is historical and has been completed by the recovery above.

Method frozen at 3d6bd22 before predictions. Added one generic observed_events operation, not an operation per question. It preserves time/action/explicit-decline scope before projection or reduction. The six legacy services, read guard and one structural repair remain unchanged. P and I share the same output/refusal instructions; I has the additional executable event contract. G receives the full schema and extended contract definitions. This is a focused capability extension, not a novel general theory or established superiority.

| Redis repaired view | Graph-answerable completion | Boundary decisions | Wrong returned answers | Infrastructure outcomes |
|---|---:|---:|---:|---:|
| Strong direct G1 | 12/12 | 2/2 | 0 | 0 |
| Protocol-only six-service P1 | 6/12 | 2/2 | 0 | 2 |
| Extended information I1 | 12/12 | 2/2 | 0 | 0 |

G and I were also fully correct on their first pass. P's six old supported tasks remain correct; it does not complete the six event goals outside its catalog. Four of those six were explicitly refused after repair and two ended with infrastructure failures. Do not count the resulting zero wrong returned answers as useful semantic superiority. G/I have equal correctness here, so no accuracy advantage over strong direct generation has been demonstrated.

Redis used 43 actual calls: 42 first passes plus one P repair. Known reported tokens 256,534; two calls have unknown usage. Thirty-five targeted tests passed. Independently computed CSV answers, earlier separately written queries and extended compiled queries were checked on both dedicated databases: 24/24 conformance checks per project. That is reference conformance, not NL accuracy. All 43 Redis inputs passed gold-field isolation audit.

DuckDB v1 stopped on the first goal after three consecutive HTTP 500 responses, one each for G/P/I. Provider error reported an upstream connection failure. No model result or query was returned; the other 13 goals were not dispatched. Three actual calls have unknown usage. This is an infrastructure interruption, not 0/14 method capability. All three dispatched inputs passed isolation audit. No silent retry or best-of sampling was used.

Next: archive this incomplete version; perform only a separately recorded low-frequency connectivity probe after offline work. If usable, run a separately named recovery version with the exact method/tasks/settings and retain the original three failures in all-attempt cost accounting. No tuning against these outcomes. The independent workload and preselected-project predictions remain pending. All 14 requests/project are previously known development/regression goals and cannot become independent primary evidence. E6_RESULTS.json preserves the completed Redis and interrupted DuckDB summaries.
