# E5 output and refusal protocol control

Both Redis and DuckDB development batches completed. The compiler and six-service catalog were unchanged. S used the original instructions; P added exact JSON/refusal examples and clarified the existing catalog boundary. One structural/protocol/execution repair was allowed, never a correct-answer-aware retry.

| Repaired view | Redis supported correct | DuckDB supported correct | Catalog-outside safe refusals, both projects | Wrong returned answers | Infrastructure outcomes |
|---|---:|---:|---:|---:|---:|
| Original S1 | 6/6 | 6/6 | 2/12 | 1 | 5 |
| Protocol-only P1 | 6/6 | 6/6 | 12/12 | 0 | 0 |

P also returned both boundary decisions correctly on each project: missing anchor requires NEEDS_INPUT, actual resolving semantics absent from the graph require UNSUPPORTED. Full catalog-decision correctness was P0/P1 28/28, S0 14/28 and S1 17/28, including infrastructure outcomes in these operational denominators. All planned first passes were sent. Shared first/repaired views are not independent samples.

The concrete wrong returned result was Redis's request for July closed Issues: S selected issue_commit_path_count with grain distinct_comments and returned 39. It passed the operation/argument contract and executed, but answered a different question. This demonstrates that structural validity is not request-semantic preservation. In this development sample a protocol-only clarification removed the observed error; it is not a new semantic-validation mechanism or a general reliability guarantee.

There were 63 actual model responses/transport terminals across both projects, including seven repair calls: 31 Redis and 32 DuckDB. Known reported tokens: 356,840; five calls had unknown usage. These five failures cannot be assigned zero cost or relabeled as semantic errors. Input isolation audited all 63 payloads: task fields were question/columns only, no expected answers, reference queries/calls or expressibility labels supplied.

Bookkeeping correction: Redis v1 stopped after two completed calls because immutable save was used for the derived progress view. Commit 3286d65 changed only progress/PID replacement. Redis v2 copied the two exact payload/terminal pairs with SHA-256 receipts and reused them without dispatching again. v1 source, raw records and interruption remain private. No answers or grading were rewritten. Twenty targeted tests passed before the resumed frozen run.

Limits: 14 developer-known cases/project, six old supported goals, six old scope goals and two boundary probes; one provider model. A safe catalog refusal does not complete a graph-answerable task. These data are development controls, not an independent main effect, user study or noninferiority proof. P is the appropriate stronger service baseline for the following information-interface study. Detailed summaries and audit counts: E5_RESULTS.json.
