# Evidence and cost checkpoint, 22 October 2026

The continuation ledger contains 214 distinct attempts including probes, generation, all structural repairs and transport failures. All have terminal receipts; 36 lack provider usage and all 36 are HTTP/transport failures. Known usage sums to 1,007,474 tokens (853,564 prompt; 153,910 completion). The actual total is unknown. No verified provider tariff or invoice is available, so no monetary cost is reported. Earlier E1/E3 calls lie outside this new 1200-call continuation budget and are not included in this figure.

Recorded per-attempt dispatch time totals 5,513.316 seconds. This is a sum of observed HTTP attempt durations, not experiment wall time, DB latency or total engineering cost. An HTTP200 response can still be truncated; transport success alone does not establish a valid prediction.

E5 uses 63 unique attempts; E6 89 including the three original failed DuckDB requests; the compact development pilot 44; partial source prediction eight; original and grounded generation four each; probes two. These add to 214. Two cached E5 responses reused after a bookkeeping interruption are charged only once. First/repaired views share calls and must not be added as separate costs.

| Development diagnostic | Strong direct | Extended service | Interpretation |
|---|---:|---:|---|
| E6 Redis answerable | 12/12 | 12/12 | Equal observed result |
| E6 DuckDB recovery answerable | 10/12 | 10/12 | Direct: one wrong answer, one infrastructure outcome; service: two infrastructure outcomes |
| E6 both-project boundaries | 4/4 | 4/4 | Small developer-known boundary set |
| Redis compact answerable | 7/12 | 10/12 | Direct six infrastructure outcomes across all14 cases vs service three; not a clean method advantage |

Full-context Redis totals are 88,773 direct and 88,222 service tokens over the same14 goals, a difference of551 (about0.62%). That does not establish a practically stable cost advantage, and the full direct context contains service descriptions it cannot use. Compact contexts remove representation-inapplicable material while retaining direct schema and executable service contracts. Compact known sums are35,207 direct and35,350 service, with six and three unknown-usage attempts respectively. Comparing those partial totals as complete costs would be invalid.

No formal noninferiority or equivalence result follows from equal correctness on small developer sets. Available-response analyses are conditional on provider response availability, not an unbiased replacement for operational accuracy. Do not attribute provider failures to the algorithms. Do not treat a correct out-of-catalog refusal as completing a graph-answerable goal. Cost per correct response is undefined when required total usage is incomplete.

Independent source prediction stopped on Redis after eight attempts: five infrastructure failures, three valid service refusals. G evaluated three cases, P two, I three. No full independent comparison or held-out-project prediction exists at this checkpoint. The four-project reference checks are preparation only. No confidence interval for a representative task population is warranted.

Next bounded work is frozen independent/holdout completion when transport permits, then the same accounting update. No extra services, models or synthetic favorable tasks will be introduced solely to obtain a gain. The paper currently supports a limited empirical boundary analysis rather than superiority over general Text-to-Cypher or full RAG systems. Novelty and external validity remain material submission risks.
