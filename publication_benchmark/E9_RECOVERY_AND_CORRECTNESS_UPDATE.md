# Recovery and correctness update, 22 October 2026

## Transport recovery

The single delayed probe succeeded at about03:58 local, but the separately named Redis source v2 batch then stopped after three consecutive new HTTP500 connection failures. The finite remaining queue stopped before every later source/grounded/boundary/holdout batch. A small successful probe therefore did not establish stable experimental transport. No automatic source v3 is dispatched.

Recovery made five new requests: one direct branch answer, one protocol-service refusal and three infrastructure failures. It reused all three prior valid refusals byte-for-byte without reserving new budget slots. Exact payload equality and all three cache request/terminal hash pairs were verified. Valid wrong/refused outputs were not rerolled. Original failed requests remain charged. All220unique structured payloads now pass the protected-reference-field isolation audit; three probe user messages are plain text. This audit does not prove absence of every textual leakage channel or model contamination.

| Redis source v2 view | Evaluated / planned | Exact snapshot answers | Safe catalog refusals | Infrastructure outcomes |
|---|---:|---:|---:|---:|
| G1 | 3/8 | 1 | 0 | 2 |
| P1 | 2/8 | 0 | 2 | 0 |
| I1 | 3/8 | 0 | 2 | 1 |

This is incomplete evaluation, not a completed accuracy contest. Its eight call records contain three cached calls and five new calls. Across source v1+v2 there are13unique actual requests, eight unknown-usage failures and15,991known tokens. Adding the v1 and v2 known sums would count the three cached responses twice. First/repaired views also share calls and must not be added.

The continuation ledger now has220unique attempts,39unknown-usage/HTTP-or-transport failures, no pending attempts and1,014,655known tokens (859,352prompt;155,303completion). Recorded dispatch duration sums to5,579.206seconds. Total token/currency cost remains unknown; no verified tariff or invoice exists. These are query/result-stage measurements, not end-to-end conversation, RAG or maintenance costs. Earlier E1/E3 calls remain separate from this continuation budget.

## Offline reference assurance

Two synthetic reference worlds were prepared without reading new model predictions. Manual answers and the unchanged CSV references agree on24goal-world checks. Separately written Cypher on a new isolated owned database also agrees on24/24. All26declared mutant-world executions differ from gold, covering13mutation definitions. Duplicate records, distinct identities, time boundaries, first/last events, self-response, comment-only activity, multi-branch joins and literal direction are represented. These are finite evaluator checks, not model successes or general equivalence proofs. The original tasks and systems were not changed.

## One actual semantic defect hidden by a snapshot match

After those checks, inspection of the returned G branch query found that it bound `B_156018` instead of the explicit requested `B_156018:`. A separate post-hoc synthetic transaction with branches `B_156018:main` and `B_1560189:foreign` made the exact query return2created branches while the literal contract returns1. The transaction was rolled back; the original snapshot score stays unchanged. See the reference branch's E7_OBSERVED_PREFIX_SCOPE_REPORT.md.

This finding is not a prospective method-effect estimate. Both service catalogs refuse the branch task, so it cannot establish service superiority. It demonstrates why matching a sparse actual snapshot does not by itself establish scope semantics. Typed argument validation also does not ensure that the model copied a requested literal condition correctly. No new service or gold-aware repair was added after this diagnosis.

## Remaining gate

Independent and preselected-project predictions are still incomplete. Existing recovery v2 is retained without automatic v3. The interface resource question is already pending; do not repeat it or silently change models. Later recorded low-frequency connectivity checks are allowed only after at least one hour without dispatch. Do not restart the existing queue or change the frozen workload because of these outcomes. A credible next execution needs stable transport and a separately documented plan that preserves every existing valid output. The first manuscript remains a limited review draft with incomplete effects; none of these evaluator checks establishes publication readiness.
