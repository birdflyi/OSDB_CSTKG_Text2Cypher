# Same-plan native set-count development control

`analysis/osdb_native_set_count_control_dev.py` is an isolated, pure compiler.
It does not execute queries, call a model, or change the existing campaign.

The current experiment access policy allows native read-only queries over the
same complete database. A client-side identity-transfer control alone cannot
establish an advantage over correct in-database composition. This additional
control addresses that design gap without changing the frozen method/results.

## Scope

Two complete, identity-only sets of the same entity type and identity kind;
union, intersection, or left difference; final count output. Existing observed
schema compilation and key contracts remain required. Aggregated, ordered,
limited, nullable-key, mixed-type, or payload projections are rejected as not
applicable to this control, not as model failures. Other shapes remain outside
this initial diagnostic scope and must not disappear from end-to-end results.

Each branch selects distinct identities with an independent parameter namespace.
The native query attaches source tags, groups by identity, selects the required
tag pattern, and counts inside the database. It transfers the final count rather
than the input identity sets. It requires no disjointness certificate and is not
claimed to produce the optimal native execution plan.

## Evidence limits

- Requires an admitted, immutable graph with complete non-null unique primary
  identities. Compilation does not certify the graph or the access policy.
- Source imports, source parameters, nanosecond time literals and quoted query
  content are preserved. No reference answers or task-specific rewrites are used.
- Eight offline checks passed. An independent SQLite grouping calculation was
  checked against Python set operations on 225 pairs of duplicate/empty/overlap
  bags across three operations (675 comparisons). This checks the algebra,
  **not the emitted Cypher on Neo4j** and not 675 independent benchmark tasks.
- Actual Neo4j EXPLAIN/execution, result typing/completeness, resource bounds,
  and independent cost timing remain unaccepted. No performance results exist.
- Same-plan controls share planning. Extra physical model calls are zero;
  independent deployment still requires the corresponding planning cost.
- Respect native internal resource use as well as output/transfer limits. A
  one-row answer does not mean the database processed one row.

The existing simple-correct control aliases the current contract policy. That
equivalence is not evidence of an independent algorithmic increment. Keep full
end-to-end and fixed-correct-plan comparisons separate, and report coverage,
errors, and all attempts alongside costs.

From `operations/analysis`, offline checks use:

```text
python -m unittest test_osdb_native_set_count_control_dev -v
```
