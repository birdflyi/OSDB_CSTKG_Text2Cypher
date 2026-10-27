# Cost-separated development paths

The preserved dispatcher records `path_wall_seconds` around both the method and
shared full-detail control replay. That field must not be treated as independent
method latency. The new isolated files are:

- `analysis/osdb_independent_path_cost_dev.py`: monotonic nested spans and one
  existing-policy initial/optional-repair path; method outcome persists first.
- `analysis/osdb_campaign_dispatch_cost_dev.py`: derives the existing run loop,
  uses shared-lock-aware reads and method-only latency observations. Live mode
  requires a `LimitedModelClient` with an active shared central gate; worker count
  must fit admitted permits and can never exceed 64. These checks do not make a
  graph factory or a model endpoint admitted.

## Measurements

Method timing ends after the final answer/outcome and before any control replay.
It includes the initial attempt, permitted repair, validation, graph-lock waits,
and client execution under observed contention. It is not isolated deployment
latency: other workers and their controls can still cause resource contention.

Full-detail replay has a separate root span and no extra model generation.
The current simple-safe-rule control remains an explicit Ours alias, not a
separately measured deployment or an independently generated model baseline.
The native set-count control remains pending live engine acceptance and is not
automatically enabled by this dispatcher.

Spans record inclusive and exclusive time; use exclusive values for additive
accounting and do not add an inclusive parent to its children. Output fields:

- method root wall, including any feedback and contention;
- model-client `generate` wall, including permit/budget/transport/record overhead;
- shared read-lock wait;
- DB-executor client wall (EXPLAIN/read/normalization/evidence overhead included);
- answer processing excluding nested read-lock/DB-executor spans;
- observed return rows/bytes, and physical statements only if measured.

The future graph factory can supply `physical_statement_count` as a cheap,
in-process, read-only counter. It must not perform I/O. A missing/broken counter
produces unknown physical counts and never blocks or retries the actual read.
One executor callback may issue EXPLAIN plus a read, so it is not counted as one
physical DB query. Missing bytes/counts and open spans remain unknown, not zero.

Server compute, first-token latency, pure decode speed, cache state and money
remain unknown. No per-token/per-row sampling, GPU probing, or display callback
is introduced. Historical records are not retroactively disaggregated.

## Failure and acceptance limits

The finalized method outcome is saved in `method_finals/` before controls run.
Later control crashes retain that outcome and a separate error span; the outer
campaign still records a runner error and stops/drains. A cost record alone is
not a completed path or a successful answer. Existing posthoc/launch integration
must explicitly handle these files before live acceptance.

Twelve offline checks passed on the final version, including a synthetic two-path
dispatch (2 fake generations, 3 fake read callbacks, 46 deliberately undispatched
paths), a control crash, repair/withholding, unknown observations, lock release,
and refusal of live mode without the central client. No real model/DB/network was
used. The older test methods were not rerun. Earlier 8- and 2-check iterations
also passed; the final rerun followed counter/central-gate changes.

These files are not deployed, do not launch a graph, and have no live acceptance.
Actual graph factory, owner/snapshot/deadline checks, source/input freeze, live
model integration, independent cost experiments and independent tasks remain.
