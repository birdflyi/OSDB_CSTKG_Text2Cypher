# Offline graph and case capacity

No `parallel` field means the existing serial route. Parallel execution is an
explicit frozen configuration change, not an automatic increase in model usage.

```json
{
  "parallel": {
    "max_graphs": 4,
    "max_preparing": 2,
    "case_workers": 2,
    "global_case_workers": 8,
    "cpu_budget": 16,
    "memory_gb_budget": 128
  }
}
```

These are configuration examples, not measured capacity or confirmed allocation.
The CPU and memory values are admission reservations, not operating-system
enforcement. Freeze the actual host/cgroup allocation separately. Backends must
apply their declared engine memory, thread and process settings.

Each graph route declares resources, with a benchmark-wide fallback:

```json
{
  "resources": {"cpu": 4, "memory_gb": 16},
  "graphs": {
    "example": {
      "resources": {"cpu": 4, "memory_gb": 24},
      "command": ["python", "worker.py", "graph-settings.json"]
    }
  }
}
```

More than one resident graph, or either explicit budget, requires explicit
nonzero resource declarations. Reservations include opening and closing
instances. The backend must validate distinct ports, owned runtime directories
and locks before opening services. The resource manager does not make fixed-port
legacy workers safe to run together.

## Lifetime and limits

`OfflineRuntime(root, backend_factory, parallel)` is a context manager. Its graph
pool keys each instance by benchmark, graph, shared settings and the graph route.
Backend startup receives a fresh `root/graphs/<identity>-<generation>` directory.
Case output stays in the caller's existing `output/<cell>/<position>` layout.

Graph references are acquired before use and released after all active case
threads have finished. Idle instances stay resident for feedback reuse. When a
new graph does not fit, only an unleased least-recently-used instance is closed.
Its capacity is not released until cleanup succeeds. Closing the runtime waits
for active leases; it does not terminate active requests to claim free capacity.
An ownership/cleanup failure prevents capacity reuse and is not retried forever.

The effective case width is the minimum of configured `case_workers`, backend
`max_case_workers`, and `global_case_workers`. An undeclared backend capability
defaults to one. The same instance cap applies even when initial and feedback
consumers share it. Runtime snapshots record the effective widths and observed
peaks. A position is a work unit: its methods retain dependency order, and its
actual answer is saved before its score or controls. Other positions can run
concurrently. Submission is completion-driven with a bounded window.

Backend methods `execute`, `control` and `score` must isolate case state before
declaring capability above one. Optional `finish_case(row)` runs on the same
thread after each method, including errors, to release unscored transactions.
Native identity comparison must keep method reads and reference reads within
the same owning transaction/thread. A mutex around individual RPC calls does
not establish this property.

## Shared consumption and feedback

```python
with OfflineRuntime(runtime_root, open_backend, limits) as runtime:
    first = run(initial_config, runtime=runtime, on_feedback=enqueue)
    second = run(feedback_config, runtime=runtime)
```

Both configurations must use identical runtime limits and their own output
directories. The runtime can be shared by concurrent consumers. The example is
sequential only to show lifetime; a coordinator can feed queued replies earlier.

`on_feedback(pin)` is called only after `feedback_request.json` is durably saved.
The pin contains its path and SHA256. The callback must enqueue and return; it
must not synchronously wait for model execution or another graph lease. Callback
failure is a persistence failure, not a silently lost feedback request. A later
consumer still needs the final response pin and normal budget/identity checks.
No model client or model quota is managed in this module.

The chain enables queued streaming feedback explicitly:

```json
{"streaming_feedback": {"batch_size": 8, "coalesce_seconds": 0.05}}
```

`feedback_stream.py` owns one model thread and one response consumer thread.
After admission, the model thread starts a small batch when durable notifications
arrive. It does not wait for initial DB consumption or prior feedback DB work.
Within a batch, finished model requests release slots for waiting requests. The
next batch starts only after the current batch has terminated. This is not a
claim of continuous cross-batch replenishment or sustained 32-request load.

Only that model thread opens the paid ledger. `RequestSession` holds this owner
for the whole phase. Legacy scanning, full admission audit and recovery sealing
run once. Subsequent batches use the indexed charged count and only read newly
completed attempts. Before reusing a reply, its selected history is hash-checked.
Each batch has its own frozen configuration, admission checkpoint pointer, raw
replies and consumption output. This session accepts only Direct-feedback
queues. Dynamic multi-stage methods keep the original full recovery route.

The cold admission and final full audit have separate timing records; either can
take minutes with large historical ledgers. They are not request-throughput time.
Final audit failure retains unresolved ownership and makes the phase incomplete.
The original request template's `physical_attempt_cap` applies to the whole
feedback phase. Each new batch receives only the remaining allowance, calculated
from actual physical ledger slots under the phase directory, including failures.
Neither successful reply counts nor number of batches resets that allowance.

On a hard stop, the phase writes its own `DRAIN`, which the request scheduler
observes through optional `drain_paths` and the shared in-memory stop event. Thus
a failed DRAIN write still stops new dispatch. It lets in-flight
requests settle. Pending notifications, incomplete batches and known charges are
saved separately. A missing physical terminal is not treated as a free attempt.
Budget exhaustion can leave initial cached DB work running; identity or evidence
failures stop new DB work as well. The shared graph pool closes only after initial
consumption, model draining and feedback consumption have all joined. This is
explicit new behavior; chains without `streaming_feedback` keep their previous
sequential feedback route.

Ordinary case errors remain case results. Ordinary graph startup errors mark
the affected graph's positions and allow other graphs to continue. Identity,
persistence and ownership failures stop new submissions and drain active work.
`partial_summary.json` retains the entire selected denominator; cases with an
already saved answer but incomplete remaining work are marked separately.
Old final files and old results are not overwritten. A successful run writes
`summary.json` after its owned runtime has closed.

## Offline checks

`common/tests/test_offline_parallel_runtime.py` exercises preparation/resident
limits, CPU/memory reservation, idle eviction, shared graph reuse, cross-graph
overlap, global and instance case limits, 50 positions with 10 rolling slots,
hard-stop draining, ordinary errors, dependency order and durable feedback
notification. These tests do not call a model, SSH or database, and do not claim
real backend performance validation.
