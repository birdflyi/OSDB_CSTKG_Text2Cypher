# Native case lanes

MTQ and ZOGRASCOPE answers can contain native entity identities. A method query
and its reference query must use the same live capture, session and transaction.
A general thread pool cannot safely move that capture between threads.

`native_cases.CaseLanes` therefore owns 1–4 fixed threads. Each occupied lane
belongs to one case until it is scored, explicitly finished, or its idle lease
expires. Only the Neo4j driver and the locked, lazily loaded reference population
are shared. Each lane has a separate `NativeWorker` handler with its own
`capture`, `last` answer and case evidence directory.

## Protocol

All new commands carry the same `identity`: `task_id`, `method`, `benchmark`,
`graph`, `position_id`. The RPC request `id` still correlates each command/reply.
The case key is a SHA256 of the canonical identity, not the answer SHA alone.

1. `execute(identity, cypher/plan)` performs the method work, saves the remote
   final answer, and returns that answer and its SHA. No reference is read.
2. The caller persists its local final answer. Only then does it issue
   `score(identity, answer_sha256)`. The original lane verifies the identity and
   SHA, reads the reference in the same capture, and closes the capture.
3. The caller always invokes `backend.finish_case(row)` in `finally`, including
   local write failures and answers which do not need scoring. The corresponding
   `finish_case(identity)` command closes the case or confirms it is already
   closed. No query is retried by this protocol.

An incomplete answer and an ordinary case error close their lane. A complete
answer waiting for its score has a default idle lease of 300 seconds, configurable
with `case_idle_seconds`. The input loop continues receiving score/finish commands
while other lanes execute. Each lane queue is bounded at two commands. The caller
must respect READY `max_case_workers`; excess execute commands are rejected
before executing a query, rather than blocking the only input reader.

Cleanup failure quarantines the case. Its slot stays occupied and new execute
commands are rejected. Other active cases may score or finish. Shutdown with a
quarantined case raises an integrity error; shutdown exceeding its 15-second
grace raises a timeout. These conditions are not reported as normal closure.
The owning process may then close its own driver/database as abnormal cleanup.
It never clears another process's lock or claims the unresolved query completed.

The default is one lane. Single-lane score requests without identity remain
compatible with the old protocol. New consumers send identity even for one lane.
READY advertises `case_protocol: thread-affine-v1`; old workers lacking that field
do not receive a new finish command from the consumer.

## Resource and reopen configuration

- Native settings `workers` accepts 1–4; the driver pool is `workers + 2`.
- Native server memory remains fixed at heap initial 256m, heap maximum 512m,
  page cache 256m. CPU/memory tuning was not added in this change.
- OSDB uses a distinct per-case executor and explicit per-graph HTTP/Bolt ports
  and lock paths. The driver is owned by the graph lease, not each case.
- CB/T2 comparator/index objects are initialized once per backend, after the
  first local final-answer seal. They stay private to scoring.
- A frozen RPC command may explicitly contain `{instance_id}`. The consumer
  replaces it with a digest of the unique runtime output directory. Worker
  `--instance-id` appends that safe token to its configured output root. Each
  LRU reopen must therefore use a new runtime output directory. Commands without
  a placeholder keep the old behavior; a fixed output path cannot be reopened
  into itself.

Offline tests use scripted captures and transports. They verify thread affinity,
out-of-order score correlation, overlap, post-seal reference access, cleanup
failures, bounded shutdown, resource argument forwarding and unique reopen
directories. They do not establish real database throughput or deployment health.
