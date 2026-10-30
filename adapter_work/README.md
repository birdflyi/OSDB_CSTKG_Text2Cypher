# Adapter preparation and runtime boundaries

Each method/benchmark directory exposes a thin adapter. Shared behavior lives in
`common`; adapters do not own HTTP clients, concurrency pools or billing ledgers.

## Prepare the fixed inventory

```powershell
python adapter_work/preparation.py --materials <preparation-material-root> --output <new-directory>
```

This reads the V22 positions and V24/V25 contracts, verifies public input hashes,
and writes the 45-cell construction inventory. It makes no model or DB requests.
The inventory contains 900 logical positions and a fixed 225-position full-flow
subset. Direct-feedback shares Direct's initial result; execution controls depend
on the corresponding Ours plan. With all trained models available, the normal
request count is 600 physical stages, including Cypher-RI's second stage.

## Send an adapter path

`common.dispatch.generate_path` connects an adapter to the existing
`engine.scheduler.Scheduler`. It persists each model result through that scheduler
before deriving the next request. RI uses raw completion prefixes and actual token
usage; its endpoint must support the protocol. Do not route RI to a normal chat
model merely to make a request succeed.

The matrix lifecycle, dependency receipts, resume reconciliation and DB-feedback
queue still need production integration. This function is not a complete matrix
runner. Rehearsal uses a fake model and is not evidence of live throughput or
completed experiments.

## Consume saved predictions

`common.consumption` saves the method's actual final answer before control replay
and scoring. Multi-query compiled plans go through `runtime.execute_plan`; the
runtime owns graph leases, compile receipts, database drivers and references.
Storage and identity errors must remain visible. Unsupported predictions stay in
the denominator and are not correct answers.

Prompt profiles in `prompts` are frozen engineering settings. Smith/CYQUARK need
trained assets and training-compatible prompts before paper-faithful evaluation;
the common Direct profile is only an explicitly labeled engineering choice.
Cypher-RI's candidate checkpoint and protocol reconstruction must also be labeled.

## Offline verification

```powershell
python -m pytest engine/tests adapter_work -q --tb=short
```

These tests use fake transports and runtime fixtures. They do not certify live
services, all database backends, or scientific equivalence to author implementations.
