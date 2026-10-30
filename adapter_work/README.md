# Adapter preparation and runtime boundaries

Each method/benchmark directory exposes a thin adapter. Shared behavior lives in
`common`; adapters do not own HTTP clients, concurrency pools or billing ledgers.

## Prepare the fixed inventory

```powershell
python adapter_work/preparation.py --materials <preparation-material-root> --output <new-directory>
```

The default preserves the historical 45-cell inventory. For the current scope,
add `--scope adapter_work/smoke_scope.json`: CypherSmith is excluded by the user's
research-scope decision. This gives 40 cells, 800 logical positions, and a fixed
200-position full-flow subset. Historical materials are not deleted or relabeled
as scientifically inapplicable.

Preparation reads V22 positions and V24/V25 contracts and verifies public input
hashes. It makes no model or DB requests. Direct-feedback shares Direct's initial
result; controls depend on the corresponding Ours plan. With all trained models
available, the current normal request count is 500 physical stages, including
100 Cypher-RI continuations. Retries and operational feedback are additional.

## Send an adapter path

`common.dispatch.generate_path` connects an adapter to the existing
`engine.scheduler.Scheduler`. It persists each model result through that scheduler
before deriving the next request. RI uses raw completion prefixes and actual token
usage; its endpoint must support the protocol. Do not route RI to a normal chat
model merely to make a request succeed.

`request_pipeline.py` provides an independent request-only matrix entry point:

```powershell
python -m adapter_work.request_pipeline audit <frozen-config.json>
python -m adapter_work.request_pipeline simulate <frozen-config.json>
```

The entry point reuses the existing scheduler, pool, journal and physical ledger.
RI stages return through the same scheduler. Jobs from later cells can fill free
slots while earlier requests finish. Dependency positions only point to persisted
Direct/Ours predictions; they are not DB executions or scores. Resource-pending
author positions stay in the denominator. CYQUARK requires an exact target-graph
checkpoint binding, not one generic model substituted across all graphs.

`run` is the live command, but preparation configs leave `execute_authorized`
false. New live runs require a frozen inventory, actual services and the approved
scope. A scope with previous attempts requires an audited recovery manifest.
Arbitrary crash reconstruction and the full DB-feedback queue are not implemented
by this entry point. Simulation uses a fake model and is not live throughput or
experimental evidence.

## Consume saved predictions

`common.consumption` saves the method's actual final answer before control replay
and scoring. Multi-query compiled plans go through `runtime.execute_plan`; the
runtime owns graph leases, compile receipts, database drivers and references.
Storage and identity errors must remain visible. Unsupported predictions stay in
the denominator and are not correct answers.

Prompt profiles in `prompts` are frozen engineering settings. Author methods need
trained assets and training-compatible prompts before paper-faithful evaluation;
the common Direct profile is only an explicitly labeled engineering choice.
Cypher-RI's candidate checkpoint and protocol reconstruction must also be labeled.

## CYQUARK training preparation

`cyquark_training.py` validates generated training records and a single-GPU SFT
recipe. It requires graph-bound provenance, three paraphrases, per-paraphrase
logical verification and explicit held-out-question exclusion. Exact-question
exclusion alone does not establish absence of semantic contamination. Tokenization
keeps the full completion and rejects overlength records instead of silently
truncating them. Loss masking, non-thinking mode, optimizer, sequence length and
seed are our explicit engineering choices where the paper does not fix them.

The module does not generate a corpus or download assets on import. Training is
opt-in and requires exactly one visible BF16-capable GPU. The paper's learning
rate, weight decay, epoch count and effective batch size are kept in this profile.
No GPU training has been validated by the offline tests. A generated corpus,
training-compatible inference prompt, checkpoint file manifest, dependency
freeze and device acceptance are still required before author-method execution.

## Offline verification

```powershell
python -m pytest engine/tests adapter_work -q --tb=short
```

These tests use fake transports and runtime fixtures. They do not certify live
services, all database backends, or scientific equivalence to author implementations.
