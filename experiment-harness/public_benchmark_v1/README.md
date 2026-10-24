# T1 public benchmark preparation

Status (2026-10-24): development infrastructure preflight passed. No model
prediction has been run, and no formal test protocol is frozen.

This directory is the new experiment branch's preparation code. Historical
service code, predictions and reference freezes are unchanged.

## Fixed upstream inputs

- Dataset: `megagonlabs/cypherbench`, revision
  `fd80c26a9f5447e4c9ba952cc70575544117306c`.
- Official source: `https://github.com/megagonlabs/cypherbench`, commit
  `4cc727d29acc78f8cea7c4daa4e2705ddf293b8b`.
- Official test snapshot SHA-256:
  `e55cb853bde5ad893a5dcfa2ba38118051c7d8faa9a0d91b3d77a097c13f72f4`.
- 8,534 training questions on four graphs; 2,348 test questions on seven
  other graphs. The preparation script checks that graph and question IDs
  do not overlap between splits.

## Scripts

1. `prepare_cypherbench.py --metadata-dir PATH --output-dir PATH
   --official-source PATH` verifies pinned assets, separates question-only
   runtime records from evaluator references, downloads schemas and the
   smallest full training graph, and writes hash receipts. Existing different
   outputs are not overwritten. Gold queries, answers and template metadata
   are excluded from runtime records by an explicit allowlist.
2. `bootstrap_remote.py --root PATH` installs a private Neo4j 5.26.32 / Java 21
   development runtime. It binds only loopback, uses a private database
   credential file, and does not modify system packages or GPU services.
   Its fixed owned-root check is intentional. When the filesystem is
   shared across instances, `execution_owner.json` binds execution to the
   selected hostname; another instance must not start this database.
   When remote package access is
   blocked, supply verified archives under `downloads/` and wheels under
   `wheels/`. Java metadata is persisted in `downloads/java21.json`.
3. `smoke_train_database.py --root PATH` verifies every transferred artifact,
   starts only its own database process, imports the full selected training
   graph, compares all node/edge properties, labels and endpoints, then
   executes three deterministic training references. The database is stopped
   in `finally`; per-attempt reports and a latest report are saved.
4. `python -m unittest discover -s experiment-harness/public_benchmark_v1
   -p test_preparation.py -v` checks leakage prevention, split separation,
   artifact tampering, duplicate tasks and immutable outputs.

Output data, credentials and runtime files belong outside Git. The runtime
record split is a runner contract, not an OS security boundary.

## Completed development checks

- Five preparation tests passed.
- The selected full training graph has 1,556 nodes and 1,525 directed edges.
  Every imported property, label and endpoint matched the source JSON.
- Three executed training references matched the distributed answer arrays
  using the upstream answer-equivalence helper. No gold-vs-identical-query
  short circuit was used.
- Initial smoke attempt failed because the adapter expected answer dictionaries
  while upstream `answer_json` stores row arrays. The adapter was corrected;
  the failed receipt was retained. No test prediction or model output was used
  to make that correction.
- No model calls, GPU allocations or formal accuracy results.

The native loader is a development deployment; these checks do not certify
equivalence to the official Docker image for every graph, type and query.
Database timings here are reference smoke timings, not method efficiency.

## Next work and boundaries

1. Finish the generic public-schema method adapter and the same-backbone direct
   and execution-feedback runners using training-only development material.
2. Validate the final-answer scoring adapter. Returning a service plan is not
   an answer; refusal is not correct execution.
3. Freeze the independent evaluation scope, prompts, method version, retries,
   scoring and call allowance before formal predictions.
4. Prioritize CypherBench, then matching MindTheQuery tasks and independent
   CoRE tasks. Audit one external specialist first, preferably CYQUARK-4B;
   do not assume weights or per-task outputs are available.
5. Preserve raw attempts and collect complete costs. Direct-feedback may reuse
   an identical direct first pass; deterministic execution controls may reuse
   plans only when their model inputs and feedback are unchanged.

The four-benchmark table is not authorization to run every candidate on every
full dataset. The existing call cap remains unchanged. Legacy background
queues must not be restarted by this preparation package.
