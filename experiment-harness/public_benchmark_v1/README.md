# T1 public benchmark preparation

Status (2026-10-24): development infrastructure preflight passed. Executed-answer
scoring and serial attempt accounting components added and checked offline.
No model prediction has been run, and no formal test protocol is frozen.

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
   -p 'test_*.py' -v` checks leakage prevention, split separation,
   artifact tampering, answer semantics and immutable attempt accounting.
5. `answer_scoring.py` loads only pure result-equivalence functions from the
   hash-pinned official evaluator. It does not load the official query-text
   identity shortcut, run queries or call a model. The eventual runner must
   supply actual executed, normalized JSON row arrays. Refusal, truncation
   and execution failures cannot become correct answers, even for empty gold.
   Set `CH7_CYPHERBENCH_SOURCE` to the pinned official checkout for tests
   outside the canonical workspace layout.
6. `attempt_budget.py` requires an explicit approval file and serial shared
   ledger, preserves historical slots, supports verified path translation,
   recognizes the first legacy probe's special terminal name, and blocks
   pending attempts, changed requests, missing history or exceeded allowance.
   It performs no model requests. A leftover lock requires owner inspection;
   it is never silently cleared. The development transport below uses this
   accounting; the ledger by itself does not send requests.

## Executable preparation v1 (development only)

### T1 execution v4 follow-up

`fetch_graph_assets.py` downloads all seven pinned TEST graph assets in ascending
file-size order, verifies full size and LFS hash, and persists per-graph receipts.
It does not load reference answers, start a DB, change the old manifest or make
predictions. Failed partials remain unpromoted and require an audit.

The development runner now accepts a predeclared `--train-tasks` runtime-only
file. Every record must exactly match a unique question in the hash-pinned TRAIN
catalog and the deployed TRAIN graph. TEST tasks are rejected. Task-file hashes
are recorded before calls. The original three-task regression is the default.

`--entity-candidates` supplies graph-derived lexical name/alias candidates to
both arms unchanged. Inflection handling affects ranking only, never rewrites
the question or silently changes a model plan. Candidates can be ambiguous and
incomplete. This is shared grounding, not gold linking; its cost and protocol
difference from schema-only reported baselines must be disclosed.

For new development validation use matched `--max-output-tokens 8192` and
`--read-timeout 240`. The original 4096/120 settings stay the defaults for old
run provenance; no historical run is rewritten. Every transport policy records
the actual values. These changes need development checks before formal freeze.
See the canonical discussion/T1_EXECUTION_PLAN_V4_20261024.md outside this tree.

`dev_preparation.py` has no formal-test mode. It admits exactly the three pinned
TRAIN smoke questions and refuses a dirty source checkout. Run it first without
`--models`: this checks the owned remote database, constructed disjoint/overlap
fixtures against independent Python references, and three distributed TRAIN
references. All are development checks, not main-table scores.

The remote directory `preparation_v1_code` must contain `dev_database_worker.py`,
`cypher_read_guard.py`, and `smoke_train_database.py`. The worker verifies the
shared-mount owner/hostname and training graph hash, starts only its private CPU
Neo4j runtime, and stops it on stdin EOF. An occupied port blocks startup.

`schema_services.py` supplies a schema-typed finite v0.1 plan language with short
directed graph patterns, typed filters, projections, aggregation, and at most two
entity-set inputs. A union count uses branch counts only when incompatible
scalar equalities prove the answer entity sets disjoint. Otherwise entity IDs
and attached payloads are retrieved. The full-detail control uses the same plan.
Unsupported operators stay explicit: no OPTIONAL MATCH, variable-length paths,
subqueries, arbitrary expressions, HAVING, or multi-stage aggregation. This is
an initial candidate, not the established v16 method or a universal translator.

With `--models`, the runner uses the same supplied model/settings for Direct and
Ours; each path has at most one format/compile/execution feedback pass. Reference
answers are read only after predictions. A valid wrong answer is not retried.
The conservative read guard disallows procedures and scoped CALL syntax in v1;
these restrictions require review before claiming a strong formal baseline.

`model_transport.py` uses explicit UTF-8 HTTP payloads, no SDK retries, immutable
request/terminal records and the canonical 12,000 cumulative ledger. One run is
limited to at most 12 physical calls. Transient failures retry the identical body
after 10 seconds; three consecutive infrastructure failures stop the batch.
Every attempt is charged, and unknown usage stays unknown. An interrupted or
stopped run is not automatically restarted. Private credentials are never saved
with request payloads or committed to Git.

The current 33 local checks cover schema validation, disjointness/overlap,
full-detail agreement, output completeness, read guards, UTF-8, bounded retries,
history/cap enforcement, and official answer-equivalence semantics. They do not
replace real database and model development checks. Every preparation summary
keeps `full_main_table_ready=false`; formal admission needs a fresh independent
review of all required gates.

The initial real TRAIN model preparation used source commit `a4fc612` and v0.1.
All seven physical calls returned HTTP200. Direct answered 3/3 smoke tasks;
Ours answered 1/3, returned a wrong empty answer on one, and exhausted the 4096
completion-token allowance twice on the third. These are developer-visible
diagnostics, not independent paper results or an overall superiority estimate.
The wrong plan equated a plural question phrase with a singular graph entity
name. No per-question literal patch or hidden gold-driven repair was applied.
Both valid Ours plans agreed with their full-detail replay, including the wrong
one: execution strategy agreement is not natural-language correctness.

v0.1.1 hardens literal type validation and persists reference-execution receipts;
it does not fix entity binding or claim improved model results. Those issues,
semantic coverage and nontrivial comparison against a simple correct policy must
be resolved on development material before formal main-table admission.

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

## Offline execution-component checks

- All 17 preparation/execution-component checks passed, including official
  bag/order/column semantics, empty answer versus refusal, metric tampering,
  failed attempts charged once, pending-call blocking and migrated legacy
  accounting. Comparator tests use real hash-pinned upstream functions.
- All 8,534 TRAIN reference answer arrays passed result-shape normalization.
  A small self-consistency check is evaluator preflight only, not accuracy.
- A read-only inspection of the canonical historical ledger found 364 unique
  reservations, no pending terminal records, cumulative cap 12,000 and 11,636
  unreserved slots. No new slot was allocated. The approval is scoped to this
  CypherBench stage; other benchmark/backbone needs remain separate.
- Generic method adaptation, direct/feedback dispatch, test-graph deployment,
  author-method reproducibility and formal freezes remain outstanding.

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
   OSDB-STKG tasks. Audit one external specialist first, preferably CYQUARK-4B;
   do not assume weights or per-task outputs are available.
5. Preserve raw attempts and collect complete costs. Direct-feedback may reuse
   an identical direct first pass; deterministic execution controls may reuse
   plans only when their model inputs and feedback are unchanged.

The four-benchmark table is not authorization to run every candidate on every
full dataset. Current approval raises the cumulative attempt cap to 12,000,
retaining all 364 historical reservations. Legacy background queues must not
be restarted by this preparation package. A completed CypherBench column alone
is not automatically completion of the entire multi-benchmark main table.
