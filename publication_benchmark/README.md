# Publication-first baseline pilot

This code establishes comparable task specifications, scoring, and two baseline
implementations before selecting a proposed architecture. It does not import the
legacy NL parser, query templates, repair module, or model-generated references.

## Current arms

- **BM25_FACT_RAG**: top-24 retrieval over serialized nodes/edges from the same
  source graph, followed by evidence-only structured answering.
- **SCHEMA_GRAPH_QA**: zero-shot schema-aware Text-to-Cypher, read-only execution
  on a dedicated Neo4j graph, followed by result-grounded structured answering.

These are baseline implementations, not exact reproductions of published RepoQA,
Microsoft GraphRAG, dense RAG, or CYGNET. Add appropriately configured stronger
baselines before making a comparative contribution claim. Each method's data
access, token usage and call count must be reported; they are not identical.

The initial eight questions are controlled DEVELOPMENT specifications. Expected
tables and decisions are computed directly from the CSV graph. They check record
multiplicity, multi-field output, time-conditioned grouping, complete counts,
composite paths, zero counts, missing anchors and unsupported semantics. They do
not establish real-user representativeness or independent generalization.

Baseline messages contain public tasks and format columns only. Expected results,
task-group labels and qualification gold remain in the scoring process. Corpus
retrieval only sees serialized data and questions. No LLM supplies scoring gold.

## Scoring

Decision and table correctness, supported-case coverage, wrong answered cases,
boundary acceptance, and evidence-reference validity are separate quantities.
Unordered row comparison preserves duplicate records. Counts use JSON numbers.
Evidence IDs must come from actually retrieved facts or actual query execution.
Valid citation IDs alone do not prove that every natural-language claim follows.
Result agreement on one graph does not prove query-semantic equivalence: formal
evaluation also needs discriminating instances/graphs and obligation checks.

An all-abstain system gets zero supported answers. A schema or execution error
is not zero facts. Lack of retrieved evidence is not an unsupported task. Retain
all these states; do not hide errors by silently changing the denominator.

## Execution

Use Python with requests and pytest. Configuration stays outside the repository
and supplies endpoint, model, api_key, temperature, top_p and reasoning_effort.
Artifacts must also stay outside the checkout.

```powershell
python -m pytest test_baselines.py -q
python run_baselines.py --config PRIVATE_CONFIG --data EXECUTION_CSV_DIRECTORY --graph-receipt PRIVATE_IMPORT_RECEIPT --output NEW_PRIVATE_RUN --batch-size 2
```

The dedicated Neo4j instance must be running on 127.0.0.1:17475, with the exact
database identity and source file hashes in the receipt. This script does not
start, modify or import a database. It only performs bounded read-only evaluation
queries, EXPLAIN, and an identity check. Stop the owned instance after use.

Completed stages can be resumed only with identical code/config/input manifests.
Requests and responses are immutable. Incomplete responses and transport errors
are retained and are not silently retried. Fixed batching is part of the manifest.
The current two-question batching plan permits at most 12 dispatches for this
eight-question pilot. Failed provider attempts may have unknown remote costs.

## Reused data asset

`graph-migration/projection/` was copied from the previous local working tree only
to reuse its offline D1 projection for additional projects. Provenance and hashes
are in `reused_assets.json`; its imported model/normalizer/exporter dependencies
were checked byte-identical against this checkout before reuse.

The second raw dataset acquired in this session is DuckDB 2023 from
[Zenodo 18817348](https://zenodo.org/records/18817348), CC-BY-4.0, containing
86,136 relation records with the same field layout as Redis. Source and projection
artifacts are private local files, not checked in. Its projected execution graph
has not yet been runtime-validated or evaluated by this runner.

## Failure accounting and retained peer assets

`run_resilient_baselines.py` preserves terminal response caches and records a
failed batch as explicit runner-generated ERROR rows. It never completes model
answers or retries the failed dispatch. A pending request alone is not terminal.
Failures retain the planned denominator, but infrastructure/protocol failures
must be reported separately from semantic mistakes. An outage is not evidence
that one method is worse. Fresh dispatches, cached calls and attempts with
unknown supplier cost must be distinguished in the cost record.

The parser may extract one complete fenced JSON block and convert exact-keyed
row dictionaries into the public column order. This changes presentation only;
it does not fill fields, change counts or salvage truncated JSON. The original
model responses remain retained. The current low-reasoning configuration and
batching policy are development settings, not a claimed method improvement.

`peer_assets/repoqa_revision1/` contains inert unmodified reference prompts and
provenance from the actual RepoQA revision-1 Zenodo replication package. These
reference files remain unchanged. The separate few-shot arm below adapts their
prompt structure; the original RepoQA system has not been independently reproduced.

## Three-arm continuation

`run_comparison.py` runs the same eight development specifications through:
BM25 fact RAG, zero-shot graph QA, and RepoQA-inspired few-shot graph QA. The
few-shot arm adapts the published prompt structure with schematic examples for
our schema. It is not the original RepoQA implementation, dataset or score.
Missing-anchor clarification is explicit, rather than adopting the peer prompt's
most-probable-interpretation heuristic. No test identifiers, results or gold are
in the examples. Four examples are fixed in source before the run.

Each fresh run has batch size 2, maximum 12,000 completion tokens per dispatch,
no automatic retries, and at most 20 dispatches. No previous answer caches are
selected for this run. Increasing the output budget is an operational setting,
not a method contribution. Raw outputs and all failures stay in the artifacts.
Graph arms call the answer model only for successful executions; nonanswer
retrieval decisions are propagated. Both graph-query-result and final-answer
scores are saved, so answer generation cannot conceal query-result errors.
A single terminal Cypher semicolon is allowed; multi-statement input is blocked.
JSON integral floats equal integers by value; strings and booleans are distinct.

Before model dispatch, all six supported Python/CSV oracle results must agree
with independently executed private reference Cypher. This checks graph import
and reference consistency, not general semantic equivalence of generated queries.

```powershell
python -m pytest test_baselines.py test_comparison.py -q
python run_comparison.py --config PRIVATE_CONFIG --data EXECUTION_CSV_DIRECTORY --graph-receipt PRIVATE_IMPORT_RECEIPT --output NEW_PRIVATE_RUN
```

`import_execution_graph.py` imports a trusted projection only into a verified
empty dedicated local instance. It preserves node and parallel-edge UIDs, refuses
repeated or partial import histories, and writes graph identity and hash receipts.
It never removes existing data. Stop and verify the first project instance before
starting a second project's separately configured instance on the same local port.

## Bounded infrastructure recovery

`run_with_transport_recovery.py` runs the same fixed three-arm comparison with
at most three transport attempts per stage. It retries only HTTP 429/5xx or
recorded transport errors, with 30/60-second minimum backoff since the prior
failure. It does not retry a complete wrong answer, JSON format failure, or
completion-length failure. Identical completed stage caches can be reused with
request equality checks. Pending unrecorded dispatches are never redispatched.

Every attempt remains in `attempts/`; `calls/` points to the first successful
response or terminal failure. `recovery_usage.json` counts all fresh attempts,
including failed attempts and missing usage. The base `summary.json` counts only
selected responses, so it must not be used alone as the all-attempt cost total.
Redis uses dedicated port 17475; the separate DuckDB instance uses 17476. A
private source snapshot preserves the exact version loaded for long-running runs.

## Mechanism study and evaluator discrimination

The current research decision and prioritized experiments are in
`MECHANISM_STUDY.md`. `audit_discrimination.py` checks seven declared faulty
semantics with CSV evaluators and isolated synthetic counterfactual witnesses.
It does not measure model errors or prove generated-query equivalence. The
synthetic records are never written into the observed project graph.
`test_discrimination.py` checks witness relevance, duplicate records, direction,
and null-time behavior. All baseline and discrimination tests total 14 tests.

## Matched-capability mechanism pilot

The new `mechanism_core.py` prototype provides six parameterized contracts with
explicit projection, multiplicity, time-window and aggregation-grain arguments.
It is independent of the historical rule parser. `build_mechanism_probes.py`
creates the two-project, 144-instance DEVELOPMENT set; four equivalent wordings
and two discriminating contrasts per base instance. It contains 24 instantiated
base groups and six task families, not 144 independent or real-user tasks.

`verify_mechanism_references.py` crosschecks supplied correct calls against the
independent CSV oracle. That is service conformance, never an NL score.
`run_mechanism_pilot.py` currently evaluates only 12 canonical requests per project
with matched business information: A generates Cypher, C emits a typed service
call. No obligation checker or repair is present. First failure categories,
actual generated queries/calls, result tables, tokens and protocol failures are
retained. `--reuse-predictions` replays ALL identical terminal predictions from a
completed run with no new model dispatch; it does not select better answers.

The pilot accepts a complete single JSON object or singleton array without
changing any field/value. Missing required fields remain protocol failures.
`diagnose_mechanism_payload.py` separately inspects as-written operation/argument
payloads and never promotes missing-envelope responses into end-to-end successes.
Current tests across baseline, discriminator and mechanism modules total 19.
See `MECHANISM_STUDY.md` for actual results, limitations and journal rationale.

```powershell
python build_mechanism_probes.py --redis-data REDIS_EXECUTION_CSV --duckdb-data DUCKDB_EXECUTION_CSV --output NEW_EXTERNAL_DATASET
python verify_mechanism_references.py --specifications PRIVATE_PROJECT_SPEC --receipt PRIVATE_GRAPH_RECEIPT --output NEW_REFERENCE_RECORD --port 17475
python run_mechanism_pilot.py --source PRIVATE_DATASET --project redis --receipt PRIVATE_GRAPH_RECEIPT --config PRIVATE_CONFIG --output NEW_PRIVATE_RUN --port 17475
```

Bring up only the owned dedicated graph before verification/execution. DuckDB
uses its own receipt and port 17476. Stop and verify owned graph processes after
the run. Private API configuration and raw graph artifacts stay outside Git.
