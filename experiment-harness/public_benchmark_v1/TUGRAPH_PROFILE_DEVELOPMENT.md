# Native TuGraph compiler profile: development only

This branch starts at frozen CypherBench commit c614367. It leaves that run's
source, predictions, scores and main table unchanged. No Text2GQL model
prediction is admitted by these changes.

`compile_selection(..., backend='tugraph-4.5.2')` uses actual schema properties,
plain verified identifiers, explicit defined-operand guards for the IR's finite
AND predicates, and one-MATCH trail lowering. Counting native nodes uses their
physical IDs. `tugraph_dialect.compile_rest_selection` only renders selections
compiled by this module, and never accepts arbitrary raw query text.

The original default is `backend='neo4j'`. Existing callers and query output
remain on that path unless explicitly changed. The version is now
`typed-services-dev-0.3-native-profile`; it is not the original frozen version.

Independent four-node/four-edge fixtures, with no benchmark tasks or model
calls, measured 17/18 checks in v1, 17/18 in v2 and 18/18 in v3. Intermediate
failures are retained. Native comma-separated parallel patterns returned one
binding instead of two; a connected trail retained both. Native node-object
COUNT DISTINCT returned four vertices rather than the three incident vertices;
physical-ID and declared-primary-key counting returned three. These observations
are limited to the pinned native engine and fixtures, not universal engine claims.

Not ready: schema/identity adapter, entity-set composition, OPTIONAL/multi-stage
lowering, DATETIME support, equal Direct inputs, typed REST/Bolt answers,
training-only model development, final metrics and formal freeze. Unsupported
syntax is explicit, not replaced by an easier question. Do not start a test
prediction from this profile alone or reuse the CypherBench-only model budget.

Verification: `python -m unittest test_tugraph_dialect test_schema_services`.
23 checks passed. Full measured evidence is indexed in the stage report, kept
outside publication Git. No credentials, raw author data or runtime packages
are required by these source files.
