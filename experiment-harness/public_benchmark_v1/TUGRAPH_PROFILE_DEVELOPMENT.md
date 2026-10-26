# Native TuGraph compiler profile: development only

Common-input preparation now has six graph-derived candidate catalogs covering
21,315 actual vertices, and identical Direct/Ours user-message bytes. Native
result validation accepted the shapes of 3,815 cached successful reference
responses and rejected 11 known gold errors as successes. These are offline
protocol checks, not new database/model runs or semantic answer validation.
The strict type-sensitive comparator is diagnostic only, not the author's EA.

Current stage: `typed-services-dev-0.7-native-optional-projection`. Six import-config
schemas (46 vertex labels, 64 directed signatures) preserve actual declared
primary properties and types. Eight fixed independent set plans, each run with
contract and full-detail strategies, gave 16/16 expected final answers from 32
native queries. String identities are measured on the native engine; integer
identity rejection/coercion checks are local only. No model predictions.

Native development added 20/20 fixed BOOL/naive-microsecond-DATETIME checks and
18/18 fixed OPTIONAL checks. The first OPTIONAL compilation had 12/18; all six
failed global filters are retained. Explicit WITH query parts and bag-preserving
node/scalar-property projection corrected them. No WITH * or pattern
comprehension is used. Nullable node counts use declared primary properties;
no timezones or finer-than-microsecond values are silently coerced.

This branch starts at frozen CypherBench commit c614367. It leaves that run's
source, predictions, scores and main table unchanged. No Text2GQL model
prediction is admitted by these changes.

`compile_selection(..., backend='tugraph-4.5.2')` uses actual schema properties,
plain verified identifiers, explicit defined-operand guards for the IR's finite
AND predicates, and one-MATCH trail lowering. Counting native nodes uses their
physical IDs. `tugraph_dialect.compile_rest_selection` only renders selections
compiled by this module, and never accepts arbitrary raw query text.

The original default is `backend='neo4j'`. Existing callers and query output
remain on that path unless explicitly changed. This branch is not the original
frozen version. The first compiler-only stage was v0.3-native-profile.

Independent four-node/four-edge fixtures, with no benchmark tasks or model
calls, measured 17/18 checks in v1, 17/18 in v2 and 18/18 in v3. Intermediate
failures are retained. Native comma-separated parallel patterns returned one
binding instead of two; a connected trail retained both. Native node-object
COUNT DISTINCT returned four vertices rather than the three incident vertices;
physical-ID and declared-primary-key counting returned three. These observations
are limited to the pinned native engine and fixtures, not universal engine claims.

Not ready: equal Direct inputs, typed REST/Bolt answers,
training-only model development, final metrics and formal freeze. Unsupported
syntax is explicit, not replaced by an easier question. Do not start a test
prediction from this profile alone or reuse the CypherBench-only model budget.

Verification: `python -m unittest test_tugraph_dialect test_schema_services test_tugraph_schema test_tugraph_temporal test_tugraph_optional test_tugraph_common_input test_tugraph_results`.
59 checks passed. Full measured evidence is indexed in the stage report, kept
outside publication Git. No credentials, raw author data or runtime packages
are required by these source files.
