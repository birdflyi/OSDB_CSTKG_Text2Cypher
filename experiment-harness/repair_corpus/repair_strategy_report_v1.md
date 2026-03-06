# Repair Strategy Report v1

This document analyzes the failure types in `repair_failure_corpus_v1.jsonl` and proposes actionable repair steps for each type.

Scope:
- Group-3 remains frozen.
- This report documents repair strategy only.
- No generation, schema, or template change is implemented here.

Reference artifacts:
- `experiment-harness/repair_corpus/repair_failure_corpus_v1.jsonl`
- `experiment-harness/repair_corpus/repair_failure_summary.md`
- `graph-migration/validators/pilot_cypher_validator.py`
- `graph-migration/runners/group3_template_runner.py`
- `graph-migration/normalizers/derived_slot_builder.py`
- `experiment-harness/repair/diagnosis.py`
- `experiment-harness/repair/lightweight_repair.py`
- `experiment-harness/repair/simple_repair.py`

## Corpus Snapshot

| Failure Type | Count |
|---|---:|
| WRONG_RELATION_TYPE | 5 |
| MISSING_PATTERN | 5 |
| TIME_RANGE_ERROR | 3 |
| AGGREGATION_ERROR | 3 |
| WRONG_DIRECTION | 2 |
| MISSING_PROPERTY_FILTER | 2 |
| ILLEGAL_PROPERTY | 2 |
| WRONG_ENTITY_SCOPE | 2 |

## Repair Pipeline Positioning

Recommended repair flow:
1. Parse validator output into `StructuredDiagnosis` items.
2. Map each failure type to a minimal edit family.
3. Apply repair candidates in increasing cost order.
4. Revalidate using static schema checks before accepting the repaired query.
5. Record applied edits, repair cost, and final validity in repair trace.

Existing reusable components:
- Static validation: `graph-migration/validators/pilot_cypher_validator.py::validate_cypher_static`
- Diagnosis carrier: `experiment-harness/repair/diagnosis.py::StructuredDiagnosis`
- Existing repair operators:
  - `LightweightRepairModule._replace_relation_type`
  - `LightweightRepairModule._flip_relation_direction`
  - `LightweightRepairModule._replace_property_nearest`
  - `LightweightRepairModule._shorten_path_length`
  - `LightweightRepairModule._simplify_aggregation_sort`
  - `LightweightRepairModule._fallback_simpler_template`
- Scope derivation: `graph-migration/normalizers/derived_slot_builder.py::build_repo_scope_prefixes`
- Group-3 slot extraction reference: `graph-migration/runners/group3_template_runner.py::_extract_slot_values`

## 1. WRONG_RELATION_TYPE

### Description and cause
This failure occurs when the generated Cypher uses a relation type outside the allowed native boundary or replaces the intended native edge with an incompatible type.
Typical examples:
- `:RELATED_TO` instead of `:EVENT_ACTION` or `:REFERENCE`
- `:EVENT_ACTION` used where the gold query requires `:REFERENCE`
- Service semantics incorrectly promoted to native relation labels

Background:
- Group-3 executable Cypher is Two-track constrained.
- Native relation types must remain only `EVENT_ACTION` and `REFERENCE`.
- Service semantics must stay in `rel.service_rel_type` filters.

### Repair steps
1. Read validator errors and extract disallowed relationship types.
2. Determine the intended native relation from:
   - query gold if available in supervised repair setting,
   - expected constraints,
   - service filter and template family.
3. Replace only the first offending native relation token.
4. Preserve any valid `service_rel_type` predicates.
5. Revalidate direction constraints after replacement.

Minimal edit priority:
1. replace relation type
2. if still invalid, flip direction
3. if relation still unsupported, fallback to simpler template

### Functions or tools
- `LightweightRepairModule._replace_relation_type`
- `SimpleRuleRepair.repair` for the simplest one-step replacement
- `validate_cypher_static`

### Suggested validation command or test
- Static validator check after replacement.
- Existing run-level smoke command:
```bash
python graph-migration/cli.py run-group3-templates --queries "data_real/pilot_queries/queries_pilot.jsonl" --templates "data_real/pilot_queries/minimal_template_pack_group3_v3.yaml" --schema "data_real/pilot_queries/schema_metadata.yaml" --token-conf "data_scripts/etc/authConf.py" --api-timeout-sec 30 --outdir "graph-migration/fixtures/real_pilot_redis/"
```
- Corpus cases to validate against:
  - `q_l1_01__free_form`
  - `q_l1_03__perturb_wrong_relation`
  - `q_l4_02__perturb_wrong_relation`

## 2. MISSING_PATTERN

### Description and cause
This failure means the generated query omits one or more required MATCH patterns, usually collapsing a multi-hop query into a single trivial traversal.
Typical symptoms:
- missing join to Actor/Repo/ExternalResource
- missing OPTIONAL MATCH branch
- missing bridge node such as `IssueComment` or `PullRequestReview`

Background:
- Multi-hop CSTKG queries often encode task semantics across several constrained subpatterns.
- Missing a pattern usually destroys query meaning even if syntax remains valid.

### Repair steps
1. Compare generated query shape with the expected query family.
2. Count missing MATCH clauses and missing relation branches.
3. Restore the smallest missing branch first:
   - main branch before optional branch,
   - entity anchor before aggregation branch.
4. Preserve original valid clauses where possible.
5. Revalidate hop count and direction constraints.

Minimal edit priority:
1. restore missing MATCH pattern
2. restore missing OPTIONAL MATCH if required by gold
3. fallback to simpler template only if pattern reconstruction fails

### Functions or tools
- `StructuredDiagnosis` with `error_type='path'`
- `LightweightRepairModule._shorten_path_length` is the reverse operation; use it as shape reference, but missing-pattern repair will likely need a new inverse operator later.
- `group3_template_runner.py` template skeletons as canonical shape source

### Suggested validation command or test
- Compare repaired query against the target template skeleton for the same query id.
- Corpus cases:
  - `q_l3_02__free_form`
  - `q_l4_01__free_form`
  - `q_l4_02__free_form`
  - `q_l4_03__free_form`

## 3. WRONG_DIRECTION

### Description and cause
The relation type may be correct, but the source and target direction violate schema direction constraints.
Typical symptoms:
- reference edge reversed
- action edge reversed between source and target labels

Background:
- `validate_cypher_static` checks configured direction constraints.
- Direction errors are frequent in graph generation because many natural-language phrasings are symmetric while the schema is not.

### Repair steps
1. Read `DIRECTION_MISMATCH` from validator output.
2. Extract the offending edge pattern.
3. Swap source and target while preserving relation type.
4. Keep entity ids attached to the semantically correct endpoint.
5. Revalidate relation type and labels after the flip.

Minimal edit priority:
1. flip relation direction
2. if direction still invalid, replace relation type
3. if both fail, fallback to template skeleton

### Functions or tools
- `LightweightRepairModule._flip_relation_direction`
- `validate_cypher_static`
- `StructuredDiagnosis`

### Suggested validation command or test
- Unit-style validation of repaired edge against `direction_constraints` in schema.
- Corpus cases:
  - `q_l2_03__free_form`
  - `q_l1_03__perturb_wrong_direction`

## 4. MISSING_PROPERTY_FILTER

### Description and cause
The query keeps the core graph pattern but drops a required filter, most often:
- `rel.service_rel_type = ...`
- `source_event_time` lower or upper bound

Background:
- Two-track semantics require explicit service filtering.
- Time-sensitive queries require canonical time filtering on `source_event_time`.
- Without these predicates, the query is structurally close but semantically too broad.

### Repair steps
1. Detect whether the missing predicate is:
   - service semantics filter,
   - time boundary filter,
   - both.
2. Recover the expected predicate from:
   - template constraints,
   - slot trace service hints,
   - time range slots,
   - query gold in supervised mode.
3. Insert the filter into the existing `WHERE` clause.
4. Keep predicate order stable: service filter before time filters.
5. Revalidate allowed properties after insertion.

Minimal edit priority:
1. restore service filter
2. restore time predicate
3. normalize the filter field to `source_event_time`

### Functions or tools
- `group3_template_runner.py::_extract_slot_values`
- `group3_template_runner.py::_derive_time_range_from_nl`
- `group3_template_runner.py::_controlled_checks` for service-filter expectations
- Future dedicated repair helper should be added next to `LightweightRepairModule._replace_property_nearest`

### Suggested validation command or test
- Re-run static validation and ensure `MISSING_SERVICE_FILTER` disappears.
- Corpus cases:
  - `q_l3_01__perturb_missing_service_filter`
  - `q_l4_01__perturb_missing_service_filter`

## 5. ILLEGAL_PROPERTY

### Description and cause
This failure occurs when a property is read from the wrong scope.
In the current CSTKG pilot, the canonical example is:
- wrong: `e.url_domain_etld1`
- correct: `rl.url_domain_etld1`

Background:
- `url_domain_etld1` is a `REFERENCE` relation-scope property.
- It is allowed through `properties_by_relation.REFERENCE`, not as a general node property.

### Repair steps
1. Identify the offending property name and alias.
2. Determine the legal scope for that property from schema relation scope.
3. Move the property access from node alias to relation alias.
4. If no legal alias exists in the query, add or rename the relation variable.
5. Revalidate property scope and relation type.

Minimal edit priority:
1. move property to relation alias
2. rename relation variable consistently
3. if still invalid, replace property with nearest allowed property only as last resort

### Functions or tools
- `validate_cypher_static`
- `LightweightRepairModule._replace_property_nearest` as a fallback, not as first choice
- schema source: `properties_by_relation` in `schema_metadata.yaml`

### Suggested validation command or test
- Static validator should clear `ILLEGAL_PROPERTY`.
- Corpus cases:
  - `q_l2_02__perturb_illegal_property`
  - `q_comp_01__perturb_illegal_property`

## 6. WRONG_ENTITY_SCOPE

### Description and cause
This failure arises when repo scope or entity prefix constraints are absent or wrong.
Typical examples:
- `STARTS WITH 'I_156018'` used for PullRequest scope
- repo scope omitted entirely
- wrong prefix family used for the scoped node label

Background:
- Group-3 does not use `BELONGS_TO`.
- Repo scoping is derived from canonical `repo_entity_id` through `build_repo_scope_prefixes`.

### Repair steps
1. Determine whether the query needs repo scope.
2. Extract or derive `repo_entity_id`.
3. Build `repo_scope_prefixes` from the repo id and target labels.
4. Insert or replace `STARTS WITH $*_base_prefix` using the correct label family.
5. Revalidate that the correct node alias carries the prefix filter.

Minimal edit priority:
1. derive repo scope prefixes
2. restore `STARTS WITH` constraint
3. replace wrong base prefix with label-aligned prefix

### Functions or tools
- `build_repo_scope_prefixes`
- `group3_template_runner.py::_extract_slot_values`
- template `repo_scope_policy` sections in the v3 template pack

### Suggested validation command or test
- Validate that repaired queries use `PR_<repo_id>`, `I_<repo_id>`, `C_<repo_id>` without separator assumptions.
- Corpus cases:
  - `q_l1_02__free_form`
  - `q_l1_02__perturb_wrong_scope`

## 7. TIME_RANGE_ERROR

### Description and cause
This failure covers missing, widened, or malformed time constraints.
Typical examples:
- time filter absent
- wrong field used instead of `source_event_time`
- missing end boundary
- year-only query not normalized into a half-open interval

Background:
- Canonical time filtering uses relation property `source_event_time`.
- Group-3 already demonstrates deterministic year-range derivation.

### Repair steps
1. Detect missing or malformed time predicates.
2. Extract time slots from query trace or derive them from the NL query.
3. Normalize all time windows to the canonical half-open form:
   - `>= time_start`
   - `< time_end`
4. Ensure the time filter is attached to the correct relation alias.
5. Revalidate property scope and query syntax.

Minimal edit priority:
1. restore canonical time field
2. restore missing boundary
3. normalize to half-open interval

### Functions or tools
- `group3_template_runner.py::_derive_time_range_from_nl`
- `group3_template_runner.py::_extract_slot_values`
- `validate_cypher_static`

### Suggested validation command or test
- Check that repaired query uses `source_event_time` and a complete range.
- Corpus cases:
  - `q_l2_01__free_form`
  - `q_l3_01__free_form`
  - `q_l2_01__perturb_missing_time_filter`

## 8. AGGREGATION_ERROR

### Description and cause
This failure occurs when aggregation intent is partially lost or structurally simplified into a non-aggregate query.
Typical symptoms:
- missing `COUNT`, `MAX`, or grouped return field
- incorrect `ORDER BY` target
- aggregation requested but plain entity return produced

Background:
- Aggregation queries are semantically fragile because errors can remain syntactically valid.
- They often co-occur with property-scope errors on evidence fields.

### Repair steps
1. Detect expected aggregation from slot trace or query intent.
2. Compare generated `RETURN` clause with gold aggregate structure.
3. Restore aggregate function and grouped projection.
4. Restore sort keys that depend on aggregate aliases.
5. Revalidate illegal property usage after aggregate restoration.

Minimal edit priority:
1. restore aggregate projection
2. restore group key
3. restore aggregate sort alias
4. if unresolved, simplify aggregation only as fallback for partial recovery

### Functions or tools
- `LightweightRepairModule._simplify_aggregation_sort` is useful as a fallback baseline.
- A future inverse operator should restore expected aggregate clauses from slot trace.
- `validate_cypher_static`

### Suggested validation command or test
- Ensure repaired query returns aggregate aliases instead of plain node tuples.
- Corpus cases:
  - `q_l3_03__free_form`
  - `q_comp_01__free_form`
  - `q_l3_03__perturb_aggregation`

## Implementation Priority Recommendation

1. `WRONG_RELATION_TYPE`
2. `WRONG_DIRECTION`
3. `ILLEGAL_PROPERTY`
4. `WRONG_ENTITY_SCOPE`
5. `MISSING_PROPERTY_FILTER`
6. `TIME_RANGE_ERROR`
7. `AGGREGATION_ERROR`
8. `MISSING_PATTERN`

Rationale:
- The first four have the clearest local edit surface and strongest validator feedback.
- `MISSING_PROPERTY_FILTER` and `TIME_RANGE_ERROR` are also local but need slot-aware reinsertion.
- `AGGREGATION_ERROR` and especially `MISSING_PATTERN` require more template-aware reconstruction.

## Recommended Next Step

Use this document as the design basis for **Repair Example Set v2** or for implementing the first narrow repair slice:
- phase 1: relation, direction, property-scope, scope-prefix fixes
- phase 2: time and service-filter reinsertion
- phase 3: aggregation and missing-pattern reconstruction
