# D1.2c-v1 bounded parser development specification v1

## Scope and non-goals

Preserve the architecture:

`bounded NL -> ControlledQueryIR -> deterministic alignment/derivation -> contract selection -> Cypher -> static validation -> bounded gold-blind repair`

This specification does not authorize implementation, a generation rerun, a v2 dataset, query-ID routing, sentence-specific patches, or an LLM. Every proposed change must be reusable across a bounded phrase family and must fail closed if a parsed constraint cannot be consumed.

## Cross-cutting acceptance invariant

Every explicit NL constraint that enters IR must end in exactly one of three states: `CONSUMED_BY_SELECTED_CONTRACT`, `PROVABLY_ENTAILED_BY_CONTRACT`, or `ABSTAIN_WITH_TYPED_UNCONSUMED_REASON`. Selection must never silently drop scope, relation, role, time, projection, aggregation, sort, branch optionality, or limit constraints.

## Priority 1 — typed prefix-scope constraints

- General/reusable rationale: canonical instances and entity-id prefixes are different constraint types. Prefixes such as `PR_156018` and `I_156018` must be represented without fabricating a concrete entity.
- Addresses: RC1, with downstream RC11 prevention.
- v1 families: q_l1_02, q_l3_01, q_l4_02, q_l4_01, q_comp_01.
- Required IR shape: `EntityScope(label, property=entity_id, operator=STARTS_WITH, value, provenance)`, coexisting with canonical anchors.
- No-drop invariant: a selected contract must expose a compatible prefix slot and consume the exact label/property/operator/value tuple.
- Negative tests: reject bare repository-like fragments without an entity kind; reject mismatched prefixes (Issue prefix in PullRequest slot); preserve `#` canonical IDs as equality, not prefix; do not infer a repository instance from the numeric prefix alone.
- Overfitting risk: recognizing only `PR_156018`/`I_156018` or routing by heldout/query ID rather than typed identifier grammar.

## Priority 2 — role-aware target and projection parsing

- General/reusable rationale: the anchored/source entity is often not the requested output. Output roles must be derived from request predicates and row-schema language, not fallback source nouns.
- Addresses: RC3, RC7.
- v1 families: q_l1_01, q_l1_03, q_l2_01, q_l2_02, q_l2_03, q_l3_01, q_l3_02, q_l4_01, q_l4_02, q_l4_03, q_comp_01.
- Required IR shape: ordered `ProjectionItem(role, label, property, distinct, nullable, alias)` entries separate from source anchors and relation endpoints.
- No-drop invariant: every requested column is consumed in order; fallback-to-source is forbidden when an explicit object/actor/resource/ID role exists.
- Negative tests: anchor and target share a noun; two-column requests with one missing column; actor IDs vs PR IDs; domain property vs ExternalResource ID; nullable optional-branch output; extra unrequested projections.
- Overfitting risk: phrase maps tied to one sentence instead of bounded grammatical roles such as “who”, “which X”, “X ID”, and “column/list of”.

## Priority 3 — relation phrase-family normalization

- General/reusable rationale: existing service semantics need bounded paraphrase families covering active/passive voice, nominalizations, author/opened-by language, and pronominal arguments.
- Addresses: RC2 and supports RC10.
- v1 families: q_l2_01, q_l2_02, q_l2_03, q_l3_02, q_l3_03, q_l4_01, q_l4_02, q_l4_03, q_comp_01.
- Required normalization targets: COMMENTED_ON_ISSUE, OPENED_BY, LINKS_TO, MENTIONS, REFERENCES, COMMENTED_ON_REVIEW, CREATED_IN, including explicit direction and endpoint roles.
- No-drop invariant: normalized relations carry source role, target role, direction, confidence/provenance, and cannot be merged merely because service names co-occur.
- Negative tests: distinguish comment creation from comment-on-issue; link noun vs unrelated hyperlink wording; mention vs reference; review comment vs issue comment; passive direction reversals; ambiguous “created in” without a review/PR role.
- Overfitting risk: one regex per heldout sentence or accepting relation words without compatible typed endpoints.

## Priority 4 — explicit temporal operator, boundary, and owner

- General/reusable rationale: lower-only, upper-only, calendar-year, and explicit half-open intervals have different semantics; each bound belongs to a particular relation/event.
- Addresses: RC4, RC5.
- v1 families: q_l2_01, q_l3_01, q_l3_03, q_l4_01, q_l4_03, q_comp_01.
- Required IR shape: one or more `TimePredicate(owner_branch, property, operator, instant, timezone, provenance)`; never encode “bounded” as an untyped boolean.
- No-drop invariant: no inferred upper bound for lower-only language; two stated bounds must both survive; owner must match a selected branch and property.
- Negative tests: “on or after” without upper bound; “during 2023” half-open year; June 1 lower-only; strict before vs inclusive through; two branches with date applying to only one; timezone absent/explicit UTC.
- Overfitting risk: converting every 2023 mention to a calendar year or attaching all dates to the first relation.

## Priority 5 — robust limit-expression normalization

- General/reusable rationale: `up to`, `at most`, `no more than`, `capped at`, `limited to`, `first/top N`, `keep N`, and `stop at N` are one bounded cardinality family.
- Addresses: RC6.
- v1 families: all 13 executable families expose at least one missed variant.
- Required IR shape: `Limit(value, inclusive=true, source_span, provenance)`; template defaults remain explicit contract defaults, not substitutes for missed NL.
- No-drop invariant: an explicit NL limit must equal the consumed rendered limit; conflicts with a contract default cause abstention or explicit override under contract rules.
- Negative tests: unrelated numbers in IDs/dates; “first” as ordering without N; multiple conflicting limits; top-N requiring order; negative/zero/excess values; numeric words if in bounded grammar.
- Overfitting risk: enumerating only the v1 surface strings or treating every nearby integer as a limit.

## Priority 6 — typed projection, grouping, distinct, aggregation, and sort

- General/reusable rationale: flat `projection={property}`, `aggregation=count(*)`, and one sort cue cannot represent multi-column, grouped, distinct, collection, max-time, count-distinct, and multi-key requirements.
- Addresses: RC7, RC8, RC9, RC11.
- v1 families: q_l2_01, q_l2_02, q_l3_01, q_l3_02, q_l3_03, q_l4_01, q_l4_02, q_l4_03, q_comp_01.
- Required IR shape: ordered projection items plus `GroupKey`, `Aggregate(function, operand, distinct)`, `DistinctScope`, and ordered `SortKey(expression_ref, direction, priority)`.
- No-drop invariant: selected contract consumes every requested output/aggregate/sort key, including distinct flags and ordering priority; extra outputs require contract justification.
- Negative tests: count(*) vs count(DISTINCT pr); row DISTINCT vs collected DISTINCT values; group-domain plus ID projection; sort by aggregate vs event time; two-key tie break; latest=max(time); missing nullable collection.
- Overfitting risk: recognizing “count” without its operand/distinct/group scope or hard-wiring q_comp column aliases.

## Priority 7 — explicit required/optional multi-branch composition

- General/reusable rationale: shared-source, multi-hop, independently optional, and ANY_OF relation structures are graph constraints, not bags of relation names.
- Addresses: RC10 and supports RC2/RC5/RC11.
- v1 families: q_l2_01, q_l3_02, q_l4_01, q_l4_02, q_l4_03, q_comp_01.
- Required IR shape: branch/path nodes with stable identities, shared variable roles, required/optional flags, relation alternatives (ALL_OF/ANY_OF), and branch-local filters/time/projections.
- No-drop invariant: optionality and shared-source identity must match the selected contract exactly; relation-set equality alone is insufficient.
- Negative tests: two unrelated sources vs same source; one optional vs both optional; optional branch filter accidentally becoming global; ANY_OF vs ALL_OF MENTIONS/REFERENCES; required actor branch missing; time attached to wrong branch.
- Overfitting risk: special-case “same source” for q_l3_02 or template-name routing instead of typed composition.

## Priority 8 — IR-to-template contract coverage audit

- General/reusable rationale: richer IR is useful only if every typed constraint has an auditable consumer in selection and rendering.
- Addresses: RC11 and all upstream labels as a final fail-closed boundary.
- v1 families: all 13 executable families.
- Required behavior: candidate-level coverage records for scope, entity, branch identity, relation alternatives, time owner/operator, projections, aggregates, sort, and limit; one selected candidate only when coverage is total and non-conflicting.
- No-drop invariant: total coverage is mechanically checked before render and rechecked against rendered semantic signature.
- Negative tests: candidate consumes relation but not target; consumes lower bound but not upper; consumes one projection column; drops secondary sort; collapses optional branch; default limit conflicts with explicit limit; two candidates tie with different semantics.
- Overfitting risk: marking fields consumed based on template ID or intent key without slot-/branch-level evidence.

## Recommended implementation order and gates

1. Typed prefix scope plus alignment/coverage tests.
2. Role-aware targets and ordered projections.
3. Relation phrase-family normalization with typed endpoints.
4. Temporal predicates with operators and owner branches.
5. Limit normalization.
6. Typed projection/aggregate/distinct/sort algebra.
7. Required/optional/ANY_OF branch composition.
8. Full IR-to-template consumption audit and adversarial no-drop tests.

Each step should be merged only with reusable positive and negative tests. After implementation informed by v1, v1 becomes development regression data only. A separately authored and contamination-audited heldout v2 is required for post-tuning evaluation.

`IMPLEMENTATION_IN_THIS_TASK = NO`  
`HELDOUT_V1_ROLE_AFTER_THIS_TASK = DEVELOPMENT_DIAGNOSTIC`  
`NEW_INDEPENDENT_HELDOUT_V2_REQUIRED_AFTER_TUNING = YES`
