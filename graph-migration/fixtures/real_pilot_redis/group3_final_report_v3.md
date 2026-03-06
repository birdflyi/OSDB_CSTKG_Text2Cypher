# Group-3 Final Report (v3)

All paths are relative to the repository root OSDB_CSTKG_Text2Cypher/.

## Version Pins
- `data_real/pilot_queries/minimal_template_pack_group3_v3.yaml`
- `graph-migration/fixtures/real_pilot_redis/group3_template_coverage_report_v3.md`
- `graph-migration/fixtures/real_pilot_redis/group3_run_summary.md`
- `graph-migration/fixtures/real_pilot_redis/group3_run_traces.jsonl`
- `data_real/pilot_queries/schema_metadata.yaml`
- (optional reference) `graph-migration/fixtures/real_pilot_redis/group3_readme.md`

## A) Setup
- Query set size: 15 total
  - Executable queries: 13
  - Injection-pending queries: 2 (`COUPLES_WITH`, `RESOLVES`)
- Two-track execution boundary:
  - Native edge types in executable Cypher are only `:EVENT_ACTION` and `:REFERENCE`
  - Service semantics are expressed via edge property filters on `rel.service_rel_type`
- Repo scope policy:
  - Scope is enforced via base prefix `ABBR_<repo_id>` and `STARTS WITH`
  - Prefixes are derived by `repo_scope_prefixes` (no separator-dependent assumptions)

## B) Template Inventory (v3)
- Executable template count: **8** (merged from prior 13-template inventory)
- Current key templates (latest corrected v3):
  - `tplv3_01`: `OneHopEA` (single-intent Issue OPENED_BY)
  - `tplv3_03`: `EntityFilter` (repo-scope PullRequest listing via `pr_base_prefix`)
- Main merges retained:
  - OneHopRef merged by enum slot `ref_semantic`
  - Shared composite/aggregation skeletons reused across multiple query IDs
- Coverage remains complete for executable set:
  - `group3_template_coverage_report_v3.md` shows 13/13 executable queries mapped
  - Controlled mode after merging remains **13/13 static-valid**

## C) Slot Model + Trace Evidence
Slot source chain:
1. mention extractor (`entity_slots`/relation cues)
2. entity aligner (canonical `entity_id`)
3. derived slot builder (`repo_scope_prefixes`)
4. deterministic time derivation (`derived_year_range`) when needed

Representative slot trace excerpt (controlled, `q_l4_01`):
- `repo_entity_id`: `R_156018` (from `entity_slots`)
- `repo_scope_prefixes`: `{"repo_id":156018, "base_prefixes":{"PullRequest":"PR_156018", ...}}`
- `derived_year_range`:
  - `extracted_text`: `2023`
  - `time_start`: `2023-01-01T00:00:00Z`
  - `time_end`: `2024-01-01T00:00:00Z`

## D) Controlled Constraint Set
Controlled acceptance enforces all of the following before accepting rendered Cypher:
- Native-only relationship types (`EVENT_ACTION`, `REFERENCE`)
- Required `rel.service_rel_type` filter for templates with service constraints
- Hop-limit policy (including bounded 3-hop where explicitly configured)
- Placeholder separation (`COUPLES_WITH`/`RESOLVES` excluded from executable templates)
- Intermediate-node restrictions (`UnknownObject`/`ExternalResource` as configured)
- Property whitelist with relation scope:
  - `url_domain_etld1` is allowed under `properties_by_relation.REFERENCE` and accessed as `rl.url_domain_etld1` in executable Cypher
  - It is not promoted to global unrestricted property scope

## E) Results
### Run-level table (denominator = executable queries = 13)
| Generator | Success | Fail | Skip |
|---|---:|---:|---:|
| template_first | 12 | 1 | 2 |
| controlled | 13 | 0 | 2 |

- Injection-pending skips:
  - Query-level skipped count: 2
  - Trace-row skipped count (2 generators x 2 queries): 4
- Controlled success rate on executable queries: **1.0000**

## F) Representative Examples (3)
### 1) OneHopEA OPENED_BY on Issue (`q_l1_01`)
```cypher
MATCH (i:Issue {entity_id: 'I_156018#12095'})-[rel:EVENT_ACTION]->(a:Actor)
WHERE rel.service_rel_type = 'OPENED_BY'
RETURN a.entity_id LIMIT 25
```
- Template: `tplv3_01`
- Notes: native EA edge + service verb filter
- Validator: `valid=true`

### 2) Repo-scope PullRequest listing (`q_l1_02`)
```cypher
MATCH (pr:PullRequest)
WHERE pr.entity_id STARTS WITH 'PR_156018'
RETURN pr.entity_id LIMIT 25
```
- Template: `tplv3_03`
- Notes: repo scope is enforced by derived base-prefix (`repo_scope_prefixes -> pr_base_prefix`)
- Validator: `valid=true`

### 3) Bounded 3-hop composite (`q_l4_03`)
```cypher
MATCH (prrc:PullRequestReviewComment)-[r1:EVENT_ACTION]->(prr:PullRequestReview)
MATCH (prr)-[r2:EVENT_ACTION]->(pr:PullRequest {entity_id: 'PR_156018#11659'})
MATCH (prrc)-[r3:REFERENCE]->(a:Actor)
WHERE r1.service_rel_type = 'COMMENTED_ON_REVIEW'
  AND r2.service_rel_type = 'CREATED_IN'
  AND r3.service_rel_type IN ['MENTIONS','REFERENCES']
  AND r3.source_event_time >= '2023-01-01T00:00:00Z'
  AND r3.source_event_time < '2024-01-01T00:00:00Z'
RETURN DISTINCT a.entity_id LIMIT 50
```
- Template: `tplv3_07`
- Notes: explicit bounded 3-hop policy, still native-only + service-filtered
- Validator: `valid=true`

## G) Boundary and Future Injection
- `COUPLES_WITH` and `RESOLVES` remain non-materialized in this stage by design.
- They are represented as `injection_pending_templates` and matched by:
  - `gold_cypher = null`
  - `gold_cypher_injected` provided for future chapter-level injection
  - `expected_to_fail_until_injected = true`
- This preserves strict boundary separation between:
  - current executable native/service projection track (Group-3), and
  - future structural/task-semantic injections (Chapter 5/6).


