# Group-3 Thesis Insert (v3)

All paths are relative to the repository root OSDB_CSTKG_Text2Cypher/.

## 7.3 Method
### 7.3.1 Two-track constrained generation setup
Group-3 adopts a strict Two-track design. Native executable relations are limited to `:EVENT_ACTION` and `:REFERENCE`, while service semantics are expressed via `rel.service_rel_type` filters. Repo scope is implemented with derived base-prefix constraints (`ABBR_<repo_id>`) and `STARTS WITH`, without introducing `BELONGS_TO` as an executable relation.

### 7.3.2 Minimal template inventory after merging
Using `minimal_template_pack_group3_v3.yaml`, the executable inventory is reduced to 8 templates while keeping full coverage over 13 executable pilot queries. The merge introduces enum slots:
- `ea_action_verb` for OneHopEA variants
- `ref_semantic` for OneHopRef variants
Current corrected anchors:
- `tplv3_01` is a clean `OneHopEA` template for Issue OPENED_BY.
- `tplv3_03` is a clean repo-scope `EntityFilter` template using `pr_base_prefix`.
No semantics are relaxed during merging.

### 7.3.3 Slot filling and traceability
Slot filling pipeline: mention extraction -> entity alignment -> derived prefix construction (`repo_scope_prefixes`) -> deterministic year-range derivation when required.
A representative controlled trace includes:
- `repo_entity_id=R_156018`
- derived `repo_scope_prefixes.base_prefixes.PullRequest=PR_156018`
- `derived_year_range: 2023 -> [2023-01-01T00:00:00Z, 2024-01-01T00:00:00Z)`

### 7.3.4 Controlled constraints
Controlled acceptance enforces:
- native-only relation types
- required service filters
- hop-limit policy (including bounded 3-hop where explicitly configured)
- placeholder exclusion (`COUPLES_WITH`, `RESOLVES`)
- relation-scoped property policy (`url_domain_etld1` allowed under `REFERENCE` only and accessed as `rl.url_domain_etld1`)

## 7.4 Experiments
### 7.4.1 Dataset and protocol
Pilot query set contains 15 items: 13 executable and 2 injection-pending placeholders. Evaluation compares template-first and controlled runs over the same v3 template inventory and schema constraints.

### 7.4.2 Main results
| Generator | Success | Fail | Skip |
|---|---:|---:|---:|
| template_first | 12 | 1 | 2 |
| controlled | 13 | 0 | 2 |

- Controlled success rate on executable queries: **1.0000**
- Injection-pending skips: 2 query-level / 4 trace-row-level

## 7.5 Case Studies
### Case 1: OneHopEA (Issue OPENED_BY)
```cypher
MATCH (i:Issue {entity_id: 'I_156018#12095'})-[rel:EVENT_ACTION]->(a:Actor)
WHERE rel.service_rel_type = 'OPENED_BY'
RETURN a.entity_id LIMIT 25
```

### Case 2: Repo-scope EntityFilter (PullRequest listing)
```cypher
MATCH (pr:PullRequest)
WHERE pr.entity_id STARTS WITH 'PR_156018'
RETURN pr.entity_id LIMIT 25
```

### Case 3: Bounded 3-hop composite
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

## 7.x Boundary and Future Injection
`COUPLES_WITH` (Chapter 5) and `RESOLVES` (Chapter 6) remain injection-pending in Group-3 and are not materialized in executable templates. They are represented with `gold_cypher=null`, `gold_cypher_injected`, and `expected_to_fail_until_injected=true`, preserving strict boundary separation between current constrained generation and later chapter-level semantic injections.

