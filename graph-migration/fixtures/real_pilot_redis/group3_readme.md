# Group-3 README (Minimal Template Pack v3)

## A. Overview

- Template pack source: `data_real/pilot_queries/minimal_template_pack_group3_v3.yaml`
- Design: Two-track (`:EVENT_ACTION` / `:REFERENCE` as native edge types only)
- Service semantics: expressed only via `rel.service_rel_type` filter
- Coverage: 8 executable templates covering 13 executable queries + 2 injection-pending queries

### A1. Template families

- `EntityFilter`
- `OneHopEA`
- `OneHopRef`
- `TwoHopComposite`
- `Aggregation`
- `InjectionPending` (appendix only)

### A2. v3 key template anchors

- `tplv3_01`: `OneHopEA` (Issue OPENED_BY single-intent skeleton)
- `tplv3_03`: `EntityFilter` (repo-scope PullRequest listing via `pr_base_prefix`)

## B. Slot model

### B1. Core entity slots

- `repo_entity_id`, `actor_entity_id`, `issue_entity_id`, `pr_entity_id`, `commit_entity_id`

### B2. Derived repo scope slots

- `repo_scope_prefixes` (`DERIVED_PREFIX_MAP`) from `build_repo_scope_prefixes(repo_entity_id, labels)`
- `pr_base_prefix`, `issue_base_prefix`, `commit_base_prefix`

### B3. Enum slots (v3 merged templates)

- `ea_action_verb`: merged OneHopEA action verb slot (for example `OPENED_BY`, `PULLED_REPO_TO`)
- `ref_semantic`: merged OneHopRef semantic slot (for example `MENTIONS`, `REFERENCES`, `LINKS_TO`)

Repo scoping policy:

- Use base prefix with no separator: `ABBR_<repo_id>`
- Apply `entity_id STARTS WITH $*_base_prefix`
- Do not use separator-specific forms such as `PR_156018#` / `C_156018@`

## C. Property whitelist

- Node properties: `entity_id`
- Edge properties (common): `service_rel_type`, `event_time`, `source_event_time`
- Reference evidence (optional): `tar_entity_match_text`, `tar_entity_match_pattern_type`, `url_raw`, `url_domain_etld1`

## D. Path boundaries

- Native rel types only: `EVENT_ACTION`, `REFERENCE`
- No `BELONGS_TO` in executable templates
- No `REFERS_TO` in executable templates
- Placeholders `COUPLES_WITH`, `RESOLVES` are injection-pending only

## E. Why BELONGS_TO is excluded

In pilot, "belongs to repo" is treated as a scope constraint instead of a materialized relation type. The execution route is:

1. align input repo mention to canonical `R_<repo_id>`;
2. derive base prefixes from ABBR map (for example `PR_<repo_id>`, `I_<repo_id>`, `C_<repo_id>`);
3. apply `entity_id STARTS WITH <base_prefix>` in templates.

This keeps native relation boundaries stable and avoids schema pollution by template-layer implementation details.

## F. Template excerpts

### F1. OneHopEA excerpt

```cypher
MATCH (i:Issue {entity_id: $issue_entity_id})-[rel:EVENT_ACTION]->(a:Actor)
WHERE rel.service_rel_type = 'OPENED_BY'
RETURN a.entity_id
LIMIT 25
```

### F2. Repo-scoped Aggregation excerpt

```cypher
MATCH (pr:PullRequest)
WHERE pr.entity_id STARTS WITH $pr_base_prefix
MATCH (pr)-[r:REFERENCE]->(e:ExternalResource)
WHERE r.service_rel_type = 'LINKS_TO'
RETURN r.url_domain_etld1 AS domain, COUNT(*) AS cnt
ORDER BY cnt DESC
LIMIT 20
```

## G. Derived-slot note

`repo_scope_prefixes` is a derived slot for template scoping/tracing only; it is not a schema property or relationship type.
