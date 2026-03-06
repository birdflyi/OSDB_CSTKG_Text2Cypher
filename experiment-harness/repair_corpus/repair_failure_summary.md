# Repair Failure Summary v1

This corpus is constructed without modifying any frozen Group-3 artifact.
Source mix: 13 real `free_form` failures from `pilot_run_traces.jsonl` + 11 deterministic `controlled_perturbation` cases derived from frozen gold Cypher.
`group3_run_summary.md` confirms current frozen Group-3 controlled mode has no live failures, so synthetic perturbations are used to widen failure-type coverage for Group-4.

- total_failure_cases: 24
- executable_query_ids_covered: 13
- excluded_query_ids: q_ch5_01, q_ch6_01

## Source Distribution

| Failure Source | Count |
|---|---:|
| controlled_perturbation | 11 |
| free_form | 13 |

## Failure Type Distribution

| Failure Type | Count |
|---|---:|
| AGGREGATION_ERROR | 3 |
| ILLEGAL_PROPERTY | 2 |
| MISSING_PATTERN | 5 |
| MISSING_PROPERTY_FILTER | 2 |
| TIME_RANGE_ERROR | 3 |
| WRONG_DIRECTION | 2 |
| WRONG_ENTITY_SCOPE | 2 |
| WRONG_RELATION_TYPE | 5 |

## Examples by Failure Type

### MISSING_PATTERN
- case_id: `q_l3_02__free_form`
- query_id: `q_l3_02`
- failure_source: `free_form`
- validator_errors: `UNKNOWN_LABEL, UNKNOWN_REL, DIRECTION_MISMATCH`
- minimal_repair_hint: `add the missing MATCH pattern, restore the missing join structure`

Generated Cypher:
```cypher
MATCH (x:Entity)-[:RELATED_TO]->(y:Entity) RETURN x, y LIMIT 10
```
Gold Cypher:
```cypher
MATCH (s)-[rm:REFERENCE]->(a:Actor {entity_id: 'A_7045099'}) WHERE rm.service_rel_type = 'MENTIONS' OPTIONAL MATCH (s)-[rr:REFERENCE]->(repo:Repo) WHERE rr.service_rel_type = 'MENTIONS' OPTIONAL MATCH (s)-[rl:REFERENCE]->(e:ExternalResource) WHERE rl.service_rel_type = 'LINKS_TO' RETURN DISTINCT repo.entity_id, e.entity_id LIMIT 50
```

### WRONG_RELATION_TYPE
- case_id: `q_l1_01__free_form`
- query_id: `q_l1_01`
- failure_source: `free_form`
- validator_errors: `UNKNOWN_LABEL, UNKNOWN_REL, DIRECTION_MISMATCH`
- minimal_repair_hint: `replace relation type, align relation semantics with gold`

Generated Cypher:
```cypher
MATCH (x:Entity)-[:RELATED_TO]->(y:Entity) RETURN x, y LIMIT 10
```
Gold Cypher:
```cypher
MATCH (i:Issue {entity_id: 'I_156018#12095'})-[rel:EVENT_ACTION]->(a:Actor) WHERE rel.service_rel_type = 'OPENED_BY' RETURN a.entity_id LIMIT 25
```

### WRONG_DIRECTION
- case_id: `q_l2_03__free_form`
- query_id: `q_l2_03`
- failure_source: `free_form`
- validator_errors: `UNKNOWN_LABEL, UNKNOWN_REL, DIRECTION_MISMATCH`
- minimal_repair_hint: `reverse relationship direction, align source and target nodes`

Generated Cypher:
```cypher
MATCH (x:Entity)-[:RELATED_TO]->(y:Entity) RETURN x, y LIMIT 10
```
Gold Cypher:
```cypher
MATCH (ic:IssueComment {entity_id: 'IC_156018#12095#ic1'})-[rel:REFERENCE]->(a:Actor) WHERE rel.service_rel_type = 'MENTIONS' RETURN a.entity_id LIMIT 25
```

### MISSING_PROPERTY_FILTER
- case_id: `q_l3_01__perturb_missing_service_filter`
- query_id: `q_l3_01`
- failure_source: `controlled_perturbation`
- validator_errors: `MISSING_SERVICE_FILTER`
- minimal_repair_hint: `restore the missing property filter, reinsert the required service or time predicate`

Generated Cypher:
```cypher
MATCH (pr:PullRequest) WHERE pr.entity_id STARTS WITH 'PR_156018' MATCH (pr)-[rr:REFERENCE]->(x:UnknownObject) WHERE rr.source_event_time >= '2023-01-01T00:00:00Z' AND rr.source_event_time < '2024-01-01T00:00:00Z' RETURN pr.entity_id, x.entity_id ORDER BY rr.source_event_time DESC LIMIT 50
```
Gold Cypher:
```cypher
MATCH (pr:PullRequest) WHERE pr.entity_id STARTS WITH 'PR_156018' MATCH (pr)-[rr:REFERENCE]->(x:UnknownObject) WHERE rr.service_rel_type = 'REFERENCES' AND rr.source_event_time >= '2023-01-01T00:00:00Z' AND rr.source_event_time < '2024-01-01T00:00:00Z' RETURN pr.entity_id, x.entity_id ORDER BY rr.source_event_time DESC LIMIT 50
```

### ILLEGAL_PROPERTY
- case_id: `q_l2_02__perturb_illegal_property`
- query_id: `q_l2_02`
- failure_source: `controlled_perturbation`
- validator_errors: `ILLEGAL_PROPERTY`
- minimal_repair_hint: `move property access to the allowed relation scope, replace illegal property reference`

Generated Cypher:
```cypher
MATCH (pr:PullRequest {entity_id: 'PR_156018#11659'})-[rel:REFERENCE]->(e:ExternalResource) WHERE rel.service_rel_type = 'LINKS_TO' RETURN e.url_domain_etld1, e.entity_id LIMIT 10
```
Gold Cypher:
```cypher
MATCH (pr:PullRequest {entity_id: 'PR_156018#11659'})-[rel:REFERENCE]->(e:ExternalResource) WHERE rel.service_rel_type = 'LINKS_TO' RETURN rel.url_domain_etld1, e.entity_id LIMIT 10
```

### WRONG_ENTITY_SCOPE
- case_id: `q_l1_02__free_form`
- query_id: `q_l1_02`
- failure_source: `free_form`
- validator_errors: `UNKNOWN_LABEL, UNKNOWN_REL, DIRECTION_MISMATCH`
- minimal_repair_hint: `restore repo or entity scope constraint, fix entity prefix or STARTS WITH filter`

Generated Cypher:
```cypher
MATCH (x:Entity)-[:RELATED_TO]->(y:Entity) RETURN x, y LIMIT 10
```
Gold Cypher:
```cypher
MATCH (pr:PullRequest) WHERE pr.entity_id STARTS WITH 'PR_156018' RETURN pr.entity_id LIMIT 25
```

### TIME_RANGE_ERROR
- case_id: `q_l2_01__free_form`
- query_id: `q_l2_01`
- failure_source: `free_form`
- validator_errors: `UNKNOWN_LABEL, UNKNOWN_REL, DIRECTION_MISMATCH`
- minimal_repair_hint: `fix time boundary predicates, restore the canonical source_event_time range`

Generated Cypher:
```cypher
MATCH (x:Entity)-[:RELATED_TO]->(y:Entity) RETURN x, y LIMIT 10
```
Gold Cypher:
```cypher
MATCH (ic:IssueComment)-[r1:EVENT_ACTION]->(i:Issue {entity_id: 'I_156018#12095'}) MATCH (ic)-[r2:EVENT_ACTION]->(a:Actor) WHERE r1.service_rel_type = 'COMMENTED_ON_ISSUE' AND r2.service_rel_type = 'OPENED_BY' AND r1.source_event_time >= '2023-01-01T00:00:00Z' RETURN DISTINCT a.entity_id LIMIT 25
```

### AGGREGATION_ERROR
- case_id: `q_l3_03__free_form`
- query_id: `q_l3_03`
- failure_source: `free_form`
- validator_errors: `UNKNOWN_LABEL, UNKNOWN_REL, DIRECTION_MISMATCH`
- minimal_repair_hint: `restore aggregation projection, fix grouping or aggregate return fields`

Generated Cypher:
```cypher
MATCH (x:Entity)-[:RELATED_TO]->(y:Entity) RETURN x, y LIMIT 10
```
Gold Cypher:
```cypher
MATCH (pr:PullRequest {entity_id: 'PR_156018#11659'})-[rel:REFERENCE]->(x:ExternalResource) WHERE rel.service_rel_type = 'LINKS_TO' AND rel.source_event_time >= '2023-01-01T00:00:00Z' AND rel.source_event_time < '2024-01-01T00:00:00Z' RETURN rel.url_domain_etld1, count(*) AS c ORDER BY c DESC LIMIT 20
```


