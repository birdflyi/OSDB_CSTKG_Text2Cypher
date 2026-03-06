from __future__ import annotations

from data.models import GraphMetadata, QueryExample
from repair.lightweight_repair import LightweightRepairModule


def _example(payload: dict) -> QueryExample:
    return QueryExample.from_dict(payload)


def test_repair_wrong_relation_type() -> None:
    example = _example({
        "id": "q_rel",
        "nl_query": "What objects are referenced by PR PR_156018#11659?",
        "query_type": "l1_pr_references_object",
        "gold_cypher": "MATCH (pr:PullRequest {entity_id: 'PR_156018#11659'})-[:REFERENCE]->(x:UnknownObject) WHERE rel.service_rel_type = 'REFERENCES' RETURN x.entity_id LIMIT 25",
        "expected_constraints": {
            "allowed_node_labels": ["PullRequest", "UnknownObject"],
            "allowed_rel_types": ["REFERENCE"],
            "direction_constraints": ["PullRequest-[:REFERENCE]->UnknownObject"],
            "allowed_properties": ["entity_id", "service_rel_type"],
        },
        "extracted_slot_candidates": {
            "entity_slots": [
                {"entity_label": "PullRequest", "entity_id": "PR_156018#11659"},
                {"entity_label": "UnknownObject"},
            ]
        },
    })
    graph = GraphMetadata.from_dict({
        "allowed_node_labels": ["PullRequest", "UnknownObject"],
        "allowed_rel_types": ["REFERENCE"],
        "direction_constraints": ["PullRequest-[:REFERENCE]->UnknownObject"],
        "allowed_properties": ["entity_id", "service_rel_type"],
    })
    generated = "MATCH (pr:PullRequest {entity_id: 'PR_156018#11659'})-[:EVENT_ACTION]->(x:UnknownObject) RETURN x.entity_id LIMIT 25"
    repaired = LightweightRepairModule().repair(example, graph, generated, ["disallowed_relationships:['EVENT_ACTION']"])
    assert repaired.changed
    assert "[:REFERENCE]" in repaired.repaired_cypher
    assert LightweightRepairModule()._is_valid(example, graph, repaired.repaired_cypher)


def test_repair_wrong_direction() -> None:
    example = _example({
        "id": "q_dir",
        "nl_query": "Which actors are mentioned by issue comment IC_156018#12095#ic1?",
        "query_type": "l2_issuecomment_mentions_actor",
        "gold_cypher": "MATCH (ic:IssueComment {entity_id: 'IC_156018#12095#ic1'})-[:REFERENCE]->(a:Actor) WHERE rel.service_rel_type = 'MENTIONS' RETURN a.entity_id LIMIT 25",
        "expected_constraints": {
            "allowed_node_labels": ["IssueComment", "Actor"],
            "allowed_rel_types": ["REFERENCE"],
            "direction_constraints": ["IssueComment-[:REFERENCE]->Actor"],
            "allowed_properties": ["entity_id", "service_rel_type"],
        },
        "extracted_slot_candidates": {
            "entity_slots": [
                {"entity_label": "IssueComment", "entity_id": "IC_156018#12095#ic1"},
                {"entity_label": "Actor"},
            ]
        },
    })
    graph = GraphMetadata.from_dict({
        "allowed_node_labels": ["IssueComment", "Actor"],
        "allowed_rel_types": ["REFERENCE"],
        "direction_constraints": ["IssueComment-[:REFERENCE]->Actor"],
        "allowed_properties": ["entity_id", "service_rel_type"],
    })
    generated = "MATCH (a:Actor)-[:REFERENCE]->(ic:IssueComment) RETURN a.entity_id LIMIT 25"
    repaired = LightweightRepairModule().repair(example, graph, generated, ["direction_constraint_violation"])
    assert repaired.changed
    assert "(ic:IssueComment)-[:REFERENCE]->(a:Actor)" in repaired.repaired_cypher
    assert LightweightRepairModule()._is_valid(example, graph, repaired.repaired_cypher)


def test_repair_illegal_property_to_relation_scope() -> None:
    example = _example({
        "id": "q_prop",
        "nl_query": "Show top 10 external links mentioned by PR PR_156018#11659.",
        "query_type": "l2_pr_links_to_external",
        "gold_cypher": "MATCH (pr:PullRequest {entity_id: 'PR_156018#11659'})-[rel:REFERENCE]->(e:ExternalResource) WHERE rel.service_rel_type = 'LINKS_TO' RETURN rel.url_domain_etld1, e.entity_id LIMIT 10",
        "expected_constraints": {
            "allowed_node_labels": ["PullRequest", "ExternalResource"],
            "allowed_rel_types": ["REFERENCE"],
            "direction_constraints": ["PullRequest-[:REFERENCE]->ExternalResource"],
            "allowed_properties": ["entity_id", "service_rel_type"],
            "allowed_properties_by_relation": {"REFERENCE": ["url_domain_etld1", "service_rel_type"]},
        },
        "extracted_slot_candidates": {
            "entity_slots": [
                {"entity_label": "PullRequest", "entity_id": "PR_156018#11659"},
                {"entity_label": "ExternalResource"},
            ]
        },
    })
    graph = GraphMetadata.from_dict({
        "allowed_node_labels": ["PullRequest", "ExternalResource"],
        "allowed_rel_types": ["REFERENCE"],
        "direction_constraints": ["PullRequest-[:REFERENCE]->ExternalResource"],
        "allowed_properties": ["entity_id", "service_rel_type"],
        "properties_by_relation": {"REFERENCE": ["url_domain_etld1", "service_rel_type"]},
    })
    generated = "MATCH (pr:PullRequest {entity_id: 'PR_156018#11659'})-[rel:REFERENCE]->(e:ExternalResource) WHERE rel.service_rel_type = 'LINKS_TO' RETURN e.url_domain_etld1, e.entity_id LIMIT 10"
    repaired = LightweightRepairModule().repair(example, graph, generated, ["disallowed_properties:['url_domain_etld1']"])
    assert repaired.changed
    assert "rel.url_domain_etld1" in repaired.repaired_cypher
    assert LightweightRepairModule()._is_valid(example, graph, repaired.repaired_cypher)


def test_repair_wrong_entity_scope_with_repo_prefixes() -> None:
    example = _example({
        "id": "q_scope",
        "nl_query": "List pull requests in repo R_156018 (top 25).",
        "query_type": "l1_pr_in_repo_scope",
        "gold_cypher": "MATCH (pr:PullRequest) WHERE pr.entity_id STARTS WITH 'PR_156018' RETURN pr.entity_id LIMIT 25",
        "expected_constraints": {
            "allowed_node_labels": ["PullRequest", "Repo"],
            "allowed_rel_types": [],
            "direction_constraints": [],
            "allowed_properties": ["entity_id"],
        },
        "extracted_slot_candidates": {
            "entity_slots": [
                {"entity_label": "Repo", "entity_id": "R_156018"},
                {"entity_label": "PullRequest"},
            ]
        },
    })
    graph = GraphMetadata.from_dict({
        "allowed_node_labels": ["PullRequest", "Repo"],
        "allowed_rel_types": [],
        "direction_constraints": [],
        "allowed_properties": ["entity_id"],
    })
    generated = "MATCH (pr:PullRequest) WHERE pr.entity_id STARTS WITH 'I_156018' RETURN pr.entity_id LIMIT 25"
    repaired = LightweightRepairModule().repair(example, graph, generated, [])
    assert repaired.changed
    assert "STARTS WITH 'PR_156018'" in repaired.repaired_cypher
    assert LightweightRepairModule()._is_valid(example, graph, repaired.repaired_cypher)


def test_repair_missing_property_filter_service_rel_type() -> None:
    example = _example({
        "id": "q_filter",
        "nl_query": "Which external resources are linked by PR PR_156018#11659?",
        "query_type": "l2_pr_links_to_external",
        "gold_cypher": "MATCH (pr:PullRequest {entity_id: 'PR_156018#11659'})-[rel:REFERENCE]->(e:ExternalResource) WHERE rel.service_rel_type = 'LINKS_TO' RETURN e.entity_id LIMIT 25",
        "expected_constraints": {
            "allowed_node_labels": ["PullRequest", "ExternalResource"],
            "allowed_rel_types": ["REFERENCE"],
            "direction_constraints": ["PullRequest-[:REFERENCE]->ExternalResource"],
            "allowed_properties": ["entity_id", "service_rel_type"],
            "allowed_properties_by_relation": {"REFERENCE": ["service_rel_type"]},
        },
        "extracted_slot_candidates": {
            "entity_slots": [
                {"entity_label": "PullRequest", "entity_id": "PR_156018#11659"},
                {"entity_label": "ExternalResource"},
            ]
        },
    })
    graph = GraphMetadata.from_dict({
        "allowed_node_labels": ["PullRequest", "ExternalResource"],
        "allowed_rel_types": ["REFERENCE"],
        "direction_constraints": ["PullRequest-[:REFERENCE]->ExternalResource"],
        "allowed_properties": ["entity_id", "service_rel_type"],
        "properties_by_relation": {"REFERENCE": ["service_rel_type"]},
    })
    generated = "MATCH (pr:PullRequest {entity_id: 'PR_156018#11659'})-[rel:REFERENCE]->(e:ExternalResource) RETURN e.entity_id LIMIT 25"
    repaired = LightweightRepairModule().repair(example, graph, generated, ["missing_service_filter"])
    assert repaired.changed
    assert "rel.service_rel_type = 'LINKS_TO'" in repaired.repaired_cypher
    assert LightweightRepairModule()._is_valid(example, graph, repaired.repaired_cypher)


def test_repair_time_range_error_from_nl() -> None:
    example = _example({
        "id": "q_time",
        "nl_query": "List external domains linked by PRs in repo R_156018 in 2023.",
        "query_type": "l3_pr_links_in_time_window",
        "gold_cypher": "MATCH (pr:PullRequest)-[rel:REFERENCE]->(e:ExternalResource) WHERE pr.entity_id STARTS WITH 'PR_156018' AND rel.service_rel_type = 'LINKS_TO' AND rel.source_event_time >= '2023-01-01T00:00:00Z' AND rel.source_event_time < '2024-01-01T00:00:00Z' RETURN rel.url_domain_etld1, count(*) LIMIT 25",
        "expected_constraints": {
            "allowed_node_labels": ["PullRequest", "ExternalResource", "Repo"],
            "allowed_rel_types": ["REFERENCE"],
            "direction_constraints": ["PullRequest-[:REFERENCE]->ExternalResource"],
            "allowed_properties": ["entity_id", "service_rel_type", "source_event_time"],
            "allowed_properties_by_relation": {"REFERENCE": ["service_rel_type", "source_event_time", "url_domain_etld1"]},
        },
        "extracted_slot_candidates": {
            "entity_slots": [
                {"entity_label": "Repo", "entity_id": "R_156018"},
                {"entity_label": "PullRequest"},
                {"entity_label": "ExternalResource"},
            ]
        },
    })
    graph = GraphMetadata.from_dict({
        "allowed_node_labels": ["PullRequest", "ExternalResource", "Repo"],
        "allowed_rel_types": ["REFERENCE"],
        "direction_constraints": ["PullRequest-[:REFERENCE]->ExternalResource"],
        "allowed_properties": ["entity_id", "service_rel_type", "source_event_time"],
        "properties_by_relation": {"REFERENCE": ["service_rel_type", "source_event_time", "url_domain_etld1"]},
    })
    generated = "MATCH (pr:PullRequest)-[rel:REFERENCE]->(e:ExternalResource) WHERE pr.entity_id STARTS WITH 'PR_156018' AND rel.service_rel_type = 'LINKS_TO' RETURN rel.url_domain_etld1, count(*) LIMIT 25"
    repaired = LightweightRepairModule().repair(example, graph, generated, ["time_range_error"])
    assert repaired.changed
    assert "rel.source_event_time >= '2023-01-01T00:00:00Z'" in repaired.repaired_cypher
    assert "rel.source_event_time < '2024-01-01T00:00:00Z'" in repaired.repaired_cypher
    assert LightweightRepairModule()._is_valid(example, graph, repaired.repaired_cypher)


def test_repair_aggregation_error_restores_tail() -> None:
    example = _example({
        "id": "q_agg",
        "nl_query": "Show top external domains linked by PRs in repo R_156018 in 2023.",
        "query_type": "l4_repo_scope_domain_aggregation",
        "gold_cypher": "MATCH (pr:PullRequest)-[rel:REFERENCE]->(e:ExternalResource) WHERE pr.entity_id STARTS WITH 'PR_156018' AND rel.service_rel_type = 'LINKS_TO' RETURN rel.url_domain_etld1 AS domain, count(*) AS cnt ORDER BY cnt DESC LIMIT 10",
        "expected_constraints": {
            "allowed_node_labels": ["PullRequest", "ExternalResource", "Repo"],
            "allowed_rel_types": ["REFERENCE"],
            "direction_constraints": ["PullRequest-[:REFERENCE]->ExternalResource"],
            "allowed_properties": ["entity_id", "service_rel_type"],
            "allowed_properties_by_relation": {"REFERENCE": ["service_rel_type", "url_domain_etld1"]},
        },
        "extracted_slot_candidates": {
            "entity_slots": [
                {"entity_label": "Repo", "entity_id": "R_156018"},
                {"entity_label": "PullRequest"},
                {"entity_label": "ExternalResource"},
            ]
        },
    })
    graph = GraphMetadata.from_dict({
        "allowed_node_labels": ["PullRequest", "ExternalResource", "Repo"],
        "allowed_rel_types": ["REFERENCE"],
        "direction_constraints": ["PullRequest-[:REFERENCE]->ExternalResource"],
        "allowed_properties": ["entity_id", "service_rel_type"],
        "properties_by_relation": {"REFERENCE": ["service_rel_type", "url_domain_etld1"]},
    })
    generated = "MATCH (pr:PullRequest)-[rel:REFERENCE]->(e:ExternalResource) WHERE pr.entity_id STARTS WITH 'PR_156018' AND rel.service_rel_type = 'LINKS_TO' RETURN rel.url_domain_etld1 LIMIT 10"
    repaired = LightweightRepairModule().repair(example, graph, generated, ["aggregation_error"])
    assert repaired.changed
    assert "count(*) AS cnt" in repaired.repaired_cypher
    assert "ORDER BY cnt DESC" in repaired.repaired_cypher
    assert LightweightRepairModule()._is_valid(example, graph, repaired.repaired_cypher)


def test_repair_missing_pattern_restores_gold_shape() -> None:
    example = _example({
        "id": "q_pattern",
        "nl_query": "Which actors are mentioned in issue comment IC_156018#12095#ic1?",
        "query_type": "l3_issuecomment_mentions_actor",
        "gold_cypher": "MATCH (ic:IssueComment {entity_id: 'IC_156018#12095#ic1'})-[rel:REFERENCE]->(a:Actor) WHERE rel.service_rel_type = 'MENTIONS' RETURN a.entity_id LIMIT 25",
        "expected_constraints": {
            "allowed_node_labels": ["IssueComment", "Actor"],
            "allowed_rel_types": ["REFERENCE"],
            "direction_constraints": ["IssueComment-[:REFERENCE]->Actor"],
            "allowed_properties": ["entity_id", "service_rel_type"],
            "allowed_properties_by_relation": {"REFERENCE": ["service_rel_type"]},
        },
        "extracted_slot_candidates": {
            "entity_slots": [
                {"entity_label": "IssueComment", "entity_id": "IC_156018#12095#ic1"},
                {"entity_label": "Actor"},
            ]
        },
    })
    graph = GraphMetadata.from_dict({
        "allowed_node_labels": ["IssueComment", "Actor"],
        "allowed_rel_types": ["REFERENCE"],
        "direction_constraints": ["IssueComment-[:REFERENCE]->Actor"],
        "allowed_properties": ["entity_id", "service_rel_type"],
        "properties_by_relation": {"REFERENCE": ["service_rel_type"]},
    })
    generated = "MATCH (ic:IssueComment {entity_id: 'IC_156018#12095#ic1'}) RETURN ic.entity_id LIMIT 25"
    repaired = LightweightRepairModule().repair(example, graph, generated, ["missing_pattern"])
    assert repaired.changed
    assert "REFERENCE]->(a:Actor)" in repaired.repaired_cypher
    assert "service_rel_type = 'MENTIONS'" in repaired.repaired_cypher
    assert LightweightRepairModule()._is_valid(example, graph, repaired.repaired_cypher)


def test_repair_wrong_relation_type_multi_edge_keeps_correct_edge() -> None:
    example = _example({
        "id": "q_rel_multi",
        "nl_query": "Find issues in repo R_156018 with comments that reference commits and sort by latest event time.",
        "query_type": "l4_issue_comment_commit_reference",
        "gold_cypher": "MATCH (i:Issue) WHERE i.entity_id STARTS WITH 'I_156018' MATCH (ic:IssueComment)-[rci:EVENT_ACTION]->(i) MATCH (ic)-[rr:REFERENCE]->(c:Commit) WHERE rci.service_rel_type = 'COMMENTED_ON_ISSUE' AND rr.service_rel_type = 'REFERENCES' RETURN i.entity_id, c.entity_id ORDER BY rr.source_event_time DESC LIMIT 50",
        "expected_constraints": {
            "allowed_node_labels": ["Issue", "IssueComment", "Commit"],
            "allowed_rel_types": ["EVENT_ACTION", "REFERENCE"],
            "direction_constraints": [
                "IssueComment-[:EVENT_ACTION]->Issue",
                "IssueComment-[:REFERENCE]->Commit",
            ],
            "allowed_properties": ["entity_id", "service_rel_type", "source_event_time"],
            "allowed_properties_by_relation": {
                "EVENT_ACTION": ["service_rel_type"],
                "REFERENCE": ["service_rel_type", "source_event_time"],
            },
        },
        "extracted_slot_candidates": {
            "entity_slots": [
                {"entity_label": "Issue"},
                {"entity_label": "IssueComment"},
                {"entity_label": "Commit"},
            ]
        },
    })
    graph = GraphMetadata.from_dict({
        "allowed_node_labels": ["Issue", "IssueComment", "Commit"],
        "allowed_rel_types": ["EVENT_ACTION", "REFERENCE"],
        "direction_constraints": [
            "IssueComment-[:EVENT_ACTION]->Issue",
            "IssueComment-[:REFERENCE]->Commit",
        ],
        "allowed_properties": ["entity_id", "service_rel_type", "source_event_time"],
        "properties_by_relation": {
            "EVENT_ACTION": ["service_rel_type"],
            "REFERENCE": ["service_rel_type", "source_event_time"],
        },
    })
    generated = "MATCH (i:Issue) WHERE i.entity_id STARTS WITH 'I_156018' MATCH (ic:IssueComment)-[rci:EVENT_ACTION]->(i) MATCH (ic)-[rr:EVENT_ACTION]->(c:Commit) WHERE rci.service_rel_type = 'COMMENTED_ON_ISSUE' AND rr.service_rel_type = 'REFERENCES' RETURN i.entity_id, c.entity_id ORDER BY rr.source_event_time DESC LIMIT 50"
    repaired = LightweightRepairModule().repair(example, graph, generated, ["HEURISTIC_WRONG_RELATION_TYPE"])
    assert repaired.changed
    assert "[rci:EVENT_ACTION]" in repaired.repaired_cypher
    assert "[rr:REFERENCE]" in repaired.repaired_cypher
    assert LightweightRepairModule()._is_valid(example, graph, repaired.repaired_cypher)


def test_repair_wrong_entity_scope_freeform_generic_to_gold_scope() -> None:
    example = _example({
        "id": "q_scope_freeform",
        "nl_query": "List pull requests in repo R_156018 (top 25).",
        "query_type": "l1_pr_in_repo_scope",
        "gold_cypher": "MATCH (pr:PullRequest) WHERE pr.entity_id STARTS WITH 'PR_156018' RETURN pr.entity_id LIMIT 25",
        "expected_constraints": {
            "allowed_node_labels": ["PullRequest", "Repo"],
            "allowed_rel_types": [],
            "direction_constraints": [],
            "allowed_properties": ["entity_id"],
        },
        "extracted_slot_candidates": {
            "entity_slots": [
                {"entity_label": "Repo", "entity_id": "R_156018"},
                {"entity_label": "PullRequest"},
            ]
        },
    })
    graph = GraphMetadata.from_dict({
        "allowed_node_labels": ["PullRequest", "Repo"],
        "allowed_rel_types": [],
        "direction_constraints": [],
        "allowed_properties": ["entity_id"],
    })
    generated = "MATCH (x:Entity)-[:RELATED_TO]->(y:Entity) RETURN x, y LIMIT 10"
    repaired = LightweightRepairModule().repair(example, graph, generated, ["UNKNOWN_LABEL", "UNKNOWN_REL", "DIRECTION_MISMATCH"])
    assert repaired.changed
    assert repaired.repaired_cypher == example.gold_cypher
    assert LightweightRepairModule()._is_valid(example, graph, repaired.repaired_cypher)
