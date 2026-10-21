"""Offline CSV-to-Cypher parity adapter and bounded static evidence, never a DB client."""
from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import yaml

from exporters.cypher_exporter import CypherExporter
from models import NormalizedEdge, NormalizedGraph, NormalizedNode

ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_BY_CELL = {
    "R01": "indv4_issue_opened_by", "R02": "indv4_repo_pull_request_filter",
    "R03": "indv4_reference_object", "R04.V1": "indv4_reference_external_property",
    "R04.V2": "indv5_reference_external_id_domain", "R07": "indv4_repo_pr_reference_window",
    "R09": "indv4_narrow_domain_aggregation", "R11": "indv4_issue_comment_commit",
    "R14": "indv5_issue_prefix_list",
}


def csv_to_import_cypher(csv_dir: Path, output: Path) -> dict[str, Any]:
    """Validate eligibility and decode JSON properties before using the existing exporter.

    The old CSV is not a Neo4j-admin file: node_uid/rel_type are not import headers,
    and properties_json must be decoded. This adapter never connects to Neo4j.
    """
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite {output}")
    schema = yaml.safe_load((ROOT / "data_real/pilot_queries/schema_metadata.yaml").read_text(encoding="utf-8"))
    graph = NormalizedGraph()
    with (csv_dir / "nodes.csv").open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            props = json.loads(row["properties_json"])
            if props.get("query_eligible") is not True or row["label"] not in schema["allowed_node_labels"]:
                raise ValueError("Unqualified node in execution CSV")
            if row["entity_id"].startswith("unresolved:") or row["entity_id"] != props.get("entity_id") or row["node_uid"] != f"{row['label']}:{row['entity_id']}":
                raise ValueError("Inconsistent execution identity")
            if row["node_uid"] in graph.nodes:
                raise ValueError("Duplicate execution node UID")
            graph.add_node(NormalizedNode(row["node_uid"], row["label"], row["entity_id"], props))
    edge_uids: set[str] = set()
    with (csv_dir / "edges.csv").open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            props = json.loads(row["properties_json"])
            native = row["rel_type"]
            if props.get("query_eligible") is not True or native not in schema["allowed_relationship_types"]:
                raise ValueError("Unqualified native edge in execution CSV")
            if native != row["native_rel_type"] or native != props.get("native_rel_type") or row["service_rel_type"] != props.get("service_rel_type"):
                raise ValueError("CSV/properties relation mismatch")
            if props.get("service_rel_type") not in schema["service_view_candidates"]:
                raise ValueError("Unsupported service relation in execution CSV")
            if props.get("edge_uid") in edge_uids or not props.get("edge_uid"):
                raise ValueError("Missing or duplicate edge UID would change record grain")
            if row["source_node_uid"] not in graph.nodes or row["target_node_uid"] not in graph.nodes:
                raise ValueError("Dangling execution edge")
            edge_uids.add(props["edge_uid"])
            graph.add_edge(NormalizedEdge(row["source_node_uid"], row["target_node_uid"], native, props))
    CypherExporter().export(graph, output)
    return {
        "physical_relation_match": "PHYSICAL_RELATION_MATCH",
        "service_relation_match": "PROPERTY_BASED_RELATION_MATCH",
        "schema_template_mismatch": False,
        "csv_decoding": "JSON_PROPERTIES_DECODED_BEFORE_CYPHER_EXPORT",
        "node_count": len(graph.nodes), "edge_count": len(graph.edges),
        "runtime_status": "UNRESOLVED_NEEDS_EVIDENCE", "runtime_result": "RUNTIME_NOT_VERIFIED",
    }


def _witnesses(graph: NormalizedGraph) -> dict[str, list[Any]]:
    def edges(source: str, native: str, service: str, target: str) -> list[Any]:
        return [edge for edge in graph.edges if (
            graph.nodes[edge.source_node_uid].label == source and edge.rel_type == native
            and edge.properties.get("service_rel_type") == service
            and graph.nodes[edge.target_node_uid].label == target
        )]

    external = edges("PullRequest", "REFERENCE", "LINKS_TO", "ExternalResource")
    unknown = edges("PullRequest", "REFERENCE", "REFERENCES", "UnknownObject")
    comments = edges("IssueComment", "EVENT_ACTION", "COMMENTED_ON_ISSUE", "Issue")
    commits = edges("IssueComment", "REFERENCE", "REFERENCES", "Commit")
    by_comment: dict[str, list[Any]] = defaultdict(list)
    for edge in comments:
        by_comment[edge.source_node_uid].append(edge)
    return {
        "R01": edges("Issue", "EVENT_ACTION", "OPENED_BY", "Actor"),
        "R02": [node for node in graph.nodes.values() if node.label == "PullRequest" and re.fullmatch(r"PR_[0-9]+#[0-9]+", node.entity_id)],
        "R03": unknown,
        "R04.V1": [edge for edge in external if edge.properties.get("url_domain_etld1")],
        "R04.V2": [edge for edge in external if edge.properties.get("url_domain_etld1")],
        "R07": [edge for edge in unknown if edge.properties.get("source_event_time")],
        "R09": [edge for edge in external if edge.properties.get("url_domain_etld1") and edge.properties.get("source_event_time")],
        "R11": [(comment, commit) for commit in commits if commit.properties.get("source_event_time") for comment in by_comment[commit.source_node_uid]],
        "R14": [node for node in graph.nodes.values() if node.label == "Issue" and re.fullmatch(r"I_[0-9]+#[0-9]+", node.entity_id)],
    }


def static_coverage(graph: NormalizedGraph) -> dict[str, Any]:
    pack = ROOT / "data_real/pilot_queries"
    templates = {}
    for name in ("independent_template_pack_v4.yaml", "independent_template_pack_v5.yaml"):
        data = yaml.safe_load((pack / name).read_text(encoding="utf-8"))
        templates.update({row["template_id"]: row for row in data["templates"]})
    witnesses = _witnesses(graph)
    contracts = {
        "R01": ["(i:Issue", "[rel:EVENT_ACTION]", "(a:Actor)", "rel.service_rel_type = 'OPENED_BY'", "RETURN a.entity_id"],
        "R02": ["(pr:PullRequest)", "pr.entity_id STARTS WITH $pr_base_prefix", "RETURN pr.entity_id"],
        "R03": ["(pr:PullRequest", "[rel:REFERENCE]", "(x:UnknownObject)", "rel.service_rel_type = 'REFERENCES'", "RETURN x.entity_id"],
        "R04.V1": ["(pr:PullRequest", "[rel:REFERENCE]", "(e:ExternalResource)", "rel.service_rel_type = 'LINKS_TO'", "RETURN rel.url_domain_etld1, e.entity_id"],
        "R04.V2": ["(pr:PullRequest", "[rel:REFERENCE]", "(e:ExternalResource)", "rel.service_rel_type = 'LINKS_TO'", "RETURN e.entity_id, rel.url_domain_etld1"],
        "R07": ["(pr:PullRequest)", "pr.entity_id STARTS WITH $pr_base_prefix", "(pr)-[rr:REFERENCE]->(x:UnknownObject)", "rr.service_rel_type = 'REFERENCES'", "rr.source_event_time >= $time_start", "rr.source_event_time < $time_end", "RETURN pr.entity_id, x.entity_id ORDER BY rr.source_event_time DESC"],
        "R09": ["(pr:PullRequest", "[rel:REFERENCE]", "(x:ExternalResource)", "rel.service_rel_type = 'LINKS_TO'", "rel.source_event_time >= $time_start", "rel.source_event_time < $time_end", "RETURN rel.url_domain_etld1, count(*) AS c ORDER BY c DESC"],
        "R11": ["(i:Issue)", "i.entity_id STARTS WITH $issue_base_prefix", "(ic:IssueComment)-[rci:EVENT_ACTION]->(i)", "(ic)-[rr:REFERENCE]->(c:Commit)", "rci.service_rel_type = 'COMMENTED_ON_ISSUE'", "rr.service_rel_type = 'REFERENCES'", "RETURN i.entity_id, c.entity_id ORDER BY rr.source_event_time DESC"],
        "R14": ["(i:Issue)", "i.entity_id STARTS WITH $issue_scope_prefix", "RETURN i.entity_id"],
    }
    cells = {}
    for cell, template_id in TEMPLATE_BY_CELL.items():
        skeleton = " ".join(templates[template_id]["cypher_skeleton"].split())
        match = all(fragment in skeleton for fragment in contracts[cell])
        match = match and skeleton.endswith(f"LIMIT {templates[template_id]['default_limit']}")
        if cell == "R11":
            match = match and "$time_start" not in skeleton and "$time_end" not in skeleton
        items = witnesses[cell]
        first = items[0] if items else None
        if isinstance(first, tuple):
            issue_id = graph.nodes[first[0].target_node_uid].entity_id
            anchor = {"issue_entity_id": issue_id, "issue_base_prefix": issue_id.split("#")[0], "record_indexes": [edge.properties["record_index"] for edge in first]}
        elif isinstance(first, NormalizedEdge):
            anchor = {"source_entity_id": graph.nodes[first.source_node_uid].entity_id, "target_entity_id": graph.nodes[first.target_node_uid].entity_id, "record_index": first.properties["record_index"]}
            if first.properties.get("source_event_time"):
                anchor["source_event_time"] = first.properties["source_event_time"]
        elif first is not None:
            anchor = {"entity_id": first.entity_id}
            anchor["pr_base_prefix" if cell == "R02" else "issue_scope_prefix"] = first.entity_id.split("#")[0]
        else:
            anchor = None
        cells[cell] = {
            "template_id": template_id, "required_contract_fragments": contracts[cell],
            "template_contract_verified": match,
            "default_limit": templates[template_id]["default_limit"],
            "classification": "SCHEMA_TEMPLATE_MISMATCH" if not match else "SCHEMA_STATIC_COMPATIBLE" if items else "SOURCE_FACT_MISSING",
            "static_witness_count": len(items), "anchor_witness": anchor,
            "execution_relation_contract": "PROPERTY_BASED_RELATION_MATCH" if cell not in {"R02", "R14"} else "ZERO_HOP_TYPED_PREFIX",
            "runtime_status": "RUNTIME_NOT_VERIFIED",
            "scope_limit": "POSITIVE_PREFIX_ONLY;NO_NEGATIVE_ADMISSION_CLAIM" if cell == "R14" else "STATIC_GRAPH_ARTIFACT_ONLY;NO_CYPHER_EXECUTION_OR_RESULT_EQUIVALENCE",
        }
    return {"cells": cells, "count_grain": "SOURCE_ROW_NOT_DEDUPLICATED;R11_COUNTS_JOINED_ROW_PAIRS", "runtime_status": "RUNTIME_NOT_VERIFIED"}


def unresolved_triage(projector: Any, input_path: Path) -> dict[str, Any]:
    old = input_path.parent / "d1_typed_projection_staging_v1/unresolved_cases.jsonl"
    if not old.exists():
        return {"v1_status": "V1_BASELINE_NOT_AVAILABLE", "v1_count": None}
    cases = [json.loads(line) for line in old.read_text(encoding="utf-8").splitlines() if line]
    details = []
    for case in cases:
        index, side = case["record_index"], case["side"]
        endpoint = projector.endpoints.get((index, side))
        if endpoint is None:
            continue
        current = next((item for item in projector.unresolved if item["record_index"] == index and item["side"] == side), {})
        evidence = current.get("alternative_evidence", {})
        label = endpoint["properties"]["declared_label"]
        key = "pull_review_comment_id" if label == "PullRequestReviewComment" else "pull_review_id" if label == "PullRequestReview" else ""
        complete = bool(key) and all(str(evidence.get(k, "")).isdigit() for k in ("repo_id", "issue_number", key))
        source = projector.endpoints.get((index, "source"), {})
        impacted = ["R03", "R07"] if label == "UnknownObject" and source.get("label") == "PullRequest" else []
        details.append({
            **case, "raw_relation_type": current.get("raw_relation_type"), "reason_v1_1": current.get("reason"),
            "current_label": endpoint["label"], "query_eligible": endpoint["properties"]["query_eligible"],
            "alternative_identity_status": "STRUCTURED_COMPONENTS_CANDIDATE_NOT_APPLIED_OR_EXTERNALLY_VALIDATED" if complete else "NO_COMPLETE_TYPED_ID_EVIDENCE",
            "alternative_evidence_keys": sorted(evidence), "directly_affected_b1_cells": impacted,
        })
    return {
        "v1_count": len(cases), "matched_current_rows": len(details),
        "v1_cases_by_side_relation_type_reason": dict(Counter("|".join(str(item.get(k) or "") for k in ("side", "raw_relation_type", "raw_entity_type", "reason_v1_1")) for item in details)),
        "v1_alternative_identity_status": dict(Counter(item["alternative_identity_status"] for item in details)),
        "v1_directly_affected_b1_record_counts": dict(Counter(cell for item in details for cell in item["directly_affected_b1_cells"])),
        "UNRESOLVED_IMPACT_BY_B1_CELL": {
            "R01": "NO_DIRECT_UNRESOLVED_INPUT_ANCHOR;CHECK_QUALIFIED_WITNESSES",
            "R02": "NO_SYNTHETIC_PR_ANCHORS_IN_EXECUTION;QUALIFIED_PR_POOL_AVAILABLE",
            "R03": "MISSING_TARGET_IDENTITIES_BLOCK_UNKNOWNOBJECT_OUTPUT_PATHS",
            "R04.V1": "INTERNAL_OR_AMBIGUOUS_URLS_EXCLUDED;QUALIFIED_EXTERNAL_PATHS_REMAIN",
            "R04.V2": "SAME_PATH_AS_V1;OUTPUT_ORDER_CHECKED_SEPARATELY",
            "R07": "MISSING_TARGET_IDENTITIES_BLOCK_UNKNOWNOBJECT_OUTPUT_PATHS_DESPITE_TIME_EVIDENCE",
            "R09": "ONLY_QUALIFIED_LINK_ROWS;COUNT_GRAIN_PRESERVED_WITHIN_ELIGIBLE_SLICE",
            "R11": "V1_UNRESOLVED_REVIEW_IDENTITIES_ARE_NOT_REQUIRED_BY_THIS_PATH;CHECK_ACTUAL_COMMENT_JOIN",
            "R14": "NO_SYNTHETIC_ISSUE_ANCHORS;POSITIVE_PREFIX_ONLY",
        },
        "details": details,
    }
