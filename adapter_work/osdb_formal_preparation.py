"""Prepare auditable OSDB candidates and CSV references, without any service.

These are AI-authored snapshot questions, not independent human questions or
admitted formal tasks. Project/date variants share a structure family. This
module never imports the tested compiler and never executes its manual Cypher.
"""
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import argparse
import csv
import hashlib
import json
from pathlib import Path
import re


PARAMETERS = {"start": "2023-01-01T00:00:00Z", "end": "2024-01-01T00:00:00Z"}
FAMILIES = {
    "ordered_reclosure": {
        "source_family": "issue_observed_duration",
        "output": "issue_count",
        "question": "Count distinct Issue identifiers with an observed CLOSED_BY event, then a strictly later REOPENED_BY event, then a strictly later CLOSED_BY event, all inside the observation interval. Other intervening events are allowed. Count each Issue once, regardless of event multiplicity or actors.",
        "structure": "Issue-key join of three EVENT_ACTION records; CLOSED_BY.time < REOPENED_BY.time < CLOSED_BY.time; distinct Issue count",
        "closest_old": ["issue_observed_duration", "issues_observed_active", "issues_closed"],
        "difference": "Requires an ordered three-event reclosure witness. Old duration selects an opening and subsequent close; activity is a one-window union. Neither alone certifies a reclosure witness.",
    },
    "cross_issue_role_bridge": {
        "source_family": "opening_actor_attribution",
        "output": "account_count",
        "question": "Count distinct Actor account identifiers that opened an Issue A and later closed a different Issue B. Issue B must have an observed opening by a different Actor strictly before that closing. All three events must lie inside the observation interval. Other openings or closings do not disqualify a witness. Count each account once.",
        "structure": "same Actor joins Issue A OPENED_BY and Issue B CLOSED_BY; A != B; Issue B OPENED_BY another Actor; both opening times < closing time; distinct Actor count",
        "closest_old": ["opening_actor_attribution", "issue_observed_response", "issue_openers"],
        "difference": "Requires a same-account cross-artifact role join and a different-opener predicate on the second Issue. Old attribution groups openings; old response checks comments on one Issue.",
    },
    "close_without_later_reopen": {
        "source_family": "issues_observed_active",
        "output": "issue_count",
        "question": "Count distinct Issue identifiers with at least one observed CLOSED_BY event inside the observation interval such that no REOPENED_BY event for that Issue is recorded at the same time or later before the interval ends. A reopen tied with a closing disqualifies that closing. This is an observed-event condition, not a claim about the Issue's real current state.",
        "structure": "Issue CLOSED_BY witness with correlated NOT EXISTS same Issue REOPENED_BY.time >= witness.time; distinct Issue count",
        "closest_old": ["issues_closed", "issues_observed_active", "missing_text_and_current_state", "observed_opening_gap"],
        "difference": "Anti-membership is correlated with each closing time on the same artifact, rather than a fixed account window. The old current-state question remains unanswerable as real-world state; this bounded observable condition does not repair that old task.",
    },
}
SCOPE = (
    "Use the whole supplied project snapshot, not an inferred main-repository prefix. "
    "Use only directed Issue-to-Actor EVENT_ACTION edges and their service_rel_type. "
    "The interval is [2023-01-01T00:00:00Z, 2024-01-01T00:00:00Z), in UTC. "
    "An event needs a present, valid source_event_time; missing timestamps do not "
    "establish a temporal witness. Identity is (node label, string entity_id). "
    "Accounts are not resolved humans; no bot exclusion. Duplicate events do not "
    "multiply the count. No inference about unobserved history or actual issue state."
)
_UTC = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z")


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def pin(path):
    path = Path(path).resolve()
    return {"path": str(path), "sha256": sha256(path)}


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def write_json(path, value):
    with Path(path).open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write("\n")


def write_rows(path, rows):
    with Path(path).open("x", encoding="utf-8", newline="\n") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def utc_time(value):
    if not isinstance(value, str) or not _UTC.fullmatch(value):
        raise ValueError(f"Expected explicit UTC timestamp, got {value!r}")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


@dataclass(frozen=True)
class Event:
    issue: tuple[str, str]
    actor: tuple[str, str]
    action: str
    time: datetime


def read_events(folder, expected_hashes):
    """Read typed projection CSV. Invalid identities/times fail, never become 0."""
    folder = Path(folder)
    sources = {name: pin(folder / name) for name in ("nodes.csv", "edges.csv")}
    if {name: item["sha256"] for name, item in sources.items()} != expected_hashes:
        raise ValueError("CSV bytes differ from the pinned historical snapshot")
    csv.field_size_limit(64 * 1024**2)
    nodes, keys = {}, set()
    diagnostics = Counter(physical_edges=0, relevant_physical_events=0,
                          relevant_missing_timestamp_events=0)
    with (folder / "nodes.csv").open(encoding="utf-8-sig", newline="") as stream:
        for row in csv.DictReader(stream):
            props = json.loads(row["properties_json"])
            identity = (row["label"], row["entity_id"])
            if (not identity[1] or type(props.get("entity_id")) is not str
                    or props["entity_id"] != identity[1]
                    or identity in keys or row["node_uid"] in nodes):
                raise ValueError("Missing, duplicate or inconsistent typed node identity")
            keys.add(identity)
            nodes[row["node_uid"]] = identity
    events, edge_ids = [], set()
    with (folder / "edges.csv").open(encoding="utf-8-sig", newline="") as stream:
        for row in csv.DictReader(stream):
            diagnostics["physical_edges"] += 1
            source, target = nodes[row["source_node_uid"]], nodes[row["target_node_uid"]]
            if (row["native_rel_type"] != "EVENT_ACTION" or source[0] != "Issue"
                    or target[0] != "Actor" or row["service_rel_type"] not in
                    {"OPENED_BY", "CLOSED_BY", "REOPENED_BY"}):
                continue
            props = json.loads(row["properties_json"])
            uid = props.get("edge_uid")
            if (not isinstance(uid, str) or not uid or uid in edge_ids
                    or props.get("service_rel_type") != row["service_rel_type"]
                    or props.get("native_rel_type") != "EVENT_ACTION"):
                raise ValueError("Inconsistent event identity or action fields")
            edge_ids.add(uid)
            diagnostics["relevant_physical_events"] += 1
            if props.get("source_event_time") is None:
                diagnostics["relevant_missing_timestamp_events"] += 1
                continue
            events.append(Event(source, target, row["service_rel_type"], utc_time(props["source_event_time"])))
    diagnostics["nodes"] = len(nodes)
    diagnostics["timed_events"] = len(events)
    for name, source in sources.items():
        if sha256(folder / name) != source["sha256"]:
            raise ValueError("Source changed while reading")
    return events, dict(diagnostics), sources


def reference_answers(events, start, end):
    """Independent set/record algorithm; return counts and private witness keys."""
    if start >= end:
        raise ValueError("Empty or reversed observation interval")
    by_issue, own_openings = defaultdict(lambda: defaultdict(list)), defaultdict(list)
    for event in events:
        if event.issue[0] != "Issue" or event.actor[0] != "Actor" or event.time.tzinfo != timezone.utc:
            raise ValueError("Reference requires typed Issue/Actor keys and UTC")
        if not start <= event.time < end:
            continue
        by_issue[event.issue][event.action].append(event)
        if event.action == "OPENED_BY":
            own_openings[event.actor].append((event.issue, event.time))
    reclosed, bridges, terminal = set(), set(), set()
    for issue, actions in by_issue.items():
        closed = actions["CLOSED_BY"]
        reopened = actions["REOPENED_BY"]
        if closed:
            first_close = min(event.time for event in closed)
            last_close = max(event.time for event in closed)
            if any(first_close < event.time < last_close for event in reopened):
                reclosed.add(issue)
            if not any(event.time >= last_close for event in reopened):
                terminal.add(issue)
        for closing in closed:
            other_opened = any(event.actor != closing.actor and event.time < closing.time
                               for event in actions["OPENED_BY"])
            opened_elsewhere = any(other != issue and when < closing.time
                                   for other, when in own_openings[closing.actor])
            if other_opened and opened_elsewhere:
                bridges.add(closing.actor)
    values = {"ordered_reclosure": reclosed, "cross_issue_role_bridge": bridges,
              "close_without_later_reopen": terminal}
    return {name: {"count": len(keys), "witness_keys": [list(key) for key in sorted(keys)]}
            for name, keys in values.items()}


def _window(variable):
    return (f"{variable}.source_event_time IS NOT NULL AND "
            f"datetime({variable}.source_event_time) >= datetime($start) AND "
            f"datetime({variable}.source_event_time) < datetime($end)")


def manual_query(family):
    """Handwritten Neo4j reference, pending real graph acceptance; no compiler."""
    if family == "ordered_reclosure":
        return ("MATCH (i:Issue)-[c1:EVENT_ACTION]->(:Actor), "
                "(i)-[r:EVENT_ACTION]->(:Actor), (i)-[c2:EVENT_ACTION]->(:Actor)\n"
                "WHERE c1.service_rel_type='CLOSED_BY' AND r.service_rel_type='REOPENED_BY' "
                "AND c2.service_rel_type='CLOSED_BY'\nAND " + " AND ".join(map(_window, ("c1", "r", "c2"))) +
                "\nAND datetime(c1.source_event_time)<datetime(r.source_event_time) "
                "AND datetime(r.source_event_time)<datetime(c2.source_event_time)\n"
                "RETURN count(DISTINCT i.entity_id) AS issue_count")
    if family == "cross_issue_role_bridge":
        return ("MATCH (a:Issue)-[o1:EVENT_ACTION]->(actor:Actor), "
                "(b:Issue)-[c:EVENT_ACTION]->(actor), (b)-[o2:EVENT_ACTION]->(other:Actor)\n"
                "WHERE o1.service_rel_type='OPENED_BY' AND c.service_rel_type='CLOSED_BY' "
                "AND o2.service_rel_type='OPENED_BY' AND a.entity_id<>b.entity_id "
                "AND actor.entity_id<>other.entity_id\nAND " + " AND ".join(map(_window, ("o1", "c", "o2"))) +
                "\nAND datetime(o1.source_event_time)<datetime(c.source_event_time) "
                "AND datetime(o2.source_event_time)<datetime(c.source_event_time)\n"
                "RETURN count(DISTINCT actor.entity_id) AS account_count")
    if family == "close_without_later_reopen":
        return ("MATCH (i:Issue)-[c:EVENT_ACTION]->(:Actor)\n"
                "WHERE c.service_rel_type='CLOSED_BY' AND " + _window("c") +
                "\nAND NOT EXISTS { MATCH (i)-[r:EVENT_ACTION]->(:Actor) "
                "WHERE r.service_rel_type='REOPENED_BY' AND " + _window("r") +
                " AND datetime(r.source_event_time)>=datetime(c.source_event_time) }\n"
                "RETURN count(DISTINCT i.entity_id) AS issue_count")
    raise ValueError(f"Unknown candidate family: {family}")


def build_preparation(config, output):
    """Create once; config pins methods before candidates and original snapshots."""
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    method_pins = [pin(path) for path in config["method_files"]]
    history = read_json(config["historical_review"])
    exposed = read_json(config["candidate_exposure"])
    receipts = {row["family"]: row for row in read_json(config["source_receipts"])}
    inputs = [pin(config[name]) for name in ("historical_review", "candidate_exposure", "source_receipts")]
    for family in {row["source_family"] for row in FAMILIES.values()}:
        path = Path(config["source_receipts"]).parent / (family + ".md")
        if sha256(path) != receipts[family]["sha256"]:
            raise ValueError("Cached public source differs from its receipt")
        receipts[family] = {**receipts[family], "cache": pin(path),
                            "locator": "Question and description; motivation only, not the operational query"}
    # This is persisted before data traversal or count inspection.
    protocol = {"kind": "AI_AUTHORED_CANDIDATE_PREPARATION_NOT_FORMAL_ADMISSION",
                "method_pins_before_reference_scan": method_pins, "input_pins": inputs,
                "parameters_fixed_before_scan": PARAMETERS, "structures_fixed_before_scan": FAMILIES,
                "input_scope": SCOPE, "method_selection_by_answer_or_success": False,
                "roles": {"candidate_author": "AI assistant", "reference_author": "same AI assistant",
                          "independent_human_review": False, "unseen_schema": False},
                "formal_N": None, "model_calls": 0, "database_calls": 0}
    write_json(output / "protocol.json", protocol)
    exposure = [{"kind": "historical", "position": row, "eligibility": "DEVELOPMENT_ONLY"} for row in history]
    exposure += [{"kind": "previous_candidate", "position": row,
                  "eligibility": "EXPOSED_DEVELOPMENT_ONLY"} for row in exposed]
    write_rows(output / "exposure.jsonl", exposure)
    comparisons = []
    for family, spec in FAMILIES.items():
        for row in history:
            comparisons.append({"candidate_family": family, "historical_position": {
                name: row.get(name) for name in ("project", "stratum", "specification_sha256", "id_sha256", "family")},
                "historical_structure": row["derived_structure_description"],
                "candidate_structure": spec["structure"],
                "flagged_close_family": row["family"] in spec["closest_old"],
                "difference_to_check": spec["difference"],
                "review_status": "PENDING_SEMANTIC_REVIEW_NOT_AUTO_CERTIFIED"})
        for row in exposed:
            comparisons.append({"candidate_family": family, "historical_candidate": row["task_id"],
                                "review_status": "PENDING_SEMANTIC_REVIEW_NOT_AUTO_CERTIFIED",
                                "warning": "Different ID or family string does not prove independent structure"})
    write_rows(output / "structure_comparisons.jsonl", comparisons)
    public, references, intake, snapshot_evidence = [], [], [], {}
    start, end = utc_time(PARAMETERS["start"]), utc_time(PARAMETERS["end"])
    for project, graph in config["graphs"].items():
        events, diagnostics, sources = read_events(graph["folder"], graph["sha256"])
        answers = reference_answers(events, start, end)
        snapshot_evidence[project] = {"sources": sources, "diagnostics": diagnostics,
                                      "actual_graph_acceptance": "NOT_EXECUTED"}
        for family, spec in FAMILIES.items():
            identifier = f"candidate:{project}:{family}:v47"
            public.append({"candidate_id": identifier, "graph": project,
                           "question": spec["question"] + " " + SCOPE,
                           "parameters": PARAMETERS, "snapshot_bindings": sources})
            references.append({"candidate_id": identifier, "columns": [spec["output"]],
                               "rows": [[answers[family]["count"]]],
                               "private_witness_keys": answers[family]["witness_keys"],
                               "query": manual_query(family), "parameters": PARAMETERS,
                               "status": "CSV_COMPUTED_MANUAL_QUERY_UNEXECUTED"})
            intake.append({"candidate_id": identifier, "project": project, "structure_family": family,
                           "source": receipts[spec["source_family"]],
                           "operationalization": "AI-authored bounded snapshot activity; not exact CHAOSS metric",
                           "structure_signature": spec["structure"], "closest_old_families": spec["closest_old"],
                           "difference_to_review": spec["difference"],
                           "exposure": "SAME_ASSISTANT_AND_EXPOSED_PROJECTS; OTHER_PRIOR_EXPOSURE_UNKNOWN",
                           "formal_ready": False, "formal_N": None,
                           "needed_acceptance": ["semantic structure review against 167 historical and 24 exposed candidates",
                                                 "manual query versus CSV on pinned real graph",
                                                 "scope and structural coverage approval; then freeze formal manifest"]})
    write_rows(output / "task_intake.jsonl", intake)
    write_rows(output / "public_inputs.jsonl", public)
    write_rows(output / "references.jsonl", references)
    write_json(output / "snapshots.json", snapshot_evidence)
    for item in method_pins + inputs:
        if sha256(item["path"]) != item["sha256"]:
            raise ValueError("A pinned preparation dependency changed")
    summary = {"candidate_bindings": len(intake), "candidate_structure_families": len(FAMILIES),
               "projects": len(config["graphs"]), "csv_references_computed": len(references),
               "manual_queries_prepared": len(references), "manual_queries_executed": 0,
               "historical_positions_registered": len(history), "exposed_candidates_registered": len(exposed),
               "pairwise_comparisons_pending_review": len(comparisons), "formal_admitted": 0, "formal_N": None,
               "model_calls": 0, "database_calls": 0,
               "zero_count_candidates_retained": sum(row["rows"] == [[0]] for row in references),
               "limits": ["Twelve project bindings would not be twelve independent semantic goals.",
                          "A known corpus plus three new AI-authored structures is not a representative independent confirmation benchmark.",
                          "Preparation does not admit or budget a model run; unsupported outcomes must remain in any admitted denominator."]}
    write_json(output / "summary.json", summary)
    write_json(output / "artifact_pins.json", [pin(path) for path in sorted(output.iterdir()) if path.is_file()])
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build_preparation(read_json(args.config), args.output), indent=2))
