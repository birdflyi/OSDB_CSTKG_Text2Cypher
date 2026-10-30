import csv
import json
from pathlib import Path

import pytest

from adapter_work.osdb_formal_preparation import (
    Event, FAMILIES, build_preparation, manual_query, read_events,
    reference_answers, sha256, utc_time,
)


START = utc_time("2023-01-01T00:00:00Z")
END = utc_time("2024-01-01T00:00:00Z")


def event(issue, actor, action, time):
    return Event(("Issue", issue), ("Actor", actor), action, utc_time(time))


def counts(events):
    return {name: answer["count"] for name, answer in reference_answers(events, START, END).items()}


def test_ordered_reclosure_and_duplicate_events():
    rows = [event("1", "same", action, f"2023-01-0{day}T00:00:00Z")
            for action, day in [("CLOSED_BY", 1), ("REOPENED_BY", 2), ("CLOSED_BY", 3)]]
    assert counts(rows + rows) == {
        "ordered_reclosure": 1, "cross_issue_role_bridge": 0, "close_without_later_reopen": 1}


def test_reopen_tied_with_close_is_not_strict_cycle_or_terminal_close():
    rows = [event("1", "a", action, "2023-01-01T00:00:00Z")
            for action in ("CLOSED_BY", "REOPENED_BY", "CLOSED_BY")]
    assert all(value == 0 for value in counts(rows).values())


def test_role_bridge_requires_distinct_artifacts_and_other_opening_actor():
    rows = [event("a", "x", "OPENED_BY", "2023-02-01T00:00:00Z"),
            event("b", "y", "OPENED_BY", "2023-02-02T00:00:00Z"),
            event("b", "x", "CLOSED_BY", "2023-02-03T00:00:00Z")]
    assert counts(rows)["cross_issue_role_bridge"] == 1
    assert counts(rows[1:])["cross_issue_role_bridge"] == 0
    self_opened = [rows[0], event("b", "x", "OPENED_BY", "2023-02-02T00:00:00Z"), rows[2]]
    assert counts(self_opened)["cross_issue_role_bridge"] == 0


def test_missing_opening_never_infers_role_bridge():
    assert counts([event("1", "a", "CLOSED_BY", "2023-02-01T00:00:00Z")]) == {
        "ordered_reclosure": 0, "cross_issue_role_bridge": 0, "close_without_later_reopen": 1}


def test_half_open_interval_and_reopen_outside_window():
    rows = [event("at_start", "a", "CLOSED_BY", "2023-01-01T00:00:00Z"),
            event("at_start", "a", "REOPENED_BY", "2024-01-01T00:00:00Z"),
            event("at_end", "a", "CLOSED_BY", "2024-01-01T00:00:00Z"),
            event("before", "a", "CLOSED_BY", "2022-12-31T23:59:59.999999Z")]
    assert counts(rows)["close_without_later_reopen"] == 1
    assert all(value == 0 for value in counts([]).values())


@pytest.mark.parametrize("value", [None, "", "2023-01-01", "2023-01-01T00:00:00+08:00", "2023-02-30T00:00:00Z"])
def test_invalid_or_ambiguous_times_raise(value):
    with pytest.raises(ValueError):
        utc_time(value)


def test_wrong_identity_label_is_not_silently_coerced():
    with pytest.raises(ValueError):
        reference_answers([Event(("PullRequest", "1"), ("Actor", "1"), "CLOSED_BY", START)], START, END)
    with pytest.raises(ValueError):
        reference_answers([], END, START)


def write_csv(path, fields, rows):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def snapshot(tmp_path):
    folder = tmp_path / "csv"
    folder.mkdir()
    nodes = [dict(node_uid=label + ":same", label=label, entity_id="same",
                  properties_json=json.dumps({"entity_id": "same"})) for label in ("Issue", "Actor")]
    edges = [dict(source_node_uid="Issue:same", target_node_uid="Actor:same", native_rel_type="EVENT_ACTION",
                  service_rel_type="CLOSED_BY", properties_json=json.dumps({
                      "edge_uid": str(index), "native_rel_type": "EVENT_ACTION", "service_rel_type": "CLOSED_BY",
                      "source_event_time": time})) for index, time in enumerate((None, "2023-01-01T00:00:00Z"))]
    write_csv(folder / "nodes.csv", list(nodes[0]), nodes)
    write_csv(folder / "edges.csv", list(edges[0]), edges)
    hashes = {name: sha256(folder / name) for name in ("nodes.csv", "edges.csv")}
    return folder, hashes


def test_csv_typed_same_string_keys_and_missing_time_accounting(tmp_path):
    folder, hashes = snapshot(tmp_path)
    rows, diagnostics, _ = read_events(folder, hashes)
    assert len(rows) == 1
    assert rows[0].issue != rows[0].actor
    assert diagnostics["relevant_missing_timestamp_events"] == 1
    assert counts(rows)["close_without_later_reopen"] == 1
    with pytest.raises(ValueError, match="pinned historical snapshot"):
        read_events(folder, dict.fromkeys(hashes, "wrong"))


def test_reference_queries_preserve_temporal_and_identity_contracts():
    queries = {family: manual_query(family) for family in FAMILIES}
    assert "<datetime(r.source_event_time)" in queries["ordered_reclosure"]
    assert "a.entity_id<>b.entity_id" in queries["cross_issue_role_bridge"]
    assert "actor.entity_id<>other.entity_id" in queries["cross_issue_role_bridge"]
    assert "NOT EXISTS" in queries["close_without_later_reopen"]
    assert ">=datetime(c.source_event_time)" in queries["close_without_later_reopen"]
    assert all("count(DISTINCT" in query for query in queries.values())
    with pytest.raises(ValueError):
        manual_query("unknown")


def test_preparation_keeps_public_reference_separate_and_does_not_admit(tmp_path):
    folder, hashes = snapshot(tmp_path)
    history = tmp_path / "history.json"
    history.write_text(json.dumps([{"family": "issues_closed", "derived_structure_description": "old close count"}]))
    exposure = tmp_path / "exposure.json"
    exposure.write_text(json.dumps([{"task_id": "old_ACT03"}]))
    receipts = tmp_path / "receipts.json"
    sources = []
    for family in {value["source_family"] for value in FAMILIES.values()}:
        source = tmp_path / (family + ".md")
        source.write_text("Source motivation, not exact operational definition.")
        sources.append({"family": family, "sha256": sha256(source), "url": "https://example.invalid/"})
    receipts.write_text(json.dumps(sources))
    config = {"method_files": [str(Path(__file__))], "historical_review": str(history),
              "candidate_exposure": str(exposure), "source_receipts": str(receipts),
              "graphs": {"fixture": {"folder": str(folder), "sha256": hashes}}}
    out = tmp_path / "output"
    summary = build_preparation(config, out)
    assert summary["candidate_bindings"] == 3
    assert summary["formal_N"] is None and summary["formal_admitted"] == 0
    public = [json.loads(row) for row in (out / "public_inputs.jsonl").read_text().splitlines()]
    assert all(not {"rows", "query", "source", "private_witness_keys"} & row.keys() for row in public)
    refs = [json.loads(row) for row in (out / "references.jsonl").read_text().splitlines()]
    assert len(refs) == 3 and all(row["status"].endswith("UNEXECUTED") for row in refs)
    with pytest.raises(FileExistsError):
        build_preparation(config, out)
