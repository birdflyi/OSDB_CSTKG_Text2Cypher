from collections import Counter

import pytest

from run_baselines import BM25, build_specs, normalize_tables, parse_array, score


def fixture_graph():
    nodes = {str(i): pair for i, pair in enumerate([
        ("Issue", "I_1#1"), ("Issue", "I_1#2"), ("Issue", "I_10#9"),
        ("Actor", "A_3"), ("PullRequest", "PR_1#1"),
        ("ExternalResource", "E_1"), ("IssueComment", "IC_1#1"), ("Commit", "C_1@abc")])}
    def edge(s, t, physical, service, uid, source_uid, domain=None):
        return {"source": s, "target": t, "physical": physical, "service": service,
                "uid": uid, "source_uid": source_uid, "time": "2023-05-01T00:00:00Z", "domain": domain}
    edges = [edge(("Issue", "I_1#1"), ("Actor", "A_3"), "EVENT_ACTION", "OPENED_BY", "a", "i"),
             edge(("IssueComment", "IC_1#1"), ("Issue", "I_1#1"), "EVENT_ACTION", "COMMENTED_ON_ISSUE", "b", "ic"),
             edge(("IssueComment", "IC_1#1"), ("Commit", "C_1@abc"), "REFERENCE", "REFERENCES", "c", "ic")]
    for uid in ("d", "e"):
        edges.append(edge(("PullRequest", "PR_1#1"), ("ExternalResource", "E_1"), "REFERENCE", "LINKS_TO", uid, "pr", "example.org"))
    return nodes, edges


def test_specs_preserve_edge_multiplicity_zero_counts_and_literal_prefix():
    specs = build_specs(*fixture_graph())
    assert specs[1]["expected_rows"] == [["example.org", "E_1"], ["example.org", "E_1"]]
    assert specs[2]["expected_rows"] == [["example.org", 2]]
    assert specs[3]["expected_rows"] == [[3]]  # I_10 is inside literal I_1 prefix.
    assert specs[5]["expected_rows"] == [[0]]
    assert [s["expected_status"] for s in specs[-2:]] == ["NEEDS_INPUT", "UNSUPPORTED"]


def test_all_abstain_cannot_score_as_high_supported_performance():
    specs = build_specs(*fixture_graph())
    answers = [{"id": s["id"], "status": s["expected_status"] if s["expected_status"] != "ANSWER" else "INSUFFICIENT_EVIDENCE",
                "rows": [], "evidence_ids": []} for s in specs]
    result = score(specs, answers, {s["id"]: set() for s in specs})["summary"]
    assert result["table_and_decision_correct"] == 2
    assert result["correct_supported"] == 0
    assert result["supported_answer_coverage"] == 0


def test_scores_reject_deduplicated_rows_and_invalid_citations():
    specs = build_specs(*fixture_graph())[1:2]
    answer = {"id": specs[0]["id"], "status": "ANSWER", "rows": [["example.org", "E_1"]], "evidence_ids": ["fake"]}
    record = score(specs, [answer], {answer["id"]: {"valid"}})["records"][0]
    assert not record["table_and_decision_correct"]
    assert not record["evidence_links_valid"]


def test_truncation_and_extra_gold_fields_are_rejected():
    with pytest.raises(ValueError):
        parse_array('[{"id":"Q001"}', ["Q001"], {"id", "status"})
    with pytest.raises(ValueError):
        parse_array('[{"id":"Q001","status":"ANSWER","expected_rows":[]}]', ["Q001"], {"id", "status"})


def test_retrieval_is_deterministic_and_never_reads_expected_answers():
    docs = [{"id": "1", "text": "Issue I_1#1 OPENED_BY Actor A_3"},
            {"id": "2", "text": "PullRequest PR_2#1 LINKS_TO ExternalResource"}]
    index = BM25(docs)
    assert index.retrieve("Who opened Issue I_1#1?", k=1)[0]["id"] == "1"
    assert index.retrieve("Who opened Issue I_1#1?", k=1) == index.retrieve("Who opened Issue I_1#1?", k=1)


def test_presentation_normalization_never_changes_values_or_fills_fields():
    text = 'Explanation\n```json\n[{"id":"Q001","status":"ANSWER","rows":[{"count":17}],"evidence_ids":[]}]\n```'
    rows = parse_array(text, ["Q001"], {"id", "status", "rows", "evidence_ids"})
    assert normalize_tables(rows, [{"id": "Q001", "columns": ["count"]}])[0]["rows"] == [[17]]
    with pytest.raises(ValueError):
        normalize_tables(rows, [{"id": "Q001", "columns": ["other"]}])
    with pytest.raises(ValueError):
        parse_array(text + '\n```json\n[]\n```', ["Q001"], {"id", "status", "rows", "evidence_ids"})


def test_failure_record_never_supplies_a_semantic_answer(tmp_path, monkeypatch):
    import json
    import run_resilient_baselines as resilient
    from run_baselines import Client
    folder = tmp_path / "calls/stage"
    folder.mkdir(parents=True)
    (folder / "response.json").write_text(json.dumps({"body": {"choices": [{"finish_reason": "length"}]}}))
    def fail(*args, **kwargs):
        raise RuntimeError("Incomplete")
    monkeypatch.setattr(resilient, "ORIGINAL_CALL", fail)
    result = resilient.call_recording_failures(Client({}, tmp_path), "stage", "", {}, ["Q001"], {"id", "status", "rows", "evidence_ids"})
    assert result == [{"id": "Q001", "status": "ERROR", "rows": [], "evidence_ids": []}]
    assert (folder / "recorded_failure.json").exists()
    with pytest.raises(RuntimeError):
        resilient.call_recording_failures(Client({}, tmp_path), "unknown_live_stage", "", {}, ["Q001"], {"id", "status", "rows", "evidence_ids"})
