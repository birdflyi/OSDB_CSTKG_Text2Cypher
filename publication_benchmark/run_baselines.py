"""Publication-first development benchmark, independent of the legacy NL engine.

Gold comes from executable specifications over a fixed CSV graph, never an LLM.
This eight-case batch is a pipeline pilot, NOT a publication-ready benchmark.
"""
import argparse
import csv
import hashlib
import json
import math
import re
import time
from collections import Counter, defaultdict
from pathlib import Path

import requests

SCHEMA = """Graph snapshot: OSS collaboration records from the provided CSV projection.
Node labels include Issue, PullRequest (NOT PR), IssueComment, Commit, Actor,
Repo, ExternalResource. Node identity property: entity_id. Physical edge types:
EVENT_ACTION and REFERENCE. Edge service meaning: service_rel_type.
Issue -EVENT_ACTION[OPENED_BY]-> Actor.
PullRequest -REFERENCE[LINKS_TO]-> ExternalResource.
IssueComment -EVENT_ACTION[COMMENTED_ON_ISSUE]-> Issue.
IssueComment -REFERENCE[REFERENCES]-> Commit.
REFERENCE.source_event_time is an ISO UTC string; REFERENCE.url_domain_etld1
belongs to the EDGE, not the external-resource node. Keep parallel edge records.
Date windows are inclusive start, exclusive end, at UTC midnight.
An entity_id prefix is a literal STRING predicate, not graph membership.
RESOLVES/COUPLES_WITH are NOT supported factual relations; links or references
cannot establish that a commit resolves an issue. There is no conversational
context identifying an unspecified pull request. Never guess such an anchor.
Read-only retrieval and aggregation are allowed. A valid empty query result is
different from unsupported semantics or insufficient retrieved evidence.
"""
STATUSES = {"ANSWER", "NEEDS_INPUT", "UNSUPPORTED", "INSUFFICIENT_EVIDENCE", "ERROR"}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as h:
        json.dump(value, h, ensure_ascii=False, indent=2)
        h.write("\n")


def parse_array(text, ids, keys):
    text = text.strip()
    if "```" in text:
        blocks = re.findall(r"```(?:json)?\s*([\s\S]*?)\s*```", text)
        if len(blocks) != 1:
            raise ValueError("Ambiguous fenced output; do not select a preferred answer")
        text = blocks[0]
    rows = json.loads(text)
    if not isinstance(rows, list) or len(rows) != len(ids):
        raise ValueError("Incomplete batch; no filling or replacement")
    if any(not isinstance(r, dict) or set(r) != set(keys) for r in rows):
        raise ValueError("Unexpected response keys")
    if {r["id"] for r in rows} != set(ids) or len({r["id"] for r in rows}) != len(rows):
        raise ValueError("ID mismatch")
    if any(r["status"] not in STATUSES for r in rows):
        raise ValueError("Invalid decision")
    return sorted(rows, key=lambda r: r["id"])


def normalize_tables(answers, tasks):
    """Presentation only: named row fields -> public column order, no gold access."""
    columns = {r["id"]: r["columns"] for r in tasks}
    result = []
    for answer in answers:
        rows = []
        for row in answer["rows"]:
            if isinstance(row, dict):
                if set(row) != set(columns[answer["id"]]):
                    raise ValueError("Named row has missing/extra public columns; no semantic guessing")
                rows.append([row[k] for k in columns[answer["id"]]])
            else:
                rows.append(row)
        result.append({**answer, "rows": rows})
    return result


class Client:
    def __init__(self, config, out):
        self.config, self.out = config, out

    def call(self, stage, prompt, data, ids, keys, max_tokens=6000):
        cfg = self.config
        payload = {k: cfg[k] for k in ("model", "temperature", "top_p", "reasoning_effort")}
        payload.update(messages=[{"role": "system", "content": prompt},
                                 {"role": "user", "content": json.dumps(data, ensure_ascii=False)}], max_tokens=max_tokens)
        folder = self.out / "calls" / stage
        req, resp = folder / "request.json", folder / "response.json"
        if req.exists():
            if json.loads(req.read_text(encoding="utf-8")) != payload:
                raise RuntimeError("Cached inputs changed; use new experiment version")
            if not resp.exists():
                raise RuntimeError("Prior dispatch has no saved response; no automatic retry")
            record = json.loads(resp.read_text(encoding="utf-8"))
        else:
            save(req, payload)
            start = time.monotonic()
            try:
                response = requests.post(cfg["endpoint"], headers={"Authorization": "Bearer " + cfg["api_key"]},
                                         json=payload, timeout=(10, 300), allow_redirects=False)
                record = {"http_status": response.status_code, "seconds": time.monotonic() - start,
                          "body": response.json()}
                save(resp, record)
            except Exception as exc:
                save(folder / "transport_error.json", {"error_type": type(exc).__name__})
                raise RuntimeError("Dispatch failed; original attempt preserved") from None
        choices = record["body"].get("choices") or []
        if record["http_status"] != 200 or not choices or choices[0].get("finish_reason") != "stop":
            raise RuntimeError("Incomplete/non-success response retained without repair")
        return parse_array(choices[0]["message"]["content"], ids, keys)

    def batches(self, stage, prompt, schema, tasks, keys, batch_size):
        values = []
        for index in range(0, len(tasks), batch_size):
            part = tasks[index:index + batch_size]
            values.extend(self.call(stage + f"_part_{index//batch_size+1:02d}", prompt,
                                    {"schema": schema, "tasks": part}, [r["id"] for r in part], keys))
        return sorted(values, key=lambda r: r["id"])


def load_graph(folder):
    nodes, edges, docs = {}, [], []
    with (folder / "nodes.csv").open(encoding="utf-8-sig", newline="") as h:
        for r in csv.DictReader(h):
            nodes[r["node_uid"]] = (r["label"], r["entity_id"])
            docs.append({"id": "NODE:" + r["node_uid"], "text": r["label"] + " entity_id " + r["entity_id"]})
    with (folder / "edges.csv").open(encoding="utf-8-sig", newline="") as h:
        for r in csv.DictReader(h):
            p = json.loads(r["properties_json"])
            source, target = nodes[r["source_node_uid"]], nodes[r["target_node_uid"]]
            edge = {"source": source, "target": target, "physical": r["native_rel_type"],
                    "service": r["service_rel_type"], "time": p.get("source_event_time"),
                    "domain": p.get("url_domain_etld1"), "uid": p["edge_uid"], "source_uid": r["source_node_uid"]}
            edges.append(edge)
            docs.append({"id": "EDGE:" + edge["uid"], "text": " ".join(map(str, (
                source[0], source[1], "outgoing", edge["physical"], edge["service"], target[0], target[1],
                "source_event_time", edge["time"], "url_domain_etld1", edge["domain"])))})
    return nodes, edges, docs


def build_specs(nodes, edges):
    opened = sorted({e["source"][1] for e in edges if e["source"][0] == "Issue" and e["target"][0] == "Actor"
                     and e["physical"] == "EVENT_ACTION" and e["service"] == "OPENED_BY"})
    issue = opened[0]
    linked = [e for e in edges if e["source"][0] == "PullRequest" and e["target"][0] == "ExternalResource"
              and e["physical"] == "REFERENCE" and e["service"] == "LINKS_TO"]
    repeated = Counter((e["source"][1], e["target"][1]) for e in linked)
    witnesses = sorted({pr for (pr, target), count in repeated.items() if count > 1})
    if not witnesses:
        raise RuntimeError("No parallel-record witness; pilot specification not satisfied")
    pr = witnesses[0]
    comments = defaultdict(list)
    for e in edges:
        if e["source"][0] == "IssueComment" and e["target"][0] == "Issue" and e["physical"] == "EVENT_ACTION" and e["service"] == "COMMENTED_ON_ISSUE":
            comments[e["source_uid"]].append(e["target"][1])
    paths = []
    for e in edges:
        if e["source_uid"] in comments and e["target"][0] == "Commit" and e["physical"] == "REFERENCE" and e["service"] == "REFERENCES":
            paths.extend((i, e["target"][1]) for i in comments[e["source_uid"]])
    prefix = sorted({i.split("#")[0] for i, commit in paths})[0]
    ls = [e for e in linked if e["source"][1] == pr]
    counts = Counter(e["domain"] for e in ls if e["time"] and "2023-01-01T00:00:00Z" <= e["time"] < "2024-01-01T00:00:00Z")
    specs = []
    def add(question, columns, rows, status="ANSWER", group=""):
        specs.append({"id": f"Q{len(specs)+1:03d}", "question": question, "columns": columns,
                      "expected_status": status, "expected_rows": rows, "group": group})
    add(f"Who opened Issue {issue}? Return the Actor identifier.", ["actor_id"],
        [[e["target"][1]] for e in edges if e["source"] == ("Issue", issue) and e["target"][0] == "Actor" and e["physical"] == "EVENT_ACTION" and e["service"] == "OPENED_BY"], group="lookup")
    add(f"For PullRequest {pr}, list outgoing LINKS_TO records. Return the link domain followed by the ExternalResource identifier, retaining repeated records.",
        ["domain", "external_resource_id"], [[e["domain"], e["target"][1]] for e in ls], group="projection_multiplicity")
    add(f"For PullRequest {pr}, count outgoing LINKS_TO records by link domain on or after 2023-01-01 and before 2024-01-01 (UTC). Count records, not distinct targets.",
        ["domain", "record_count"], [[d, c] for d, c in counts.items()], group="time_and_aggregation_grain")
    add(f"How many Issue nodes have an entity_id starting with the literal prefix {prefix}? Count all matching nodes, not a returned sample.",
        ["issue_count"], [[sum(label == "Issue" and eid.startswith(prefix) for label, eid in nodes.values())]], group="global_count")
    add(f"Within Issue entity_id prefix {prefix}, count all paths where an IssueComment COMMENTED_ON_ISSUE the Issue and the same comment REFERENCES a Commit. Count matching path records, not distinct comments or commits.",
        ["path_record_count"], [[sum(i.startswith(prefix) for i, commit in paths)]], group="composite_path_count")
    add(f"For PullRequest {pr}, how many outgoing LINKS_TO records occurred on or after 2022-01-01 and before 2023-01-01 (UTC)? Return the count, including zero.",
        ["record_count"], [[sum(bool(e["time"] and "2022-01-01T00:00:00Z" <= e["time"] < "2023-01-01T00:00:00Z") for e in ls)]], group="zero_count")
    add("Which domains are linked by the pull request? No pull request identifier or earlier context has been provided.",
        ["domain"], [], "NEEDS_INPUT", "missing_anchor")
    add(f"Which Commit does Issue {issue} RESOLVES? Establish the actual resolving relationship, not merely a reference or link.",
        ["commit_id"], [], "UNSUPPORTED", "unsupported_semantics")
    return specs


def tokenize(text):
    return re.findall(r"[\w#@.:-]+", text.lower())


class BM25:
    def __init__(self, docs):
        self.docs = docs
        self.terms = [Counter(tokenize(d["text"])) for d in docs]
        self.lengths = [sum(t.values()) for t in self.terms]
        self.average = sum(self.lengths) / len(docs)
        self.df = Counter(term for terms in self.terms for term in terms)

    def retrieve(self, question, k=24):
        terms = set(tokenize(question)); n = len(self.docs)
        ranked = []
        for i, document in enumerate(self.terms):
            score = 0.0
            for term in terms & document.keys():
                tf, df = document[term], self.df[term]
                idf = math.log(1 + (n - df + .5) / (df + .5))
                score += idf * tf * 2.5 / (tf + 1.5 * (.25 + .75 * self.lengths[i] / self.average))
            if score > 0:
                ranked.append((-score, i))
        return [self.docs[i] for score, i in sorted(ranked)[:k]]


LOCAL = requests.Session()
LOCAL.trust_env = False
DB_URL = "http://127.0.0.1:17475/db/neo4j/tx/commit"


def db(query):
    result = LOCAL.post(DB_URL, json={"statements": [{"statement": query, "resultDataContents": ["row"]}]}, timeout=(3, 45))
    result.raise_for_status()
    body = result.json()
    if body.get("errors"):
        raise RuntimeError(json.dumps(body["errors"]))
    return body["results"][0]


def execute(query):
    if not isinstance(query, str) or not re.match(r"^\s*MATCH\b", query, re.I) or re.search(
        r";|\b(CREATE|MERGE|DELETE|DETACH|SET|REMOVE|DROP|CALL|LOAD|FOREACH|UNION|USE)\b|\b(apoc|gds|dbms)\s*\.", query, re.I):
        return {"status": "EXECUTION_ERROR", "reason": "bounded_readonly_precheck"}
    try:
        db("EXPLAIN " + query)
        result = db(query)
        if len(result["data"]) > 200:
            return {"status": "EXECUTION_ERROR", "reason": "pilot_output_budget_exceeded"}
        return {"status": "EXECUTED", "columns": result["columns"], "rows": [r["row"] for r in result["data"]]}
    except Exception as exc:
        return {"status": "EXECUTION_ERROR", "reason": str(exc)}


def score(specs, answers, allowed_evidence):
    def numeric_value(value):
        # JSON numbers 3 and 3.0 have the same value. Strings/bools stay distinct.
        if isinstance(value, float) and math.isfinite(value) and value.is_integer():
            return int(value)
        if isinstance(value, list):
            return [numeric_value(v) for v in value]
        return value

    by_id = {r["id"]: r for r in answers}
    records = []
    for s in specs:
        a = by_id[s["id"]]
        if not isinstance(a["rows"], list) or not isinstance(a["evidence_ids"], list):
            raise ValueError("Answer rows/citations must be arrays")
        if not all(isinstance(r, list) and len(r) == len(s["columns"]) for r in a["rows"]):
            raise ValueError("Wrong table shape")
        bag = lambda rows: Counter(json.dumps(numeric_value(r), ensure_ascii=False, sort_keys=True) for r in rows)
        correct = a["status"] == s["expected_status"] and bag(a["rows"]) == bag(s["expected_rows"])
        citations_valid = all(isinstance(e, str) and e in allowed_evidence[s["id"]] for e in a["evidence_ids"])
        evidence_linked = citations_valid and (bool(a["evidence_ids"]) if a["status"] == "ANSWER" else True)
        records.append({"id": s["id"], "expected_status": s["expected_status"], "status": a["status"],
                        "table_and_decision_correct": correct, "evidence_links_valid": evidence_linked,
                        "correct_with_valid_evidence_links": correct and evidence_linked})
    supported = [r for r in records if r["expected_status"] == "ANSWER"]
    answered = [r for r in records if r["status"] == "ANSWER"]
    return {"records": records, "summary": {"cases": len(records),
        "table_and_decision_correct": sum(r["table_and_decision_correct"] for r in records),
        "correct_with_valid_evidence_links": sum(r["correct_with_valid_evidence_links"] for r in records),
        "supported_cases": len(supported), "correct_supported": sum(r["table_and_decision_correct"] for r in supported),
        "supported_answer_coverage": sum(r["status"] == "ANSWER" for r in supported) / len(supported) if supported else None,
        "answered_cases": len(answered), "wrong_answered_cases": sum(not r["table_and_decision_correct"] for r in answered),
        "boundary_wrong_acceptances": sum(r["expected_status"] != "ANSWER" and r["status"] == "ANSWER" for r in records),
        "note": "Evidence-link validity checks provenance references, not independent entailment. Exact specifications cover this finite controlled pilot only."}}


ANSWER_PROMPT = """Answer repository questions using only supplied evidence and schema.
Return ONLY a JSON array; object keys exactly id,status,rows,evidence_ids.
status is ANSWER, NEEDS_INPUT, UNSUPPORTED, INSUFFICIENT_EVIDENCE, or ERROR.
Rows must follow the supplied column order, retain record multiplicity and use
JSON numbers for counts. If not ANSWER, rows must be []. Do not guess missing
anchors or infer RESOLVES from REFERENCES. Cite supplied evidence IDs for answers.
Retrieval subsets cannot establish exhaustive counts merely by counting snippets.
If evidence is insufficient, say so rather than inventing a numeric answer.
"""


def run(config_path, data, receipt_path, out, batch_size=2):
    if batch_size not in {1, 2, 4, 8}:
        raise ValueError("Unsupported fixed batch size")
    data, out = Path(data).resolve(), Path(out).resolve()
    root = Path(__file__).resolve().parents[1]
    if out == root or root in out.parents:
        raise ValueError("Keep model/data artifacts outside checkout")
    cfg = json.loads(Path(config_path).read_text(encoding="utf-8"))
    receipt = json.loads(Path(receipt_path).read_text(encoding="utf-8"))
    hashes = {p.name: sha(p) for p in (data / "nodes.csv", data / "edges.csv")}
    if hashes != receipt["source_hashes"]:
        raise ValueError("CSV and imported graph differ")
    if db("CALL db.info() YIELD id, name RETURN id, name")["data"] != receipt["database_identity"]:
        raise ValueError("Wrong dedicated database")
    nodes, edges, docs = load_graph(data)
    specs = build_specs(nodes, edges)
    tasks = [{k: s[k] for k in ("id", "question", "columns")} for s in specs]
    ids = [r["id"] for r in tasks]
    manifest = {"mode": "PUBLICATION_BASELINE_DEVELOPMENT_PILOT_NOT_HELDOUT", "data_hashes": hashes,
        "code_sha256": sha(__file__), "model": cfg["model"], "endpoint": cfg["endpoint"],
        "generation_parameters": {k: cfg[k] for k in ("temperature", "top_p", "reasoning_effort")},
        "schema": SCHEMA, "max_model_calls": 3 * math.ceil(len(tasks)/batch_size),
        "request_batch_size": batch_size, "max_completion_tokens_per_call": 6000,
        "automatic_retries": 0, "BM25_top_k": 24,
        "sources": "Task specifications authored from actual CSV facts; no model gold or legacy templates/IR",
        "arms": ["BM25_FACT_RAG", "ZERO_SHOT_SCHEMA_TEXT2CYPHER_EXECUTE_ANSWER"],
        "comparability": "Same source graph/model/tasks/output protocol; retrieval access and token counts differ by design and are reported"}
    if (out / "manifest.json").exists():
        if json.loads((out / "manifest.json").read_text(encoding="utf-8")) != manifest:
            raise ValueError("Manifest changed")
    else:
        save(out / "manifest.json", manifest)
        save(out / "task_specifications.json", specs)
        save(out / "tasks.json", tasks)
    client = Client(cfg, out)
    retrieval = BM25(docs)
    contexts = [{**task, "retrieved_facts": retrieval.retrieve(task["question"])} for task in tasks]
    if not (out / "retrieval_contexts.json").exists():
        save(out / "retrieval_contexts.json", contexts)
    rag = client.batches("01_bm25_rag", ANSWER_PROMPT, SCHEMA, contexts,
                         {"id", "status", "rows", "evidence_ids"}, batch_size)
    queries = client.batches("02_text2cypher", "You generate read-only Cypher against the supplied schema. "
        "Return ONLY a JSON array with keys id,status,cypher,reason. ANSWER means one literal query; "
        "other statuses require cypher null. Preserve all conditions, requested columns and record grain. "
        "Use the full graph for aggregate counts. Do not replace missing anchors or unsupported semantics "
        "with nearby supported tasks. Status options: ANSWER,NEEDS_INPUT,UNSUPPORTED,INSUFFICIENT_EVIDENCE,ERROR.",
        SCHEMA, tasks, {"id", "status", "cypher", "reason"}, batch_size)
    executions, answer_context = [], []
    for task in tasks:
        q = next(r for r in queries if r["id"] == task["id"])
        if q["status"] == "ANSWER":
            result = execute(q["cypher"])
        else:
            if q["cypher"] is not None:
                raise ValueError("Non-answer decision contains query")
            result = {"status": "NO_QUERY"}
        executions.append({"id": task["id"], "generation": q, "execution": result})
        answer_context.append({**task, "retrieval_decision": q["status"], "query": q["cypher"],
            "execution": result, "evidence_id": "EXEC:" + task["id"] if result["status"] == "EXECUTED" else None})
    if not (out / "query_execution.json").exists():
        save(out / "query_execution.json", executions)
    graph_answers = client.batches("03_graph_answers", ANSWER_PROMPT +
        "\nExecution errors are not evidence for zero matches. Use EXEC identifiers for executed results.",
        SCHEMA, answer_context, {"id", "status", "rows", "evidence_ids"}, batch_size)
    if not (out / "answers_model_parsed.json").exists():
        save(out / "answers_model_parsed.json", {"BM25_FACT_RAG": rag, "SCHEMA_GRAPH_QA": graph_answers})
    rag, graph_answers = normalize_tables(rag, tasks), normalize_tables(graph_answers, tasks)
    if not (out / "answers.json").exists():
        save(out / "answers.json", {"BM25_FACT_RAG": rag, "SCHEMA_GRAPH_QA": graph_answers})
    rag_evidence = {r["id"]: {f["id"] for f in r["retrieved_facts"]} for r in contexts}
    graph_evidence = {r["id"]: {"EXEC:" + r["id"]} if r["execution"]["status"] == "EXECUTED" else set() for r in executions}
    scores = {"BM25_FACT_RAG": score(specs, rag, rag_evidence), "SCHEMA_GRAPH_QA": score(specs, graph_answers, graph_evidence)}
    if not (out / "scores.json").exists():
        save(out / "scores.json", scores)
    calls = [json.loads(p.read_text(encoding="utf-8")) for p in (out / "calls").glob("*/response.json")]
    summary = {"mode": manifest["mode"], "cases": len(specs), "arm_scores": {k: v["summary"] for k, v in scores.items()},
        "model_calls": len(calls), "total_tokens": sum(r["body"].get("usage", {}).get("total_tokens", 0) for r in calls),
        "provider_returned_models": sorted({r["body"].get("model", "UNKNOWN") for r in calls}),
        "call_usage": {p.parent.name: json.loads(p.read_text(encoding="utf-8"))["body"].get("usage") for p in (out / "calls").glob("*/response.json")},
        "execution_errors": sum(r["execution"]["status"] == "EXECUTION_ERROR" for r in executions),
        "scope": "Eight controlled single-graph development cases. No proposed-method advantage or exact published-system reproduction established."}
    if not (out / "summary.json").exists():
        save(out / "summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--graph-receipt", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--batch-size", type=int, default=2)
    args = parser.parse_args()
    run(args.config, args.data, args.graph_receipt, args.output, args.batch_size)
