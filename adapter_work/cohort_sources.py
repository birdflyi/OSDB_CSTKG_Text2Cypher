"""Build release-position indexes without models, databases, or sampling.

The population holds identifiers and reference bindings, not reference answers.
Public payloads and evaluator-only references are written to separate files.
Published duplicates remain distinct positions. Fingerprints are not evidence
of semantic equivalence or independent questions.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
import hashlib
import json
from pathlib import Path

from adapter_work.query_structure import extract_structure


def encoded(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def digest(value) -> str:
    return hashlib.sha256(encoded(value)).hexdigest()


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


class Population:
    def __init__(self, benchmark: str, output: Path):
        self.benchmark = benchmark
        self.output = output / benchmark
        self.rows: list[dict] = []
        self.public: list[dict] = []
        self.references: list[dict] = []
        self.pins: dict[str, str] = {}

    def pin(self, path: Path, expected: str | None = None) -> dict:
        name = str(path.resolve())
        if name not in self.pins:
            self.pins[name] = hashlib.sha256(path.read_bytes()).hexdigest()
        actual = self.pins[name]
        if expected is not None and actual != expected:
            raise ValueError(f"Source SHA mismatch: {path}")
        return {"path": name, "sha256": actual}

    def add(self, *, task_id: str, graph: str, split: str, source_group: str,
            source: Path, locator: dict, public: dict, reference: dict,
            query: str | None, question_family: str = "unknown", metadata: dict | None = None):
        source_pin = self.pin(source)
        position_id = self.benchmark + ":" + digest([source_pin["sha256"], locator])[:24]
        if not graph or not task_id:
            raise ValueError(f"Missing identity: {source}:{locator}")
        public_row = {"position_id": position_id, "task_id": task_id, "graph": graph, "public_input": public}
        reference_row = {"position_id": position_id, "task_id": task_id, "graph": graph, "reference": reference}
        structure = extract_structure(query)
        self.public.append(public_row)
        self.references.append(reference_row)
        self.rows.append({
            "position_id": position_id, "source_position_id": position_id,
            "benchmark": self.benchmark, "task_id": task_id, "graph": graph,
            "split": split, "source_group": source_group,
            "source_file": source_pin["path"], "source_sha256": source_pin["sha256"],
            "source_record_locator": locator, "public_input_sha256": digest(public),
            "public_binding": {"path": str(self.output / "public_inputs.jsonl"),
                               "line_1based": len(self.public), "record_sha256": digest(public_row)},
            "reference_binding": {"path": str(self.output / "references.jsonl"),
                                  "line_1based": len(self.references), "record_sha256": digest(reference_row)},
            "question_family": question_family, "structure": structure,
            "structure_bucket": structure["structure_bucket"],
            "question_fingerprint": digest([graph, public.get("question")]),
            "exact_question_query_fingerprint": digest([graph, public.get("question"), query]),
            "exposure_status": "unknown", "exposure_note": "No claim of unseen questions; prior-run audit required.",
            "source_metadata": metadata or {},
        })

    def save(self, expected_count: int) -> dict:
        if len(self.rows) != expected_count:
            raise ValueError(f"{self.benchmark}: expected {expected_count}, found {len(self.rows)}")
        ids = [r["position_id"] for r in self.rows]
        if len(set(ids)) != len(ids):
            raise ValueError("Duplicate source position identity")
        self.output.mkdir(parents=True, exist_ok=False)
        hashes = {}
        for name, rows in (("public_inputs.jsonl", self.public), ("references.jsonl", self.references)):
            data = b"".join(encoded(row) + b"\n" for row in rows)
            (self.output / name).write_bytes(data)
            hashes[name] = hashlib.sha256(data).hexdigest()
        for row in self.rows:
            row["public_binding"]["sha256"] = hashes["public_inputs.jsonl"]
            row["reference_binding"]["sha256"] = hashes["references.jsonl"]
        data = b"".join(encoded(row) + b"\n" for row in self.rows)
        (self.output / "population.jsonl").write_bytes(data)
        hashes["population.jsonl"] = hashlib.sha256(data).hexdigest()
        summary = {
            "benchmark": self.benchmark, "positions": len(self.rows),
            "unique_task_ids": len({r["task_id"] for r in self.rows}),
            "graphs": dict(sorted(Counter(r["graph"] for r in self.rows).items())),
            "splits": dict(sorted(Counter(r["split"] for r in self.rows).items())),
            "source_groups": dict(sorted(Counter(r["source_group"] for r in self.rows).items())),
            "structure_buckets": dict(sorted(Counter(r["structure_bucket"] for r in self.rows).items())),
            "unique_question_fingerprints": len({r["question_fingerprint"] for r in self.rows}),
            "unique_exact_question_query_fingerprints": len({r["exact_question_query_fingerprint"] for r in self.rows}),
            "question_family_unknown": sum(r["question_family"] == "unknown" for r in self.rows),
            "source_files": len(self.pins), "files": hashes,
            "limitations": ["Lexical structure is not AST validation or actual execution difficulty.",
                            "Fingerprints do not prove semantic identity or independence.",
                            "Exposure remains unknown unless separately audited.",
                            "Public payload is not a frozen prompt or prediction cache key."],
            "model_calls": 0, "database_calls": 0, "run_allowed": False,
        }
        (self.output / "source_pins.json").write_bytes(encoded(self.pins) + b"\n")
        (self.output / "summary.json").write_bytes(encoded(summary) + b"\n")
        return summary


def build_cypherbench(root: Path, output: Path) -> Population:
    folder = root / "experiments/public_benchmark_v1/cypherbench"
    pop = Population("cypherbench", output)
    manifest = read_json(folder / "manifest.json")
    pop.pin(folder / "manifest.json")
    for item in manifest["artifacts"]:
        if item["path"] in {"runtime/test.jsonl", "evaluator/test.jsonl"}:
            pop.pin(folder / item["path"], item["sha256"])
    public = read_jsonl(folder / "runtime/test.jsonl")
    references = read_jsonl(folder / "evaluator/test.jsonl")
    if len(public) != len(references):
        raise ValueError("CypherBench public/reference cardinality mismatch")
    for i, (p, r) in enumerate(zip(public, references)):
        if (p["qid"], p["graph"]) != (r["qid"], r["graph"]):
            raise ValueError("CypherBench public/reference identity mismatch")
        template = r.get("from_template")
        pop.add(task_id=p["qid"], graph=p["graph"], split="test", source_group="published",
                source=folder / "runtime/test.jsonl", locator={"line_1based": i + 1},
                public={"question": p["nl_question"], "graph": p["graph"], "schema_binding": None},
                reference={**r, "source_binding": {**pop.pin(folder / "evaluator/test.jsonl"), "line_1based": i + 1}},
                query=r["gold_cypher"], question_family="author_template:" + digest(template) if template else "unknown",
                metadata={"schema_binding_status": "requires_runtime_graph_schema", "dataset_revision": manifest["dataset_revision"]})
    return pop


def build_text2gql(root: Path, output: Path) -> Population:
    folder = root / "reports/text2gql_all_common_inputs_v1_20261027"
    pop = Population("text2gql", output)
    pins_path = folder / "source_input_hashes.json"
    pins = read_json(pins_path)
    pop.pin(pins_path)
    ref_path = root / "reports/text2gql_reference_identity_bridge_v1_20261027/reference_index.json"
    refs = {(r["graph"], r["id"]): (i, r) for i, r in enumerate(read_json(ref_path))}
    ref_pin = pop.pin(ref_path)
    public_by_id = {}
    for path in sorted(folder.glob("*_direct.jsonl")):
        pop.pin(path)
        for row in read_jsonl(path):
            payload = json.loads(row["messages"][1]["content"])
            key = (row["graph"], row["id"])
            if key in public_by_id:
                raise ValueError("Duplicate Text2GQL public input")
            public_by_id[key] = payload
    for source_name, sha in sorted(pins.items()):
        if "/dataset/test/" not in source_name.replace("\\", "/"):
            continue
        path = Path(source_name)
        pop.pin(path, sha)
        for i, r in enumerate(read_json(path)):
            graph, task_id = path.parent.parent.name, str(r["id"])
            payload = public_by_id[(graph, task_id)]
            if payload["question"] != r["initial_question"]:
                raise ValueError("Text2GQL prepared question differs from source")
            ref_i, reference = refs[(graph, task_id)]
            if hashlib.sha256(r["initial_gql"].encode()).hexdigest() != reference["query_sha256"]:
                raise ValueError("Text2GQL reference query binding mismatch")
            pop.add(task_id=task_id, graph=graph, split="test", source_group="published",
                    source=path, locator={"json_index_0based": i}, public=payload,
                    reference={"reference_query": r["initial_gql"], "stored_result": reference,
                               "source_binding": {**ref_pin, "json_index_0based": ref_i}}, query=r["initial_gql"],
                    metadata={"author_difficulty": r.get("difficulty"), "author_source": r.get("source"),
                              "author_database_field": r["database"],
                              "reference_status": reference["reference_status"],
                              "reference_contract_concern": reference["reference_contract_concern"]})
    return pop


def build_mindthequery(root: Path, output: Path) -> Population:
    source_root = root / "assets/t1_preparation_20261029/MindTheQuery"
    package = root / "assets/t1_preparation_20261029/property_graph_packages_v5_final/MindTheQuery"
    pop = Population("mindthequery", output)
    manifest = read_json(package / "manifest.json")
    pop.pin(package / "manifest.json")
    # Existing prepared IDs cover only part of the release. Preserve those IDs
    # where their source locator matches, and label new release-only IDs clearly.
    old_refs_path = package / "evaluation/references.jsonl"
    pop.pin(old_refs_path, manifest["files"]["evaluation/references.jsonl"])
    old_ids = {(r["source_file"].replace("\\", "/"), r["source_position"]): r["task_id"]
               for r in read_jsonl(old_refs_path)}
    for source in sorted((source_root / "Train_Test_Splits").glob("*/*/test/*.json")):
        relative = source.relative_to(source_root).as_posix()
        parts = source.relative_to(source_root / "Train_Test_Splits").parts
        source_group, graph = parts[:2]
        graph = {"bloom": "bloom50"}.get(graph, graph)
        schema = manifest["graphs"][graph]
        schema_pin = pop.pin(package / schema["schema_file"], schema["schema_sha256"])
        for i, r in enumerate(read_json(source)):
            task_id = old_ids.get((relative, i), "mtq-release:" + digest([relative, i])[:24])
            labels = {k: r[k] for k in r if k.endswith("passed") or k in {"logical [0/1]", "result [0/1]"}}
            pop.add(task_id=task_id, graph=graph, split="test", source_group=source_group,
                    source=source, locator={"json_index_0based": i},
                    public={"question": r["NL Question"], "graph": graph, "input_variant": "plain", "schema_binding": schema_pin},
                    reference={"reference_query": r["Cypher"], "validation_labels": labels}, query=r["Cypher"],
                    metadata={"released_unique_id": r["unique_id"], "author_category": source.stem,
                              "source_chunk": r.get("source_chunk"), "original_index": r.get("original_index"),
                              "validation_labels": labels})
    return pop


def build_zograscope(root: Path, output: Path) -> Population:
    source_root = root / "assets/t1_preparation_20261029/ZOGRASCOPE/data"
    package = root / "assets/t1_preparation_20261029/property_graph_packages_v5_final/ZOGRASCOPE"
    pop = Population("zograscope", output)
    manifest = read_json(package / "manifest.json")
    pop.pin(package / "manifest.json")
    paths = {name: package / name for name in ("public/plain.jsonl", "evaluation/references.jsonl", "evaluation/cohorts.jsonl")}
    for name, path in paths.items():
        pop.pin(path, manifest["files"][name])
    inputs = {r["task_id"]: r for r in read_jsonl(paths["public/plain.jsonl"])}
    refs = {r["task_id"]: r for r in read_jsonl(paths["evaluation/references.jsonl"])}
    cohorts = read_jsonl(paths["evaluation/cohorts.jsonl"])
    sources = {}
    for name in {r["source_file"] for r in cohorts}:
        path = source_root / name
        pop.pin(path)
        with path.open(encoding="utf-8-sig", newline="") as stream:
            sources[name] = list(csv.DictReader(stream))
    for split, filename in (("iid", "ids_iid_test.txt"), ("compositional", "ids_compositional_test.txt")):
        path = source_root / filename
        pop.pin(path)
        expected = [line.strip() for line in path.read_text().splitlines() if line.strip()]
        actual = [r["task_id"].split(":")[-1] for r in cohorts if r["split"] == split]
        if Counter(actual) != Counter(expected):
            raise ValueError(f"ZOGRASCOPE split membership mismatch: {split}")
    for c in cohorts:
        p, r = inputs[c["task_id"]], refs[c["task_id"]]
        source = source_root / c["source_file"]
        raw = sources[c["source_file"]][c["source_position"]]
        if raw["id"] != c["task_id"].split(":")[-1] or raw["mr"] != r["reference_query"] or raw["nl"] != p["question"]:
            raise ValueError("ZOGRASCOPE source/public/reference mismatch")
        schema_pin = pop.pin(package / p["schema_file"], p["schema_sha256"])
        pop.add(task_id=c["task_id"], graph=p["graph"], split=c["split"], source_group="published",
                source=source, locator={"csv_record_0based": c["source_position"], "split": c["split"]},
                public={"question": p["question"], "graph": p["graph"], "input_variant": "plain", "schema_binding": schema_pin},
                reference=r, query=r["reference_query"], question_family="author_template:" + str(r["template_id"]),
                metadata={"released_id": raw["id"], "query_type": r.get("query_type")})
    return pop


def build_population(root: Path, output: Path) -> dict:
    output.mkdir(parents=True, exist_ok=False)
    exposure_path = root / "experiments/ch7_engine_20261028/six_method_smoke_v30/inventory/matrix.json"
    exposure = read_json(exposure_path) if exposure_path.exists() else None
    summaries = []
    for builder, expected in ((build_cypherbench, 2348), (build_text2gql, 3826),
                              (build_mindthequery, 10113), (build_zograscope, 3370)):
        population = builder(root, output)
        if exposure is not None:
            exposure_pin = population.pin(exposure_path)
            known = {(str(cell["benchmark"]).lower(), str(p["task_id"]), str(p["graph"]))
                     for cell in exposure["cells"] for p in cell["positions"]}
            for row in population.rows:
                if (population.benchmark, row["task_id"], row["graph"]) in known:
                    row["exposure_status"] = "known_smoke_member"
                    row["exposure_note"] = "Member of V30 prepared smoke inventory; not asserted unseen."
                    row["exposure_binding"] = exposure_pin
        summary = population.save(expected)
        if population.benchmark == "zograscope":
            summary["unique_released_ids"] = len({r["source_metadata"]["released_id"] for r in population.rows})
            if summary["unique_released_ids"] != 2672:
                raise ValueError("Unexpected ZOGRASCOPE unique release IDs")
            (population.output / "summary.json").write_bytes(encoded(summary) + b"\n")
        summaries.append(summary)
    result = {"benchmarks": summaries, "positions": sum(s["positions"] for s in summaries),
              "osdb": {"formal_population": None, "status": "independent_formal_tasks_not_frozen"},
              "source_code": {str(Path(__file__).resolve()): hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                              str(Path(__file__).with_name("query_structure.py").resolve()): hashlib.sha256(Path(__file__).with_name("query_structure.py").read_bytes()).hexdigest()},
              "model_calls": 0, "database_calls": 0, "run_allowed": False}
    (output / "summary.json").write_bytes(encoded(result) + b"\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = build_population(args.root, args.output)
    print(json.dumps({"positions": result["positions"], "output": str(args.output)}))
