import json
from pathlib import Path

import pytest

from adapter_work.common.cohort import canonical, digest, load_cohort, select_positions


def pin(path):
    return {"path": str(path.resolve()), "sha256": digest(path.read_bytes())}


def save(path, value):
    path.write_bytes(canonical(value) + b"\n")
    return pin(path)


def make_bundle(tmp_path, *, benchmark="cypherbench", repeated_task=False, count=2):
    tmp_path.mkdir(parents=True, exist_ok=True)
    source = tmp_path / "release.json"
    source.write_bytes(b"[]")
    public, refs, population = [], [], []
    schema = {"name": "g", "entities": [{"label": "N", "properties": {"name": "str"}}], "relations": []}
    for i in range(count):
        position, task = f"p{i:04d}", "same" if repeated_task else f"task{i}"
        payload = {"question": "Which nodes?", "graph": "g", "schema": schema}
        public.append({"position_id": position, "task_id": task, "graph": "g", "public_input": payload})
        refs.append({"position_id": position, "task_id": task, "graph": "g", "reference": {"secret": "DO_NOT_READ_OR_SEND"}})
        population.append({"position_id": position, "benchmark": benchmark, "task_id": task, "graph": "g",
                           "split": "test" if i == 0 else "compositional", "source_group": "published",
                           "source_file": str(source), "source_sha256": pin(source)["sha256"],
                           "public_input_sha256": digest(canonical(payload)), "structure_bucket": "simple",
                           "exposure_status": "unknown"})
    for name, records in (("public.jsonl", public), ("refs.jsonl", refs)):
        (tmp_path / name).write_bytes(b"".join(canonical(r) + b"\n" for r in records))
    for i, row in enumerate(population):
        row["public_binding"] = {**pin(tmp_path / "public.jsonl"), "line_1based": i + 1, "record_sha256": digest(canonical(public[i]))}
        row["reference_binding"] = {**pin(tmp_path / "refs.jsonl"), "line_1based": i + 1, "record_sha256": digest(canonical(refs[i]))}
    (tmp_path / "population.jsonl").write_bytes(b"".join(canonical(r) + b"\n" for r in population))
    selection = [{**{k: r[k] for k in ("position_id", "benchmark", "task_id", "graph", "split", "source_group")},
                  "population_row_sha256": digest(canonical(r)), "stratum": {"graph": "g"}}
                 for r in population]
    (tmp_path / "selection.jsonl").write_bytes(b"".join(canonical(r) + b"\n" for r in selection))
    policy = save(tmp_path / "policy.json", {"benchmark": benchmark, "mode": "full_release", "population_sha256": pin(tmp_path / "population.jsonl")["sha256"]})
    manifest = {"kind": "offline_preparation_selection", "benchmark": benchmark, "selection_mode": "full_release",
                "population_count": count, "selected_count": count, "source": pin(tmp_path / "population.jsonl"),
                "policy": policy, "selection_sha256": pin(tmp_path / "selection.jsonl")["sha256"]}
    return save(tmp_path / "manifest.json", manifest)


def refresh_manifest(pin_value):
    path = Path(pin_value["path"])
    manifest = json.loads(path.read_bytes())
    manifest["selection_sha256"] = pin(path.with_name("selection.jsonl"))["sha256"]
    return save(path, manifest)


def test_complete_full_release_preserves_duplicate_task_members(tmp_path):
    frozen = make_bundle(tmp_path, repeated_task=True)
    cohort = load_cohort(frozen)
    assert len(cohort.positions) == 2
    assert len({p["task_id"] for p in cohort.positions}) == 1
    assert len({p["execution_task_id"] for p in cohort.positions}) == 2
    assert select_positions(cohort, "M01-B01") == cohort.positions
    with pytest.raises(ValueError, match="another benchmark"):
        select_positions(cohort, "M01-B02")


@pytest.mark.parametrize("change", ["order", "duplicate", "omit", "source_hash", "wrong_task"])
def test_corrupt_selection_rejected(tmp_path, change):
    frozen = make_bundle(tmp_path)
    path = tmp_path / "selection.jsonl"
    rows = [json.loads(line) for line in path.read_bytes().splitlines()]
    if change == "order": rows.reverse()
    if change == "duplicate": rows[1] = rows[0]
    if change == "omit": rows.pop()
    if change == "source_hash": rows[0]["population_row_sha256"] = "0" * 64
    if change == "wrong_task": rows[0]["task_id"] = "another"
    path.write_bytes(b"".join(canonical(r) + b"\n" for r in rows))
    frozen = refresh_manifest(frozen)
    with pytest.raises(ValueError):
        load_cohort(frozen)


def test_reference_hash_checked_without_decoding_answers(tmp_path, monkeypatch):
    frozen = make_bundle(tmp_path)
    original_loads = json.loads
    def guarded(value, *args, **kwargs):
        if b"DO_NOT_READ_OR_SEND" in (value.encode() if isinstance(value, str) else value):
            raise AssertionError("Reference semantics were loaded")
        return original_loads(value, *args, **kwargs)
    monkeypatch.setattr(json, "loads", guarded)
    assert len(load_cohort(frozen).positions) == 2
    (tmp_path / "refs.jsonl").write_bytes(b"changed")
    with pytest.raises(ValueError, match="SHA mismatch"):
        load_cohort(frozen)


def test_public_record_identity_not_just_file_presence(tmp_path):
    frozen = make_bundle(tmp_path)
    (tmp_path / "public.jsonl").write_bytes(b"{}\n")
    with pytest.raises(ValueError, match="SHA mismatch"):
        load_cohort(frozen)
