import hashlib
import json

import pytest

from adapter_work.cohort_sources import Population, digest, read_jsonl


def sample(pop, source, *, locator=None, query="RETURN 'secret_reference'", task_id="same-id"):
    pop.add(task_id=task_id, graph="g", split="test", source_group="Manual", source=source,
            locator=locator or {"json_index_0based": 0}, public={"question": "Original?", "graph": "g"},
            reference={"reference_query": query}, query=query)


def test_public_reference_separation_and_recomputable_bindings(tmp_path):
    source = tmp_path / "release.json"
    source.write_text('[]')
    pop = Population("example", tmp_path / "out")
    sample(pop, source)
    summary = pop.save(1)
    row = read_jsonl(pop.output / "population.jsonl")[0]
    public = (pop.output / "public_inputs.jsonl").read_text()
    assert "secret_reference" not in public
    assert "secret_reference" not in json.dumps(row)
    assert row["source_sha256"] == hashlib.sha256(source.read_bytes()).hexdigest()
    assert row["public_binding"]["record_sha256"] == digest(json.loads(public))
    assert row["reference_binding"]["record_sha256"] == digest(read_jsonl(pop.output / "references.jsonl")[0])
    assert summary["positions"] == 1
    assert row["exposure_status"] == "unknown"


def test_duplicate_task_ids_preserved_as_separate_source_positions(tmp_path):
    source = tmp_path / "release.json"
    source.write_text('[]')
    pop = Population("example", tmp_path / "out")
    sample(pop, source, locator={"index": 0})
    sample(pop, source, locator={"index": 1})
    summary = pop.save(2)
    assert summary["unique_task_ids"] == 1
    assert len({r["position_id"] for r in pop.rows}) == 2


def test_count_identity_and_sha_mismatches_fail_closed(tmp_path):
    source = tmp_path / "release.json"
    source.write_text('[]')
    pop = Population("example", tmp_path / "out")
    with pytest.raises(ValueError, match="SHA mismatch"):
        pop.pin(source, "0" * 64)
    sample(pop, source)
    with pytest.raises(ValueError, match="expected"):
        pop.save(2)
    sample(pop, source)
    with pytest.raises(ValueError, match="Duplicate source"):
        pop.save(2)


def test_existing_outputs_never_overwritten(tmp_path):
    source = tmp_path / "release.json"
    source.write_text('[]')
    pop = Population("example", tmp_path / "out")
    sample(pop, source)
    pop.save(1)
    with pytest.raises(FileExistsError):
        pop.save(1)
