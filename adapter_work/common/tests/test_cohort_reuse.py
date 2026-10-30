from types import SimpleNamespace

import pytest

from adapter_work.request_pipeline import load_reuse_aliases
from adapter_work.cohort_pipeline import save
from engine.config import canonical, digest
from engine.errors import IntegrityError


def fixture(tmp_path):
    wire = b'{"model":"m","messages":[]}'
    request = tmp_path / "request.json"
    request.write_bytes(wire)
    terminal = save(tmp_path / "terminal.json", dict(request_id="r", budget_slot=1, status="ok", content="RETURN 1"))
    evidence = save(tmp_path / "source_evidence.json", dict(profile_sha256="p", graph_snapshot_sha256="g"))
    job = SimpleNamespace(item=SimpleNamespace(key="new", model="m", graph="g", benchmark="B", wire=wire),
                          cohort=dict(profile_sha256="p", public_input_sha256="u"))
    record = dict(request_id="r", slot=1, scope="old", logical_key="old-key",
                  request_path=str(request), request_sha256=digest(wire))
    ledger = SimpleNamespace(records={"r": record})
    compatibility = dict(logical_model="m", graph="g", profile_sha256="p", public_input_sha256="u", graph_snapshot_sha256="g")
    alias = dict(target_key="new", source_key="old-key", source_scope="old",
                 compatibility=compatibility, source_compatibility=compatibility, evidence=[evidence, terminal])
    return job, ledger, alias


def test_explicit_cross_scope_reuse_preserves_original_paid_history(tmp_path):
    job, ledger, alias = fixture(tmp_path)
    result = load_reuse_aliases(save(tmp_path / "aliases.json", [alias]), [job], ledger, {"B": {"g": "g"}})
    assert result["new"]["response"]["request_id"] == "r"
    assert len(result["new"]["history"]) == 1
    assert ledger.records["r"]["logical_key"] == "old-key"


@pytest.mark.parametrize("change", ["profile", "wire", "terminal"])
def test_reuse_rejects_changed_context_or_saved_response(tmp_path, change):
    job, ledger, alias = fixture(tmp_path)
    if change == "profile":
        job.cohort["profile_sha256"] = "different"
    elif change == "wire":
        job.item.wire = b'{"model":"m","messages":[1]}'
    else:
        (tmp_path / "terminal.json").write_bytes(b"{}")
    with pytest.raises(IntegrityError):
        load_reuse_aliases(save(tmp_path / "aliases.json", [alias]), [job], ledger, {"B": {"g": "g"}})


@pytest.mark.parametrize("different", [False, True])
def test_equivalent_public_serialization_requires_equal_canonical_content(tmp_path, different):
    job, ledger, alias = fixture(tmp_path)
    old = tmp_path / "old_public.json"
    old.write_bytes(b'{\n  "question": "Q"\n}\n')
    old_pin = dict(path=str(old), sha256=digest(old.read_bytes()))
    new_pin = save(tmp_path / "new_public.json", dict(question="changed" if different else "Q"))
    job.cohort["public_input_sha256"] = new_pin["sha256"]
    alias["compatibility"] = dict(alias["compatibility"], public_input_sha256=new_pin["sha256"])
    alias["source_compatibility"] = dict(alias["source_compatibility"], public_input_sha256=old_pin["sha256"])
    alias["public_equivalence"] = dict(source=old_pin, target=new_pin, canonical_sha256=digest(canonical(dict(question="Q"))))
    aliases = save(tmp_path / "aliases.json", [alias])
    if different:
        with pytest.raises(IntegrityError, match="semantically"):
            load_reuse_aliases(aliases, [job], ledger, {"B": {"g": "g"}})
    else:
        assert load_reuse_aliases(aliases, [job], ledger, {"B": {"g": "g"}})["new"]["response"]["status"] == "ok"


@pytest.mark.parametrize("benchmark,mode,old_metadata,new_metadata", [
    ("Text2GQL", "text2gql_context_wrapper_v1", {"format":"original_public_user_messages"}, {"format":"cohort_public_context_v1"}),
    ("ZOGRASCOPE", "zograscope_plain_wrapper_v1", {"format":"question_and_schema"}, {"input_variant":"plain"}),
])
@pytest.mark.parametrize("change", [None, "value", "extra_field", "wrong_wrapper"])
def test_only_two_named_metadata_migrations_preserve_input(tmp_path, benchmark, mode, old_metadata, new_metadata, change):
    job, ledger, alias = fixture(tmp_path)
    job.item.benchmark = benchmark
    old_value = dict(question="Q", **old_metadata)
    new_value = dict(question="Q", **new_metadata)
    if change == "value":
        new_value["question"] = "different"
    elif change == "extra_field":
        new_value["candidates"] = ["different"]
    elif change == "wrong_wrapper":
        new_value[next(iter(new_metadata))] = "unknown"
    old_pin = save(tmp_path / "old.json", old_value)
    new_pin = save(tmp_path / "new.json", new_value)
    job.cohort["public_input_sha256"] = new_pin["sha256"]
    alias["compatibility"] = dict(alias["compatibility"], public_input_sha256=new_pin["sha256"])
    alias["source_compatibility"] = dict(alias["source_compatibility"], public_input_sha256=old_pin["sha256"])
    alias["public_equivalence"] = dict(mode=mode,source=old_pin,target=new_pin,canonical_sha256=digest(canonical(dict(question="Q"))))
    aliases = save(tmp_path / "aliases.json", [alias])
    if change:
        with pytest.raises(IntegrityError):
            load_reuse_aliases(aliases,[job],ledger,{benchmark:{"g":"g"}})
    else:
        assert "new" in load_reuse_aliases(aliases,[job],ledger,{benchmark:{"g":"g"}})
