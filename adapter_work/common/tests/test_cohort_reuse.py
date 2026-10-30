from types import SimpleNamespace

import pytest

from adapter_work.request_pipeline import load_reuse_aliases
from adapter_work.cohort_pipeline import save
from engine.config import digest
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
