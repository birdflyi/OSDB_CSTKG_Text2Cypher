from engine import campaign
from engine.config import digest


def test_new_runtime_modules_are_frozen_without_a_second_module_list(tmp_path, monkeypatch):
    for name in ("runtime.py", "adapters/new.py", "tests/test_example.py"):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"original\n")
    monkeypatch.setattr(campaign, "__file__", str(tmp_path / "campaign.py"))
    before = campaign.code_manifest()
    assert set(before) == {"runtime.py", "adapters/new.py"}
    (tmp_path / "adapters/new.py").write_bytes(b"changed\n")
    after = campaign.code_manifest()
    assert after["runtime.py"] == before["runtime.py"]
    assert after["adapters/new.py"] == digest(b"changed\n") != before["adapters/new.py"]
