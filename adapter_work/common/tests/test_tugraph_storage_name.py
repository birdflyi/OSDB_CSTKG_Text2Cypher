"""Public Text2GQL graph IDs must not be used as invalid TuGraph storage names."""

import json
import re
from types import SimpleNamespace

import pytest

from adapter_work import tugraph_worker as worker


def test_storage_graph_name_is_valid_stable_and_keeps_valid_names():
    name = "Manufacturing_BOM(Bill_Of_Materials)"
    mapped = worker.graph_storage_name(name)
    assert re.fullmatch(r"[A-Za-z0-9_]+", mapped)
    assert mapped == worker.graph_storage_name(name)
    assert mapped != worker.graph_storage_name("Manufacturing_BOM_Bill_Of_Materials_")
    assert worker.graph_storage_name("Financial_Payment") == "Financial_Payment"
    with pytest.raises(worker.IntegrityError):
        worker.graph_storage_name("")


def test_import_uses_storage_name_and_retains_public_identity(tmp_path, monkeypatch):
    public = "Manufacturing_BOM(Bill_Of_Materials)"
    (tmp_path / "execution_owner.json").write_text(json.dumps({"hostname": worker.socket.gethostname()}))
    source = tmp_path / "source"
    source.mkdir()
    (source / "import_config.json").write_text('{"files": []}')
    calls = []

    def fake_import(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=1)

    monkeypatch.setattr(worker.subprocess, "run", fake_import)
    output = tmp_path / "owned"
    settings = dict(owner_root=str(tmp_path), output=str(output), data=str(source),
                    data_sha256={}, runtime=str(tmp_path / "runtime"), runtime_sha256={},
                    port=0, graph=public)
    with pytest.raises(RuntimeError, match="import failed"):
        worker.TuGraphWorker(settings)
    assert calls[0][calls[0].index("--graph") + 1] == worker.graph_storage_name(public)
    receipt = json.loads((output / "import_result.json").read_text())
    assert receipt["graph"] == public
    assert receipt["storage_graph"] == worker.graph_storage_name(public)


@pytest.mark.parametrize("method", ["direct", "ours", "correct-detail", "simple-correct", "native-set-count"])
def test_execution_routes_storage_graph_but_saves_public_case(tmp_path, monkeypatch, method):
    public = "Manufacturing_BOM(Bill_Of_Materials)"
    instance = worker.TuGraphWorker.__new__(worker.TuGraphWorker)
    instance.settings = {"graph": public}
    instance.storage_graph = worker.graph_storage_name(public)
    instance.folder = tmp_path
    instance.transport = object()
    seen = []

    class Journal:
        def __init__(self, *args, **kwargs):
            pass

        def execute(self, request_id, graph, query):
            seen.append(graph)
            return dict(status="EXECUTED", complete=True, rows=[[1]])

    def plan(*args, **kwargs):
        seen.append(kwargs["graph"])
        return dict(status="EXECUTED", rows=[[1]])

    monkeypatch.setattr(worker, "NativeQueryJournal", Journal)
    # This test checks physical graph routing, independently of dialect support.
    if method == "native-set-count":
        import tugraph_dialect
        monkeypatch.setattr(tugraph_dialect, "render_compiler_query", lambda *args: "RETURN 1")
    monkeypatch.setattr(worker, "execute_native_plan", plan)
    schema = dict(entities=[dict(label="Person", properties={"key": "str"},
                                 identity_property="key")], relations=[])
    call = dict(nodes=[dict(id="n", label="Person")], edges=[], where=[],
                returns=[dict(field="n.key", alias="id")])
    request_plan = dict(status="ANSWER", calls=[call, call],
                        combine=dict(op="intersection", key="id", output="count"))
    request = dict(identity=dict(benchmark="Text2GQL", graph=public, method=method),
                   cypher="RETURN 1", plan=request_plan, schema=schema)
    answer = instance.execute(request)
    assert seen == [instance.storage_graph]
    assert answer["answer"]["complete"]
    saved = list(tmp_path.glob("cases/*/input.json"))
    assert len(saved) == 1
    assert json.loads(saved[0].read_text())["identity"]["graph"] == public
