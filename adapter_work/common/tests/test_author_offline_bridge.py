"""Scripted responses and injected DB doubles: never an author result."""

from contextlib import nullcontext
import json
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "adapter_work")]
from common import author, ri
from common.methods import AUTHOR_METHODS
from engine.config import canonical, digest
from engine.errors import IntegrityError
from offline_backends import NativeBackend, SelectionBackend
from offline_pipeline import run, run_group, save, validate_author_source

BENCHMARKS = ("CypherBench", "Text2GQL", "MindTheQuery", "ZOGRASCOPE", "OSDB-STKG")
POSITIONS = Path(r"D:\ch7\reports\t1_preparation_20261029\adapter_positions_v22\cells")


def answer():
    return dict(status="EXECUTED", complete=True, columns=["value"], rows=[[1]])


def fixture_source(method, benchmark, directory):
    path = POSITIONS / (method + "__" + benchmark + ".positions.json")
    frozen = json.loads(path.read_text(encoding="utf-8-sig"))["positions"][0]
    row = dict(
        frozen,
        method=method,
        benchmark=benchmark,
        cell_id=method + benchmark,
        public_input=dict(
            path=frozen["public_input_path"], sha256=frozen["public_input_sha256"]
        ),
        full_flow_five=True,
    )
    if method == "cypher-ri":
        spec = ri.prepare(path, benchmark, row["cell_id"], limit=1)[0]
        selection = '<json>{"labels":[],"relationships":[]}</json>'
        step = ri.next_request(spec, selection, generated_tokens=10)
        assert step.action == "request" and step.request.protocol == "completion_prefix"
        assert step.request.messages[0]["content"].startswith(
            spec.messages[0]["content"]
        )
        assert selection in step.request.messages[0]["content"]
        raw = "<answer>RETURN 1 AS value</answer>"
        prediction = ri.parse(raw, http_status=200)
    else:
        spec = author.prepare(path, benchmark, row["cell_id"], method, limit=1)[0]
        assert spec["method"] == method and spec["protocol"] == "chat"
        raw = "RETURN 1 AS value"
        prediction = author.parse(raw, http_status=200)
    result = dict(
        (k, row[k]) for k in ("method", "benchmark", "graph", "task_id", "position_id")
    )
    result.update(
        model="scripted-test-only",
        final=dict(
            status=prediction.status,
            prediction=prediction.as_dict(),
            raw_prediction=raw,
        ),
    )
    pin = save(directory / "source.json", result)
    return row, dict(result=result, receipt=pin)


class CypherTestBackend:
    """Use real consumer query routing; replace only the DB connection."""

    def __init__(self, benchmark):
        self.identities = []
        if benchmark in ("CypherBench", "OSDB-STKG"):

            def factory(row, directory):
                self.identities.append(row["method"])

                def execute(query, parameters):
                    assert query == "RETURN 1 AS value" and parameters == {}
                    return answer()

                return execute

            self.backend = SelectionBackend(benchmark, {}, factory)
        else:

            def request(payload, timeout):
                self.identities.append(payload["identity"]["method"])
                assert (
                    payload["cypher"] == "RETURN 1 AS value" and "plan" not in payload
                )
                return dict(
                    answer=answer(), remote_final_sha256=digest(canonical(answer()))
                )

            self.backend = NativeBackend(SimpleNamespace(request=request))

    def execute(self, *args):
        return self.backend.execute(*args)

    def score(self, row, saved, receipt):
        assert json.loads(Path(receipt["path"]).read_bytes()) == saved
        return dict(status="test_only", value=None, paper_result=False)


@pytest.mark.parametrize("method", AUTHOR_METHODS)
@pytest.mark.parametrize("benchmark", BENCHMARKS)
def test_ten_author_cells_prepare_parse_consume_without_identity_loss(
    method, benchmark, tmp_path
):
    row, source = fixture_source(method, benchmark, tmp_path)
    cell = dict(
        method=method, benchmark=benchmark, cell_id=row["cell_id"], positions=[row]
    )
    inventory = save(tmp_path / "inventory.json", dict(cells=[cell]))
    backend = CypherTestBackend(benchmark)
    config = dict(
        inventory=inventory,
        per_cell=1,
        source_finals=[source["receipt"]],
        execute_authorized=True,
        test_only=True,
        output=str(tmp_path / "run"),
        backends={benchmark: {}},
    )
    summary = run(config, backend_factory=lambda *args: nullcontext(backend))
    assert summary["test_only"] is True and summary["paper_result"] is False
    assert summary["planned"] == 1 and summary["model_calls"] == 0
    assert summary["results"][0]["method"] == method
    assert summary["results"][0]["status"] == "EXECUTED"
    assert backend.identities == [method]


def test_scripted_config_cannot_start_real_backend(tmp_path):
    with pytest.raises(IntegrityError, match="test backend"):
        run(dict(test_only=True))


def test_author_bad_query_does_not_stop_next_case(tmp_path):
    benchmark, method = "CypherBench", "cyquark"
    row, source = fixture_source(method, benchmark, tmp_path)
    other = dict(row, position_id="next")

    class Backend(CypherTestBackend):
        def execute(self, current, source, directory):
            if current["position_id"] == row["position_id"]:
                raise ValueError("ordinary invalid query")
            return super().execute(current, source, directory)

    results = run_group(
        [row, other],
        {(method, benchmark, r["position_id"]): source for r in (row, other)},
        Backend(benchmark),
        tmp_path / "run",
    )
    assert [r["status"] for r in results] == ["execution_error", "EXECUTED"]
    assert len(results) == 2


@pytest.mark.parametrize("method", AUTHOR_METHODS)
def test_author_assets_must_be_explicit_before_real_consumption(method, tmp_path):
    row, source = fixture_source(method, "CypherBench", tmp_path)
    asset = dict(
        method=method,
        model="scripted-test-only",
        benchmark=row["benchmark"],
        graph=row["graph"],
        checkpoint_sha256="a" * 64,
        tokenizer_sha256="b" * 64,
        prompt_profile_sha256="c" * 64,
        protocol="completion_prefix" if method == "cypher-ri" else "chat",
    )
    pin = save(tmp_path / "asset.json", asset)
    binding = dict(ready=False, model=asset["model"], asset_manifest=pin)

    def config(label):
        bindings = {
            method: (
                binding
                if method == "cypher-ri"
                else {"graphs": {row["benchmark"] + "/" + row["graph"]: binding}}
            )
        }
        return dict(
            author_request_config=save(
                tmp_path / (label + ".json"), dict(bindings=bindings)
            )
        )

    with pytest.raises(IntegrityError, match="resource_pending"):
        validate_author_source(row, source, config("pending"))
    binding["ready"] = True
    admitted = config("ready")
    source["result"]["model_attempts"] = [dict(model="authorized-wire-alias")]
    validate_author_source(row, source, admitted)
    source["result"]["model"] = "ordinary-deepseek"
    with pytest.raises(IntegrityError, match="another model"):
        validate_author_source(row, source, admitted)
    source["result"]["model"] = asset["model"]
    asset["tokenizer_sha256"] = None
    binding["asset_manifest"] = save(tmp_path / "missing-tokenizer.json", asset)
    with pytest.raises(IntegrityError, match="tokenizer"):
        validate_author_source(row, source, config("wrong"))


@pytest.mark.parametrize("method", AUTHOR_METHODS)
@pytest.mark.parametrize("worker_kind", ("native", "tugraph"))
def test_remote_workers_keep_author_identity_and_take_cypher_branch(
    method, worker_kind, tmp_path, monkeypatch
):
    identity = dict(
        method=method,
        benchmark="MindTheQuery" if worker_kind == "native" else "Text2GQL",
        graph="g",
        task_id="q",
        position_id="p",
    )
    request = dict(identity=identity, cypher="RETURN 1 AS value", parameters={})
    if worker_kind == "native":
        import native_worker as module

        class Capture:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def read(self, query, parameters, purpose):
                assert query == request["cypher"] and purpose == "method"
                return answer()

        monkeypatch.setattr(module, "NativeReadCapture", Capture)
        worker = module.NativeWorker.__new__(module.NativeWorker)
        worker.capture = None
        worker.driver = None
        worker.settings = dict(
            benchmark=identity["benchmark"], graph="g", snapshot_sha256="test-only"
        )
    else:
        import tugraph_worker as module

        class Journal:
            def __init__(self, *args, **kwargs):
                pass

            def execute(self, recorded_method, graph, query):
                assert recorded_method == method and query == request["cypher"]
                return answer()

        monkeypatch.setattr(module, "NativeQueryJournal", Journal)
        worker = module.TuGraphWorker.__new__(module.TuGraphWorker)
        worker.settings = dict(graph="g", port=18871)
        worker.token = "test-only-token"
        worker.storage_graph = "g"
    worker.folder = tmp_path
    result = worker.execute(request)
    assert result["answer"]["status"] == "EXECUTED"
    recorded = next((tmp_path / "cases").glob("*/input.json"))
    assert json.loads(recorded.read_bytes())["identity"]["method"] == method
