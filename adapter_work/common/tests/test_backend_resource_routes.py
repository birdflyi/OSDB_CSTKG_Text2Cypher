"""No DB: lifecycle configuration, pinned scorer cache and reopen identity."""

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "adapter_work")]
from engine.config import canonical, digest
from engine.errors import IntegrityError
from native_cases import instance_settings
from offline_backends import (
    SelectionBackend,
    Text2GQLBackend,
    open_backend,
    instance_command,
)
from offline_pipeline import save


def result():
    return dict(status="EXECUTED", complete=True, rows=[[1]], columns=["n"])


def test_cb_scorer_is_lazily_cached_after_final_and_not_exposed_to_execution(
    tmp_path, monkeypatch
):
    import answer_scoring

    calls = []
    monkeypatch.setattr(
        answer_scoring,
        "load_official_comparator",
        lambda path: calls.append(path) or object(),
    )
    monkeypatch.setattr(answer_scoring, "score_executed_answer", lambda **kwargs: True)
    monkeypatch.setattr(answer_scoring, "read_reference_answer", lambda raw: [[1]])
    raw = (
        canonical(dict(qid="q", gold_cypher="RETURN 1", answer_json="fixture")) + b"\n"
    )
    reference = tmp_path / "references.jsonl"
    settings = dict(
        reference=dict(path=str(reference), sha256=digest(raw)),
        official_source="fixture",
    )
    backend = SelectionBackend("CypherBench", settings, None)
    assert backend._score_cache is None and not calls
    reference.write_bytes(raw)
    receipt = save(tmp_path / "final.json", result())
    assert backend.score(dict(task_id="q"), result(), receipt)["value"] is True
    # Once verified, the private snapshot is reused rather than reparsed per case.
    reference.unlink()
    assert backend.score(dict(task_id="q"), result(), receipt)["value"] is True
    assert calls == ["fixture"]


def test_text2gql_index_and_comparator_initialize_once_after_seal(
    tmp_path, monkeypatch
):
    import text2gql_reference_store_eval_20261027 as store_module
    import text2gql_score_contract_dev_20261027 as contract_module
    import audit_text2gql_score_boundaries_20261027 as subset_module

    constructed = []

    class Store:
        def __init__(self, *args, **kwargs):
            constructed.append("store")

        def cohort(self):
            return []

        def metadata(self, graph, task_id):
            return dict(query_sha256="fixture")

        def load(self, *args, **kwargs):
            return dict(answer=result())

    class Contract:
        def __init__(self, *args, **kwargs):
            constructed.append("contract")

        def score_one(self, *args):
            return dict(primary=True)

    monkeypatch.setattr(store_module, "SealedReferenceStore", Store)
    monkeypatch.setattr(contract_module, "FinalAnswerContract", Contract)
    monkeypatch.setattr(
        subset_module,
        "subset",
        lambda *args: (SimpleNamespace(compare_results=lambda *a: True), None),
    )
    comparator = tmp_path / "comparator.py"
    comparator.write_text("# pinned comparator")
    settings = dict(
        reference=dict(path="unused", sha256="fixture"),
        comparator=dict(path=str(comparator), sha256=digest(comparator.read_bytes())),
    )
    backend = Text2GQLBackend(
        SimpleNamespace(ready_value=dict(max_case_workers=3)), settings
    )
    assert not constructed and backend.max_case_workers == 3
    receipt = save(tmp_path / "final.json", result())
    for _ in range(2):
        assert (
            backend.score(dict(graph="g", task_id="q"), result(), receipt)["value"]
            is True
        )
    assert constructed == ["store", "contract"]


def test_osdb_graph_ports_and_case_executor_are_separate(tmp_path, monkeypatch):
    import osdb_owned_graph_factory_dev as module

    calls, executors = [], []

    class Factory:
        def __init__(self, *args, **kwargs):
            calls.append((args, kwargs))

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        @contextmanager
        def __call__(self, graph):
            def case_executor(directory):
                executors.append(directory)
                return dict(execute=lambda *args: result())

            yield dict(case_executor=case_executor)

    monkeypatch.setattr(module, "OwnedGraphFactory", Factory)
    binding = save(
        tmp_path / "binding.json",
        dict(
            inventory=dict(path="unused", sha256="fixture"),
            profiles={},
            neo4j_home="unused",
            http_port=18001,
            bolt_port=18002,
        ),
    )
    settings = dict(
        binding=binding,
        lock_path=str(tmp_path / "legacy-lock"),
        case_workers=3,
        graphs={
            "g": dict(
                http_port=18003, bolt_port=18004, lock_path=str(tmp_path / "g-lock")
            )
        },
    )
    source = dict(final=dict(prediction=dict(cypher="RETURN 1")))
    with open_backend("OSDB-STKG", "g", settings, tmp_path / "run") as backend:
        assert backend.max_case_workers == 3
        backend.execute(dict(method="direct"), source, tmp_path / "case1")
        backend.execute(dict(method="direct"), source, tmp_path / "case2")
    assert executors == [tmp_path / "case1/reads", tmp_path / "case2/reads"]
    assert calls[0][0][-1] == tmp_path / "g-lock"
    assert calls[0][1]["http"] == 18003 and calls[0][1]["bolt"] == 18004
    assert calls[0][1]["workers"] == 3


def test_reopen_command_and_instance_settings_use_unique_owned_subdirectories(tmp_path):
    settings = dict(output=str(tmp_path / "owned"), graph="g", port=18871)
    old = ["python", "worker.py", "settings.json"]
    assert instance_command(old, tmp_path / "one") == old
    command = old + ["--instance-id", "{instance_id}"]
    first = instance_command(command, tmp_path / "one")[-1]
    second = instance_command(command, tmp_path / "two")[-1]
    assert first != second
    for token in (first, second):
        selected = instance_settings(settings, token)
        Path(selected["output"]).mkdir(parents=True, exist_ok=False)
        assert Path(selected["output"]).parent == tmp_path / "owned"
    assert settings["output"] == str(tmp_path / "owned")
    assert instance_settings(settings, None) == settings
    with pytest.raises(FileExistsError):
        Path(instance_settings(settings, first)["output"]).mkdir(exist_ok=False)
    for invalid in ("../escape", "a/b", "a b", "", "x" * 65):
        with pytest.raises(IntegrityError):
            instance_settings(settings, invalid)
