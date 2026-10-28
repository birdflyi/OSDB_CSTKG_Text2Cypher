import io
import json

from rich.console import Console

from engine.config import canonical, digest
from engine.ids import component, logical_key
from engine.wall_data import SnapshotReader, estimate_remaining
from engine.wall_view import Selection, render


def fixture(tmp_path):
    root = tmp_path / "run"
    root.mkdir()

    def write(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(canonical(value))

    registry = tmp_path / "registry.json"
    write(
        registry,
        dict(
            endpoints=[
                dict(
                    name="e",
                    model="m",
                    url="http://localhost/api",
                    cap=32,
                    enabled=True,
                    key_ref="SECRET_NOT_FOR_WALL",
                )
            ]
        ),
    )
    admission = tmp_path / "admission.json"
    write(admission, dict(models=["m"]))
    manifest = tmp_path / "manifest.json"
    write(
        manifest, [dict(task_id=n, graph="g", benchmark="B", method="Direct") for n in ("a", "b")]
    )
    config = tmp_path / "config.json"
    write(
        config,
        dict(
            run_dir=str(root),
            global_cap=32,
            registry_path=str(registry),
            admission_path=str(admission),
            stages=[dict(name="direct", manifest_path=str(manifest))],
        ),
    )
    plan = tmp_path / "plan.json"
    write(
        plan,
        dict(
            mode="smoke",
            cells=[
                dict(name="direct", method="Direct", benchmark="B", pipeline_config=str(config)),
                dict(name="missing", method="Ours", benchmark="Other", missing=["缺作者资源"]),
                dict(name="shared", method="Direct+反馈", benchmark="B", shared_with="direct"),
            ],
        ),
    )
    write(root / "progress.json", dict(phase="运行中", current_block="direct"))
    write(root / "blocks/direct/started.json", dict(started_epoch=80))
    generation = root / "runs/direct/_generation"
    generation.mkdir(parents=True)
    write(
        generation / "progress.json",
        dict(
            updated_epoch=100,
            global_cap=32,
            network_inflight=1,
            network_by_endpoint={"e": 1},
            endpoints=[dict(name="e", effective_cap=16, inflight=2)],
        ),
    )
    key = lambda n: component("m") + "__" + logical_key("B", n, "Direct", "initial")
    (generation / "journal.jsonl").write_text(
        json.dumps(dict(kind="send_started", logical_key=key("a"), endpoint="e", t=90)) + "\n",
        encoding="utf-8",
    )
    graph = root / "runs/direct/g"
    graph.mkdir()
    (graph / "journal.jsonl").write_text(
        "\n".join(
            json.dumps(e)
            for e in [
                dict(kind="case_stage", logical_key=key("b"), stage="数据库处理中", t=95),
                dict(kind="path_final_saved", logical_key=key("b"), t=98),
                dict(kind="graph_scored", graph="g", t=99),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    write(graph / "scores.json", [dict(view="final", qid="b", score=dict(ex=0))])
    return plan, root


def test_snapshot_separates_actual_network_from_permits_and_cap(tmp_path):
    plan, root = fixture(tmp_path)
    before = {str(p): digest(p.read_bytes()) for p in tmp_path.rglob("*") if p.is_file()}
    value = SnapshotReader().snapshot(plan, root, now=101)
    assert len(value["cells"]) == 3
    assert value["network_inflight"] == 1
    endpoint = value["endpoints"][0]
    assert (
        endpoint["inflight"],
        endpoint["permits"],
        endpoint["effective_cap"],
        endpoint["cap"],
    ) == (1, 2, 16, 32)
    first = value["cells"][0]
    assert first["cases"][0]["state"] == "模型处理中"
    assert first["cases"][1]["state"] == "已完成" and first["cases"][1]["correct"] == 0
    assert first["completed"] == 1 and first["eta_seconds"] is None
    assert "SECRET_NOT_FOR_WALL" not in json.dumps(value)
    assert before == {str(p): digest(p.read_bytes()) for p in tmp_path.rglob("*") if p.is_file()}


def test_stale_active_counts_are_unknown_not_zero_or_old_values(tmp_path):
    plan, root = fixture(tmp_path)
    reader = SnapshotReader()
    value = reader.snapshot(plan, root, now=130)
    assert value["stale"] and value["network_inflight"] is None
    assert value["endpoints"][0]["inflight"] is None
    (root / "progress.json").write_bytes(canonical(dict(phase="完成")))
    ended = reader.snapshot(plan, root, now=131)
    assert ended["network_inflight"] == 0 and not ended["stale"]


def test_selection_stays_pinned_and_chinese_render_has_required_sections(tmp_path):
    plan, root = fixture(tmp_path)
    value = SnapshotReader().snapshot(plan, root, now=101)
    selected = Selection()
    selected.choose(value)
    selected.key("l", value)
    assert not selected.follow and selected.case == "b"
    value["progress"]["current_block"] = "missing"
    assert selected.choose(value)["name"] == "direct"
    console = Console(file=io.StringIO(), width=160, height=60, color_system=None)
    console.print(render(value, selected, 60, now=101))
    output = console.file.getvalue()
    for text in [
        "全部方法",
        "模型端点",
        "配置 / 生效上限",
        "当前任务",
        "预计剩余",
        "当前关注案例",
        "案例ID",
        "答案",
        "J/K",
    ]:
        assert text in output
    assert "SECRET_NOT_FOR_WALL" not in output


def test_eta_requires_recent_completed_work():
    assert estimate_remaining([], 10, 100) is None
    assert estimate_remaining([50, 50, 50], 10, 100) is None
    assert estimate_remaining([50, 60, 70], 6, 100) == 60
    assert estimate_remaining([50], 0, 100) == 0
