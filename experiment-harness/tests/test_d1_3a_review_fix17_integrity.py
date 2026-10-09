from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[2]
D1_3A_DIR = ROOT / "experiment-harness" / "d1_3a"
sys.path.insert(0, str(D1_3A_DIR))

from artifact_safety import ensure_output_paths_available  # noqa: E402


def _load_evaluator():
    path = D1_3A_DIR / "evaluate_v1_dev_regression.py"
    spec = importlib.util.spec_from_file_location("d1_3a_dev_evaluator_fix17", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _valid_rows(ids: list[str]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    traces = [
        {
            "heldout_id": item_id,
            "selected_template": None,
            "generated_ir": {},
        }
        for item_id in ids
    ]
    gold = [{"heldout_id": item_id, "expected_behavior": "ABSTAIN"} for item_id in ids]
    return traces, gold


def test_evaluate_accepts_equal_unique_45_id_inputs(monkeypatch: pytest.MonkeyPatch) -> None:
    evaluator = _load_evaluator()
    monkeypatch.setattr(
        evaluator,
        "_load_frozen_evaluator",
        lambda: SimpleNamespace(
            _classify_row=lambda gold, trace: {
                "heldout_id": gold["heldout_id"],
                "expected_behavior": gold["expected_behavior"],
                "classification": "CORRECT_ABSTENTION",
            }
        ),
    )
    ids = [f"H_{index:03d}" for index in range(45)]
    traces, gold = _valid_rows(ids)

    rows, summary = evaluator.evaluate(traces, gold)

    assert len(rows) == summary["N"] == 45
    assert [row["heldout_id"] for row in rows] == ids


@pytest.mark.parametrize(
    ("source", "mutate", "error_text"),
    [
        (
            "traces",
            lambda traces, gold: traces.append(dict(traces[0])),
            "development traces has duplicate heldout_id",
        ),
        (
            "gold",
            lambda traces, gold: gold.append(dict(gold[0])),
            "frozen gold rows has duplicate heldout_id",
        ),
        (
            "traces",
            lambda traces, gold: traces[0].update(heldout_id="  \t"),
            "development traces row 1 has invalid heldout_id",
        ),
        (
            "gold",
            lambda traces, gold: gold[0].update(heldout_id=""),
            "frozen gold rows row 1 has invalid heldout_id",
        ),
    ],
)
def test_evaluate_rejects_duplicate_or_blank_raw_ids_before_mapping(
    source: str, mutate, error_text: str
) -> None:
    evaluator = _load_evaluator()
    ids = [f"H_{index:03d}" for index in range(45)]
    traces, gold = _valid_rows(ids)
    mutate(traces, gold)
    with pytest.raises(ValueError, match=error_text):
        evaluator.evaluate(traces, gold)


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _valid_cli_artifacts(tmp_path: Path) -> dict[str, Path]:
    ids = [f"H_{index:03d}" for index in range(45)]
    traces, gold = _valid_rows(ids)
    for trace in traces:
        trace.update(
            {
                "post_repair_rendered_cypher": None,
                "post_repair_validation": {},
                "failure_stage": "template_selection_or_abstention",
                "repair": None,
            }
        )
    frozen = [
        {"heldout_id": item_id, "classification": "CORRECT_ABSTENTION"}
        for item_id in ids
    ]
    pre_fix = [
        {
            "heldout_id": item_id,
            "expected_behavior": "ABSTAIN_OR_PENDING",
            "classification": "CORRECT_ABSTENTION",
        }
        for item_id in ids
    ]
    paths = {name: tmp_path / f"{name}.jsonl" for name in ("traces", "gold", "frozen", "pre_fix")}
    for name, rows in (("traces", traces), ("gold", gold), ("frozen", frozen), ("pre_fix", pre_fix)):
        _write_jsonl(paths[name], rows)
    return paths


@pytest.mark.parametrize(
    ("artifact", "mutation", "error_text"),
    [
        ("pre_fix", "duplicate", "pre-fix rows has duplicate heldout_id"),
        ("pre_fix", "blank", "pre-fix rows row 1 has invalid heldout_id"),
        ("frozen", "duplicate", "frozen baseline rows has duplicate heldout_id"),
        ("frozen", "blank", "frozen baseline rows row 1 has invalid heldout_id"),
    ],
)
def test_main_rejects_invalid_comparison_ids_without_writing_evidence(
    tmp_path: Path, artifact: str, mutation: str, error_text: str
) -> None:
    paths = _valid_cli_artifacts(tmp_path)
    original_rows = [json.loads(line) for line in paths[artifact].read_text(encoding="utf-8").splitlines()]
    if mutation == "duplicate":
        original_rows.append(dict(original_rows[0]))
    else:
        original_rows[0]["heldout_id"] = "   "
    _write_jsonl(paths[artifact], original_rows)
    output_dir = tmp_path / "evaluation-output"
    env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}

    completed = subprocess.run(
        [
            sys.executable,
            str(D1_3A_DIR / "evaluate_v1_dev_regression.py"),
            "--traces", str(paths["traces"]),
            "--gold", str(paths["gold"]),
            "--frozen-rows", str(paths["frozen"]),
            "--pre-fix-rows", str(paths["pre_fix"]),
            "--output-dir", str(output_dir),
            "--artifact-version", "v19",
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
    )

    assert completed.returncode != 0
    assert error_text in (completed.stderr + completed.stdout)
    assert not output_dir.exists()


def _make_symlink_or_skip(link: Path, target: Path, *, is_directory: bool) -> None:
    try:
        os.symlink(target, link, target_is_directory=is_directory)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"platform does not permit creating this symlink: {exc}")


def test_protected_lexical_namespace_symlink_to_scratch_cannot_be_overwritten(
    tmp_path: Path,
) -> None:
    results_root = tmp_path / "results"
    scratch = tmp_path / "external-scratch"
    scratch.mkdir()
    protected_alias = results_root / "d1_3a_v1_dev_regression_fix17_v19"
    results_root.mkdir()
    _make_symlink_or_skip(protected_alias, scratch, is_directory=True)
    target = protected_alias / "artifact.json"
    target.write_text("preserve me", encoding="utf-8")

    with pytest.raises(FileExistsError, match="append-only canonical"):
        ensure_output_paths_available(
            [target],
            artifact_version="v19",
            allow_overwrite=True,
            results_root=results_root,
        )
    assert target.read_text(encoding="utf-8") == "preserve me"


def test_scratch_symlink_alias_into_protected_evidence_cannot_be_overwritten(
    tmp_path: Path,
) -> None:
    results_root = tmp_path / "results"
    protected_dir = results_root / "d1_3a_v1_dev_regression_fix17_v19"
    protected_dir.mkdir(parents=True)
    target = protected_dir / "artifact.json"
    target.write_text("preserve me", encoding="utf-8")
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    alias = scratch / "alias"
    _make_symlink_or_skip(alias, protected_dir, is_directory=True)

    with pytest.raises(FileExistsError, match="append-only canonical"):
        ensure_output_paths_available(
            [alias / target.name],
            artifact_version="v19",
            allow_overwrite=True,
            results_root=results_root,
        )
    assert target.read_text(encoding="utf-8") == "preserve me"


def test_existing_nonprotected_scratch_symlink_can_only_be_overwritten_with_flag(
    tmp_path: Path,
) -> None:
    results_root = tmp_path / "results"
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    target = scratch / "artifact.json"
    target.write_text("scratch", encoding="utf-8")
    alias = tmp_path / "scratch-alias"
    _make_symlink_or_skip(alias, target, is_directory=False)

    with pytest.raises(FileExistsError, match="without"):
        ensure_output_paths_available(
            [alias], artifact_version="v19", results_root=results_root
        )
    ensure_output_paths_available(
        [alias], artifact_version="v19", allow_overwrite=True, results_root=results_root
    )
