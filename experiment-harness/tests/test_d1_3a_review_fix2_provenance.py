from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[2]
D1_3A_DIR = ROOT / "experiment-harness" / "d1_3a"
GRAPH_ROOT = ROOT / "graph-migration"
sys.path.insert(0, str(D1_3A_DIR))

from artifact_safety import (  # noqa: E402
    ensure_output_paths_available,
)
from template_provenance import (  # noqa: E402
    resolved_template_bundle_sha256,
    template_dependency_closure,
)


def _script_default_artifact_version(relative_path: str) -> str:
    code = (
        "import importlib.util, pathlib, sys; "
        "root=pathlib.Path.cwd(); "
        f"script=root / {relative_path!r}; "
        "sys.path.insert(0, str(script.parent)); "
        "spec=importlib.util.spec_from_file_location('d1_3a_cli', script); "
        "module=importlib.util.module_from_spec(spec); "
        "spec.loader.exec_module(module); "
        "print(module.build_parser().parse_args([]).artifact_version)"
    )
    child_env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=ROOT,
        env=child_env,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def test_generator_and_evaluator_default_to_v2_namespace() -> None:
    assert _script_default_artifact_version(
        "experiment-harness/d1_3a/generate_v1_dev_regression.py"
    ) == "v2"
    assert _script_default_artifact_version(
        "experiment-harness/d1_3a/evaluate_v1_dev_regression.py"
    ) == "v2"


@pytest.mark.parametrize("version", ["v1", "v4", "v99"])
def test_existing_canonical_artifacts_are_immutable_for_any_version(
    tmp_path: Path, version: str
) -> None:
    canonical_dir = tmp_path / "results" / "d1_3a_v1_dev_regression"
    canonical_dir.mkdir(parents=True)
    target = canonical_dir / f"d1_3a_v1_dev_generation_traces_{version}.jsonl"
    original = b"pre-existing canonical development evidence\n"
    target.write_bytes(original)

    with pytest.raises(FileExistsError, match="append-only canonical"):
        ensure_output_paths_available(
            [target],
            artifact_version=version,
            allow_overwrite=True,
            canonical_evidence_dir=canonical_dir,
        )
    assert target.read_bytes() == original


def test_canonical_v5_can_be_written_once_but_not_overwritten(tmp_path: Path) -> None:
    canonical_dir = tmp_path / "results" / "d1_3a_v1_dev_regression"
    canonical_dir.mkdir(parents=True)
    target = canonical_dir / "d1_3a_v1_dev_generation_traces_v5.jsonl"
    ensure_output_paths_available(
        [target], artifact_version="v5", canonical_evidence_dir=canonical_dir
    )
    target.write_text("new development artifact\n", encoding="utf-8")
    assert target.read_text(encoding="utf-8") == "new development artifact\n"
    with pytest.raises(FileExistsError, match="append-only canonical"):
        ensure_output_paths_available(
            [target], artifact_version="v5", allow_overwrite=True,
            canonical_evidence_dir=canonical_dir,
        )


def test_development_override_remains_available_only_outside_canonical_dir(tmp_path: Path) -> None:
    canonical_dir = tmp_path / "results" / "d1_3a_v1_dev_regression"
    temporary = tmp_path / "synthetic" / "artifact.jsonl"
    temporary.parent.mkdir(parents=True)
    temporary.write_text("old synthetic artifact\n", encoding="utf-8")
    ensure_output_paths_available(
        [temporary], artifact_version="v99", allow_overwrite=True,
        canonical_evidence_dir=canonical_dir,
    )


def test_project_v5_template_dependency_closure_is_stable_and_complete() -> None:
    pack = ROOT / "data_real" / "pilot_queries" / "independent_template_pack_v5.yaml"
    records = template_dependency_closure(pack, ROOT)
    assert [item["path"] for item in records] == [
        "data_real/pilot_queries/independent_template_pack_v4.yaml",
        "data_real/pilot_queries/independent_template_pack_v5.yaml",
    ]
    assert records == template_dependency_closure(pack, ROOT)
    assert len(resolved_template_bundle_sha256(records)) == 64


def test_base_dependency_change_changes_bundle_hash_but_not_facade_hash(tmp_path: Path) -> None:
    (tmp_path / "base.yaml").write_text("templates: []\n", encoding="utf-8")
    (tmp_path / "middle.yaml").write_text("extends: base.yaml\n", encoding="utf-8")
    top = tmp_path / "top.yaml"
    top.write_text("extends: middle.yaml\n", encoding="utf-8")
    before = template_dependency_closure(top, tmp_path)
    facade_hash_before = next(item["sha256"] for item in before if item["path"] == "top.yaml")
    bundle_before = resolved_template_bundle_sha256(before)

    (tmp_path / "base.yaml").write_text("templates:\n  - template_id: changed\n", encoding="utf-8")
    after = template_dependency_closure(top, tmp_path)
    facade_hash_after = next(item["sha256"] for item in after if item["path"] == "top.yaml")
    assert [item["path"] for item in before] == ["base.yaml", "middle.yaml", "top.yaml"]
    assert facade_hash_after == facade_hash_before
    assert resolved_template_bundle_sha256(after) != bundle_before


def test_template_dependency_cycles_and_missing_files_fail_clearly(tmp_path: Path) -> None:
    (tmp_path / "a.yaml").write_text("extends: b.yaml\n", encoding="utf-8")
    (tmp_path / "b.yaml").write_text("extends: a.yaml\n", encoding="utf-8")
    with pytest.raises(ValueError, match="cycle"):
        template_dependency_closure(tmp_path / "a.yaml", tmp_path)

    with pytest.raises(FileNotFoundError, match="does not exist"):
        template_dependency_closure(tmp_path / "missing.yaml", tmp_path)


def test_template_provenance_rejects_list_valued_extends(tmp_path: Path) -> None:
    (tmp_path / "base_a.yaml").write_text("templates: []\n", encoding="utf-8")
    (tmp_path / "base_b.yaml").write_text("templates: []\n", encoding="utf-8")
    top = tmp_path / "top.yaml"
    top.write_text("extends:\n  - base_a.yaml\n  - base_b.yaml\n", encoding="utf-8")

    with pytest.raises(ValueError, match="MULTIPLE_TEMPLATE_BASES_NOT_SUPPORTED"):
        template_dependency_closure(top, tmp_path)


def test_template_provenance_and_runtime_loader_share_single_base_policy(tmp_path: Path) -> None:
    (tmp_path / "base.yaml").write_text("templates: []\n", encoding="utf-8")
    top = tmp_path / "top.yaml"
    top.write_text("extends: [base.yaml, base.yaml]\ntemplates: []\n", encoding="utf-8")

    with pytest.raises(ValueError, match="MULTIPLE_TEMPLATE_BASES_NOT_SUPPORTED"):
        template_dependency_closure(top, tmp_path)

    code = (
        "import sys; "
        f"sys.path.insert(0, {str(GRAPH_ROOT)!r}); "
        "from runners.independent_controlled_pipeline import load_independent_templates; "
        f"load_independent_templates({str(top)!r})"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "MULTIPLE_TEMPLATE_BASES_NOT_SUPPORTED" in result.stderr


def test_bundle_hash_uses_canonical_records_with_relative_paths() -> None:
    records = [
        {"path": "base.yaml", "sha256": hashlib.sha256(b"base").hexdigest()},
        {"path": "top.yaml", "sha256": hashlib.sha256(b"top").hexdigest()},
    ]
    expected_bytes = json.dumps(
        records, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    assert resolved_template_bundle_sha256(records) == hashlib.sha256(expected_bytes).hexdigest()
