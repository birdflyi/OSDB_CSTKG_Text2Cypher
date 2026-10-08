from __future__ import annotations

"""No-clobber helpers for versioned D1.3a development artifacts."""

from pathlib import Path
import re
from typing import Iterable


DEFAULT_ARTIFACT_VERSION = "v2"
CANONICAL_EVIDENCE_DIR = Path(__file__).resolve().parents[1] / "results" / "d1_3a_v1_dev_regression"


def validate_artifact_version(value: str) -> str:
    if not re.fullmatch(r"v[1-9][0-9]*", value):
        raise ValueError(f"invalid artifact version {value!r}; expected vN")
    return value


def ensure_output_paths_available(
    paths: Iterable[Path],
    *,
    artifact_version: str,
    allow_overwrite: bool = False,
    canonical_evidence_dir: Path = CANONICAL_EVIDENCE_DIR,
) -> None:
    """Keep canonical evidence append-only; allow opted-in temp reruns only."""
    version = validate_artifact_version(artifact_version)
    resolved_paths = [Path(path).resolve() for path in paths]
    canonical_root = canonical_evidence_dir.resolve()
    canonical_paths = []
    for path in resolved_paths:
        try:
            path.relative_to(canonical_root)
        except ValueError:
            continue
        canonical_paths.append(path)

    existing = [path for path in resolved_paths if path.exists()]
    if not existing:
        return
    existing_canonical = [path for path in existing if path in canonical_paths]
    if existing_canonical:
        raise FileExistsError(
            "refusing to overwrite append-only canonical D1.3a evidence: "
            + ", ".join(str(path) for path in existing_canonical)
        )
    if not allow_overwrite:
        raise FileExistsError(
            "refusing to overwrite existing development artifacts without "
            "--allow-overwrite-development-artifact: "
            + ", ".join(str(path) for path in existing)
        )
