from __future__ import annotations

"""No-clobber helpers for versioned D1.3a development artifacts."""

from pathlib import Path
import os
import re
from typing import Iterable


DEFAULT_ARTIFACT_VERSION = "v2"
RESULTS_ROOT = Path(__file__).resolve().parents[1] / "results"
VERSIONED_EVIDENCE_NAMESPACE = re.compile(r"^d1_3a_v1_dev_regression(?:_|$)")
# Kept as a compatibility alias for callers that imported the former single
# directory constant. Protection itself is now namespace-wide by default.
CANONICAL_EVIDENCE_DIR = RESULTS_ROOT / "d1_3a_v1_dev_regression"


def validate_artifact_version(value: str) -> str:
    if not re.fullmatch(r"v[1-9][0-9]*", value):
        raise ValueError(f"invalid artifact version {value!r}; expected vN")
    return value


def ensure_output_paths_available(
    paths: Iterable[Path],
    *,
    artifact_version: str,
    allow_overwrite: bool = False,
    canonical_evidence_dir: Path | None = None,
    results_root: Path = RESULTS_ROOT,
) -> None:
    """Keep every versioned D1.3a evidence namespace append-only.

    ``canonical_evidence_dir`` remains available for isolated tests and legacy
    callers. Production calls omit it and protect every immediate result
    directory named ``d1_3a_v1_dev_regression`` or with that prefix.
    """
    version = validate_artifact_version(artifact_version)
    resolved_paths = [Path(path).resolve() for path in paths]

    def is_protected(path: Path) -> bool:
        if canonical_evidence_dir is not None:
            try:
                Path(os.path.abspath(path)).relative_to(canonical_evidence_dir.resolve())
                return True
            except ValueError:
                pass
        lexical_path = Path(os.path.abspath(path))
        try:
            relative = lexical_path.relative_to(results_root.resolve())
        except ValueError:
            relative = None
        if relative and relative.parts and VERSIONED_EVIDENCE_NAMESPACE.match(relative.parts[0]):
            return True
        # Also protect a scratch-looking alias that resolves into the frozen
        # namespace; resolving only would miss a protected lexical path whose
        # directory itself is a symlink out of results/.
        try:
            resolved_relative = path.relative_to(results_root.resolve())
        except ValueError:
            return False
        return bool(resolved_relative.parts) and bool(
            VERSIONED_EVIDENCE_NAMESPACE.match(resolved_relative.parts[0])
        )

    existing = [path for path in resolved_paths if path.exists()]
    if not existing:
        return
    existing_canonical = [path for path in existing if is_protected(path)]
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
