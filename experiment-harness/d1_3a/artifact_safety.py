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
    def lexical_absolute(path: Path) -> Path:
        # abspath normalizes . and .. without dereferencing symlinks.
        return Path(os.path.abspath(os.fspath(path)))

    lexical_results_root = lexical_absolute(results_root)
    try:
        resolved_results_root = results_root.resolve(strict=False)
        lexical_canonical_dir = (
            lexical_absolute(canonical_evidence_dir)
            if canonical_evidence_dir is not None
            else None
        )
        resolved_canonical_dir = (
            canonical_evidence_dir.resolve(strict=False)
            if canonical_evidence_dir is not None
            else None
        )
        classified_paths = [
            (lexical_absolute(path), path.resolve(strict=False)) for path in paths
        ]
    except (OSError, RuntimeError, ValueError) as exc:
        raise ValueError("could not safely classify development artifact path") from exc

    def is_within(path: Path, parent: Path) -> bool:
        try:
            path.relative_to(parent)
            return True
        except ValueError:
            return False

    def is_protected(lexical_path: Path, resolved_path: Path) -> bool:
        for candidate in (lexical_path, resolved_path):
            if lexical_canonical_dir is not None and is_within(candidate, lexical_canonical_dir):
                return True
            if resolved_canonical_dir is not None and is_within(candidate, resolved_canonical_dir):
                return True

        lexical_relative = None
        try:
            lexical_relative = lexical_path.relative_to(lexical_results_root)
        except ValueError:
            pass
        if (
            lexical_relative
            and lexical_relative.parts
            and VERSIONED_EVIDENCE_NAMESPACE.match(lexical_relative.parts[0])
        ):
            return True

        try:
            resolved_relative = resolved_path.relative_to(resolved_results_root)
        except ValueError:
            return False
        return bool(resolved_relative.parts) and bool(
            VERSIONED_EVIDENCE_NAMESPACE.match(resolved_relative.parts[0])
        )

    existing = [
        (lexical_path, resolved_path)
        for lexical_path, resolved_path in classified_paths
        if os.path.lexists(lexical_path) or resolved_path.exists()
    ]
    if not existing:
        return
    existing_canonical = [
        (lexical_path, resolved_path)
        for lexical_path, resolved_path in existing
        if is_protected(lexical_path, resolved_path)
    ]
    if existing_canonical:
        raise FileExistsError(
            "refusing to overwrite append-only canonical D1.3a evidence: "
            + ", ".join(str(lexical_path) for lexical_path, _ in existing_canonical)
        )
    if not allow_overwrite:
        raise FileExistsError(
            "refusing to overwrite existing development artifacts without "
            "--allow-overwrite-development-artifact: "
            + ", ".join(str(lexical_path) for lexical_path, _ in existing)
        )
