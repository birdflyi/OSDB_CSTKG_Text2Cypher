from __future__ import annotations

"""No-clobber helpers for versioned D1.3a development artifacts."""

from pathlib import Path
import re
from typing import Iterable


DEFAULT_ARTIFACT_VERSION = "v2"
PROTECTED_ARTIFACT_VERSIONS = {"v1", "v2", "v3"}


def validate_artifact_version(value: str) -> str:
    if not re.fullmatch(r"v[1-9][0-9]*", value):
        raise ValueError(f"invalid artifact version {value!r}; expected vN")
    return value


def ensure_output_paths_available(
    paths: Iterable[Path], *, artifact_version: str, allow_overwrite: bool = False
) -> None:
    """Refuse existing artifacts; frozen v1/v2/v3 are immutable even by override."""
    version = validate_artifact_version(artifact_version)
    existing = [path for path in paths if path.exists()]
    if not existing:
        return
    if version in PROTECTED_ARTIFACT_VERSIONS:
        raise FileExistsError(
            f"refusing to overwrite protected {version} development artifacts: "
            + ", ".join(str(path) for path in existing)
        )
    if not allow_overwrite:
        raise FileExistsError(
            "refusing to overwrite existing development artifacts without "
            "--allow-overwrite-development-artifact: "
            + ", ".join(str(path) for path in existing)
        )
