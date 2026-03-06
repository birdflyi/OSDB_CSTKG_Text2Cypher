from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from loaders.real_query_loader import load_structured_file


RE_REPO_ENTITY_ID = re.compile(r"^R_(\d+)$")


def _config_path() -> Path:
    return Path(__file__).resolve().parents[1] / "config" / "entity_abbr_map.yaml"


def _load_abbr_map(path: str | Path | None = None) -> dict[str, str]:
    p = Path(path) if path else _config_path()
    payload: Any = load_structured_file(p) if p.exists() else {}
    if not isinstance(payload, dict):
        return {}
    raw = payload.get("entity_abbr_map", payload)
    if not isinstance(raw, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in raw.items():
        label = str(k).strip()
        abbr = str(v).strip()
        if label and abbr:
            out[label] = abbr
    return out


def build_repo_scope_prefixes(
    repo_entity_id: str,
    labels: list[str],
    abbr_map_path: str | Path | None = None,
) -> dict[str, Any]:
    """
    Build deterministic repo-scope base prefixes from repo entity id.
    Example:
      repo_entity_id=R_156018, labels=[PullRequest,Issue,Commit]
      -> PR_156018 / I_156018 / C_156018
    """
    repo_id: int | None = None
    m = RE_REPO_ENTITY_ID.match(str(repo_entity_id or "").strip())
    if m:
        repo_id = int(m.group(1))

    base_prefixes: dict[str, str] = {}
    if repo_id is not None:
        abbr_map = _load_abbr_map(abbr_map_path)
        for label in labels or []:
            lb = str(label).strip()
            if not lb:
                continue
            abbr = abbr_map.get(lb)
            if not abbr:
                continue
            base_prefixes[lb] = f"{abbr}_{repo_id}"

    return {
        "repo_id": repo_id,
        "base_prefixes": base_prefixes,
    }

