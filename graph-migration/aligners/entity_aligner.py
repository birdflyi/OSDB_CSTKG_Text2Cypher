from __future__ import annotations

import ast
import csv
import hashlib
import importlib.util
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


RE_REPO_CANONICAL = re.compile(r"^R_(\d+)$")
RE_ACTOR_CANONICAL = re.compile(r"^A_(\d+)$")
RE_HTTP_LINK = re.compile(r"^https?://", re.IGNORECASE)
RE_GITHUB_REPO_URL = re.compile(
    r"^https?://(?:www\.|redirect\.)?github(?:-redirect\.dependabot)?\.com/([A-Za-z0-9][-0-9a-zA-Z]*)/([A-Za-z0-9][-_0-9a-zA-Z\.]*)(?:[/?#].*)?$",
    re.IGNORECASE,
)


@dataclass
class ResolveResult:
    entity_id: str | None
    provenance: str
    api_called: bool
    error: str | None = None
    debug: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "entity_id": self.entity_id,
            "provenance": self.provenance,
            "api_called": self.api_called,
            "error": self.error,
        }
        if isinstance(self.debug, dict):
            payload["debug_api"] = self.debug
        return payload


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _fixtures_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "fixtures" / "real_pilot_redis"


def _default_clean_csv() -> Path:
    return _project_root() / "data_real" / "redis_redis_2023_aug_exidfix.csv"


def _repo_index_path() -> Path:
    return _fixtures_dir() / "repo_name_index.csv"


def _actor_index_path() -> Path:
    return _fixtures_dir() / "actor_login_index.csv"


def _index_build_report_path() -> Path:
    return _fixtures_dir() / "index_build_report.md"


def _load_github_tokens(token_conf_path: str | None = None) -> tuple[list[str], str]:
    if token_conf_path:
        try:
            conf_path = Path(token_conf_path).resolve()
            spec = importlib.util.spec_from_file_location("osdb_auth_conf", str(conf_path))
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                conf_tokens = getattr(mod, "GITHUB_TOKENS", None)
                if isinstance(conf_tokens, list):
                    vals = [str(t).strip() for t in conf_tokens if str(t).strip()]
                    return vals, "authConf.py"
        except Exception:
            pass

    try:
        import GH_CoRE.utils.request_api as request_api  # type: ignore
        tokens = getattr(request_api, "GITHUB_TOKENS", None)
        if isinstance(tokens, list):
            vals = [str(t).strip() for t in tokens if str(t).strip()]
            if vals:
                return vals, "GH_CoRE.utils.request_api default"
    except Exception:
        pass

    try:
        from etc.authConf import GITHUB_TOKENS as LOC_GITHUB_TOKENS  # type: ignore
        if isinstance(LOC_GITHUB_TOKENS, list):
            vals = [str(t).strip() for t in LOC_GITHUB_TOKENS if str(t).strip()]
            if vals:
                return vals, "etc.authConf import path"
    except Exception:
        pass

    return [], "unknown"


def _inject_request_api_tokens(token_conf_path: str | None = None) -> dict[str, Any]:
    tokens, token_source = _load_github_tokens(token_conf_path=token_conf_path)
    try:
        import GH_CoRE.utils.request_api as request_api  # type: ignore

        if tokens:
            request_api.GITHUB_TOKENS = tokens
            try:
                request_api.RequestGitHubAPI.token_pool = request_api.GitHubTokenPool(github_tokens=tokens)
                request_api.RequestGitHubAPI.token = request_api.RequestGitHubAPI.token_pool.github_tokens[0] if request_api.RequestGitHubAPI.token_pool.github_tokens else ""
                request_api.RequestGitHubAPI.headers["Authorization"] = f"token {request_api.RequestGitHubAPI.token}"
            except Exception:
                pass
    except Exception:
        pass
    return {
        "token_source": token_source,
        "token_count": len(tokens),
        "token_fingerprints": [hashlib.sha256(t.encode("utf-8")).hexdigest()[:6] for t in tokens[:3]],
    }


def probe_github_api(api_timeout_sec: int = 3, token_conf_path: str | None = None) -> dict[str, Any]:
    """
    Lightweight reachability probe for GitHub API using GH_CoRE request_api token source.
    Never returns raw tokens/headers.
    """
    timeout_s = max(int(api_timeout_sec), 1)
    script = r"""
import hashlib, importlib.util, json, os, sys, time
import requests
import GH_CoRE.utils.request_api as request_api

timeout_sec = int(sys.argv[1]) if len(sys.argv) > 1 else 3
token_conf = sys.argv[2] if len(sys.argv) > 2 else ""

def load_tokens():
    token_source = "GH_CoRE.utils.request_api default"
    tokens = None
    if token_conf:
        try:
            conf_path = os.path.abspath(token_conf)
            spec = importlib.util.spec_from_file_location("osdb_auth_conf_probe", conf_path)
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                conf_tokens = getattr(mod, "GITHUB_TOKENS", None)
                if isinstance(conf_tokens, list):
                    tokens = [str(t).strip() for t in conf_tokens if str(t).strip()]
                    token_source = "authConf.py"
        except Exception:
            pass
    if tokens is None:
        try:
            from etc.authConf import GITHUB_TOKENS as LOC_GITHUB_TOKENS
            if isinstance(LOC_GITHUB_TOKENS, list):
                tokens = [str(t).strip() for t in LOC_GITHUB_TOKENS if str(t).strip()]
                token_source = "etc.authConf import path"
        except Exception:
            tokens = None
    if tokens is None:
        tokens = [str(t).strip() for t in (getattr(request_api, "GITHUB_TOKENS", []) or []) if str(t).strip()]
    return tokens, token_source

tokens, token_source = load_tokens()
request_api.GITHUB_TOKENS = tokens

diag = {
    "reachable": False,
    "last_error_type": None,
    "last_http_status": None,
    "elapsed_ms": None,
    "token_source": token_source,
    "token_count": len(tokens),
    "token_fingerprints": [hashlib.sha256(t.encode("utf-8")).hexdigest()[:6] for t in tokens[:3]],
}

token = tokens[0] if tokens else ""
headers = {
    "Accept": "application/vnd.github.v3+json",
    "Authorization": f"token {token}" if token else "",
    "User-Agent": "OSDB-Graph-Migration-Tool/Probe",
}

def classify_http(status):
    if status in (401,):
        return "api_unauthorized"
    if status in (403, 429):
        return "api_forbidden_or_rate_limited"
    return None

probe_urls = ["https://api.github.com/rate_limit", "https://api.github.com/users/torvalds"]
t0 = time.time()
for url in probe_urls:
    try:
        # Use GH_CoRE request_api RequestAPI class to perform request.
        req = request_api.RequestAPI(auth_type="token", token=token, headers=headers)
        resp = req.request_get(url)
        diag["last_http_status"] = getattr(resp, "status_code", None)
        code = classify_http(diag["last_http_status"])
        if code:
            diag["last_error_type"] = code
            continue
        if 200 <= int(diag["last_http_status"] or 0) < 300:
            diag["reachable"] = True
            diag["last_error_type"] = None
            break
        diag["last_error_type"] = "api_network_error"
    except requests.exceptions.Timeout:
        diag["last_error_type"] = "api_timeout"
    except requests.exceptions.SSLError:
        diag["last_error_type"] = "api_network_error"
    except requests.exceptions.ProxyError:
        diag["last_error_type"] = "api_network_error"
    except requests.exceptions.ConnectionError:
        diag["last_error_type"] = "api_network_error"
    except requests.exceptions.RequestException:
        diag["last_error_type"] = "api_network_error"
    except Exception:
        diag["last_error_type"] = "api_network_error"

diag["elapsed_ms"] = int((time.time() - t0) * 1000)
if diag["reachable"] and diag["last_error_type"] is None:
    pass
elif diag["last_error_type"] is None:
    diag["last_error_type"] = "api_network_error"
print(json.dumps(diag, ensure_ascii=False))
"""
    try:
        cp = subprocess.run(
            [sys.executable, "-c", script, str(timeout_s), token_conf_path or ""],
            capture_output=True,
            text=True,
            timeout=timeout_s + 5,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {
            "reachable": False,
            "last_error_type": "api_timeout",
            "last_http_status": None,
            "elapsed_ms": timeout_s * 1000,
            "token_source": "unknown",
            "token_count": 0,
            "token_fingerprints": [],
        }

    if cp.returncode != 0:
        return {
            "reachable": False,
            "last_error_type": "api_network_error",
            "last_http_status": None,
            "elapsed_ms": None,
            "token_source": "unknown",
            "token_count": 0,
            "token_fingerprints": [],
        }

    lines = [ln.strip() for ln in (cp.stdout or "").splitlines() if ln.strip()]
    for ln in reversed(lines):
        try:
            payload = json.loads(ln)
            if isinstance(payload, dict):
                return payload
        except Exception:
            continue
    return {
        "reachable": False,
        "last_error_type": "api_network_error",
        "last_http_status": None,
        "elapsed_ms": None,
        "token_source": "unknown",
        "token_count": 0,
        "token_fingerprints": [],
    }


def _s(v: Any) -> str:
    if v is None:
        return ""
    t = str(v).strip()
    return "" if t.lower() in {"", "nan", "none", "null"} else t


def _parse_kv_hint(text: str) -> tuple[str, str] | None:
    """
    Lightweight KV parser for explicit user hints.
    Supported separators: '=' or ':' (first occurrence).
    Returns (lower_key, stripped_value) or None.
    """
    t = _s(text)
    if not t:
        return None
    sep = ""
    if "=" in t:
        sep = "="
    elif ":" in t:
        sep = ":"
    if not sep:
        return None
    k, v = t.split(sep, 1)
    key = _s(k).lower()
    val = _s(v)
    if not key or not val:
        return None
    return key, val


def _parse_obj_dict(raw: Any) -> dict[str, Any]:
    txt = _s(raw)
    if not txt:
        return {}
    try:
        val = json.loads(txt)
        return val if isinstance(val, dict) else {}
    except Exception:
        pass
    try:
        val = ast.literal_eval(txt)
        return val if isinstance(val, dict) else {}
    except Exception:
        return {}


def _repo_id_from_canonical(entity_id: str) -> str:
    m = RE_REPO_CANONICAL.match(_s(entity_id))
    return m.group(1) if m else ""


def _actor_id_from_canonical(entity_id: str) -> str:
    m = RE_ACTOR_CANONICAL.match(_s(entity_id))
    return m.group(1) if m else ""


def _extract_repo_id_from_any(value: Any) -> str:
    s = _s(value)
    if not s:
        return ""
    if s.isdigit():
        return s
    return _repo_id_from_canonical(s)


def _extract_actor_id_from_any(value: Any) -> str:
    s = _s(value)
    if not s:
        return ""
    if s.isdigit():
        return s
    return _actor_id_from_canonical(s)


def _normalize_actor_login(text: str) -> str:
    s = _s(text)
    if s.startswith("@"):
        s = s[1:]
    return s.strip().lower()


def _normalize_repo_full_name(text: str) -> str:
    return _s(text).strip().lower()


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    except Exception:
        return pd.DataFrame()


def _write_index(path: Path, rows: list[dict[str, str]], key_cols: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        # Ensure file exists with header.
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=key_cols)
            writer.writeheader()
        return
    df = pd.DataFrame(rows)
    for c in key_cols:
        if c not in df.columns:
            df[c] = ""
    df = df[key_cols].copy()
    for c in key_cols:
        df[c] = df[c].map(_s)
    # Keep only rows with both fields.
    df = df[(df[key_cols[0]] != "") & (df[key_cols[1]] != "")]
    if df.empty:
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=key_cols)
            writer.writeheader()
        return
    df = df.drop_duplicates(subset=key_cols, keep="last")
    df.to_csv(path, index=False, encoding="utf-8")


def _upsert_index_row(path: Path, key_cols: list[str], row: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = _safe_read_csv(path)
    new_df = pd.DataFrame([{k: _s(row.get(k)) for k in key_cols}])
    if existing.empty:
        out = new_df
    else:
        for c in key_cols:
            if c not in existing.columns:
                existing[c] = ""
            existing[c] = existing[c].map(_s)
        out = pd.concat([existing[key_cols], new_df[key_cols]], ignore_index=True)
    out = out[(out[key_cols[0]] != "") & (out[key_cols[1]] != "")]
    out = out.drop_duplicates(subset=key_cols, keep="last")
    out.to_csv(path, index=False, encoding="utf-8")


def _write_index_build_report(report: dict[str, Any]) -> None:
    path = _index_build_report_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Index Build Report",
        "",
        f"- source_csv: `{report.get('source_csv', '')}`",
        f"- source_rows: `{report.get('source_rows', 0)}`",
        "",
        "## repo_name_index.csv",
        "",
        f"- rows_written: `{report.get('repo_rows_written', 0)}`",
        f"- distinct_repo_id: `{report.get('repo_distinct_ids', 0)}`",
        f"- duplicate_repo_full_name_count: `{report.get('repo_duplicate_names_count', 0)}`",
        f"- missing_repo_id_rate: `{report.get('repo_missing_id_rate', 0.0):.4f}`",
        f"- missing_repo_full_name_rate: `{report.get('repo_missing_name_rate', 0.0):.4f}`",
        "",
        "## actor_login_index.csv",
        "",
        f"- rows_written: `{report.get('actor_rows_written', 0)}`",
        f"- distinct_actor_id: `{report.get('actor_distinct_ids', 0)}`",
        f"- duplicate_actor_login_count: `{report.get('actor_duplicate_names_count', 0)}`",
        f"- missing_actor_id_rate: `{report.get('actor_missing_id_rate', 0.0):.4f}`",
        f"- missing_actor_login_rate: `{report.get('actor_missing_login_rate', 0.0):.4f}`",
        "",
        "Notes:",
        "- Rates are computed best-effort from parsed `tar_entity_objnt_prop_dict` records.",
        "- Missing values in source do not fail index build; unresolved items fall back to API stage at runtime.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _extract_from_d_record_repo(d_record: dict[str, Any] | None) -> tuple[str, str]:
    if not isinstance(d_record, dict):
        return "", ""
    rid = _extract_repo_id_from_any(d_record.get("repo_id"))
    rname = _normalize_repo_full_name(d_record.get("repo_name") or d_record.get("repo_full_name"))
    return rid, rname


def _extract_from_d_record_actor(d_record: dict[str, Any] | None) -> tuple[str, str]:
    if not isinstance(d_record, dict):
        return "", ""
    aid = _extract_actor_id_from_any(d_record.get("actor_id"))
    alogin = _normalize_actor_login(d_record.get("actor_login") or d_record.get("login"))
    return aid, alogin


def _extract_objentity_exid_and_type(ent_obj: Any) -> tuple[str, str]:
    if ent_obj is None:
        return "", ""
    ent_type = _s(getattr(ent_obj, "__type__", ""))
    exid = ""
    try:
        has_pk = bool(getattr(ent_obj, "__PK__", None))
        if has_pk:
            exid = _s(ent_obj.__repr__(brief=True))
    except Exception:
        exid = ""
    return exid, ent_type


def _find_pattern_matches(pattern_type: str, text: str) -> list[str]:
    text_s = _s(text)
    if not text_s:
        return []
    try:
        from GH_CoRE.data_dict_settings import re_ref_patterns  # type: ignore
        pats = re_ref_patterns.get(pattern_type, [])
    except Exception:
        pats = []
    if not isinstance(pats, list):
        return []

    matches: list[str] = []
    for p in pats:
        try:
            found = re.findall(p, text_s)
        except Exception:
            continue
        for m in found:
            # re.findall may return tuple for groups.
            if isinstance(m, tuple):
                m = next((str(x) for x in m if _s(x)), "")
            s = _s(m)
            if s and s not in matches:
                matches.append(s)
    return matches


def _safe_get_ent_obj_by_pattern(
    link_pattern_type: str,
    link_text: str,
    d_record: dict[str, Any] | None = None,
    timeout_sec: int = 2,
) -> tuple[str, str, str, dict[str, Any]]:
    """
    Returns (entity_id_exid, entity_type, actor_login/repo_name candidate, obj_dict).
    Runs in subprocess with short timeout to avoid blocking main flow.
    """
    script = r"""
import json, sys
from GH_CoRE.model.Entity_search import get_ent_obj_in_link_text

ptype = sys.argv[1]
ltxt = sys.argv[2]
drec_json = sys.argv[3] if len(sys.argv) > 3 else ""
try:
    drec = json.loads(drec_json) if drec_json else None
except Exception:
    drec = None

ent_obj = get_ent_obj_in_link_text(ptype, ltxt, drec)
if ent_obj is None:
    print(json.dumps({"entity_id": "", "entity_type": "", "name_hint": "", "obj_dict": {}}, ensure_ascii=False))
    raise SystemExit(0)

entity_id = ""
entity_type = str(getattr(ent_obj, "__type__", "") or "")
try:
    if getattr(ent_obj, "__PK__", None):
        entity_id = str(ent_obj.__repr__(brief=True))
except Exception:
    entity_id = ""
obj_dict = {}
try:
    d = ent_obj.get_dict()
    if isinstance(d, dict):
        obj_dict = d
except Exception:
    obj_dict = {}
name_hint = str(
    obj_dict.get("repo_name")
    or obj_dict.get("repo_full_name")
    or obj_dict.get("actor_login")
    or obj_dict.get("login")
    or ""
)
print(json.dumps({"entity_id": entity_id, "entity_type": entity_type, "name_hint": name_hint, "obj_dict": obj_dict}, ensure_ascii=False))
"""
    try:
        cp = subprocess.run(
            [
                sys.executable,
                "-c",
                script,
                str(link_pattern_type),
                str(link_text),
                json.dumps(d_record or {}, ensure_ascii=False),
            ],
            capture_output=True,
            text=True,
            timeout=max(int(timeout_sec), 1),
            check=False,
        )
    except Exception:
        return "", "", "", {}
    if cp.returncode != 0:
        return "", "", "", {}
    lines = [ln.strip() for ln in (cp.stdout or "").splitlines() if ln.strip()]
    for ln in reversed(lines):
        try:
            obj = json.loads(ln)
            if isinstance(obj, dict):
                return (
                    _s(obj.get("entity_id")),
                    _s(obj.get("entity_type")),
                    _s(obj.get("name_hint")),
                    obj.get("obj_dict") if isinstance(obj.get("obj_dict"), dict) else {},
                )
        except Exception:
            continue
    return "", "", "", {}


def build_local_indexes_if_missing(clean_csv_path: str | Path | None = None) -> dict[str, Any]:
    repo_index = _repo_index_path()
    actor_index = _actor_index_path()
    built = {"repo_index_built": False, "actor_index_built": False}
    if repo_index.exists() and actor_index.exists():
        return built

    source = Path(clean_csv_path) if clean_csv_path else _default_clean_csv()
    df = _safe_read_csv(source)
    if df.empty:
        # create empty files to keep deterministic behavior
        if not repo_index.exists():
            _write_index(repo_index, [], ["repo_id", "repo_full_name"])
            built["repo_index_built"] = True
        if not actor_index.exists():
            _write_index(actor_index, [], ["actor_id", "actor_login"])
            built["actor_index_built"] = True
        return built

    repo_rows: list[dict[str, str]] = []
    actor_rows: list[dict[str, str]] = []
    obj_total = 0
    repo_id_missing = 0
    repo_name_missing = 0
    actor_id_missing = 0
    actor_login_missing = 0
    for _, row in df.iterrows():
        src_id = _s(row.get("src_entity_id"))
        src_type = _s(row.get("src_entity_type"))
        tar_id = _s(row.get("tar_entity_id"))
        tar_type = _s(row.get("tar_entity_type"))
        obj = _parse_obj_dict(row.get("tar_entity_objnt_prop_dict"))
        if obj:
            obj_total += 1

        # from explicit object dict
        repo_id = _extract_repo_id_from_any(obj.get("repo_id"))
        repo_name = _normalize_repo_full_name(obj.get("repo_name") or obj.get("repo_full_name"))
        if obj:
            if not repo_id:
                repo_id_missing += 1
            if not repo_name:
                repo_name_missing += 1
        if repo_id and repo_name:
            repo_rows.append({"repo_id": repo_id, "repo_full_name": repo_name})

        actor_id = _extract_actor_id_from_any(obj.get("actor_id"))
        actor_login = _normalize_actor_login(obj.get("actor_login") or obj.get("login"))
        if obj:
            if not actor_id:
                actor_id_missing += 1
            if not actor_login:
                actor_login_missing += 1
        if actor_id and actor_login:
            actor_rows.append({"actor_id": actor_id, "actor_login": actor_login})

        # from entity IDs by type
        if src_type == "Repo":
            rid = _repo_id_from_canonical(src_id)
            if rid:
                repo_rows.append({"repo_id": rid, "repo_full_name": repo_name})
        if tar_type == "Repo":
            rid = _repo_id_from_canonical(tar_id)
            if rid:
                repo_rows.append({"repo_id": rid, "repo_full_name": repo_name})
        if src_type == "Actor":
            aid = _actor_id_from_canonical(src_id)
            if aid:
                actor_rows.append({"actor_id": aid, "actor_login": actor_login})
        if tar_type == "Actor":
            aid = _actor_id_from_canonical(tar_id)
            if aid:
                actor_rows.append({"actor_id": aid, "actor_login": actor_login})

    # Keep only rows with both cols non-empty.
    repo_rows = [r for r in repo_rows if _s(r.get("repo_id")) and _s(r.get("repo_full_name"))]
    actor_rows = [r for r in actor_rows if _s(r.get("actor_id")) and _s(r.get("actor_login"))]

    if not repo_index.exists():
        _write_index(repo_index, repo_rows, ["repo_id", "repo_full_name"])
        built["repo_index_built"] = True
    if not actor_index.exists():
        _write_index(actor_index, actor_rows, ["actor_id", "actor_login"])
        built["actor_index_built"] = True

    repo_df = _safe_read_csv(repo_index)
    actor_df = _safe_read_csv(actor_index)
    repo_dups = 0
    actor_dups = 0
    if not repo_df.empty and {"repo_full_name"} <= set(repo_df.columns):
        repo_dups = int(repo_df["repo_full_name"].astype(str).duplicated(keep=False).sum())
    if not actor_df.empty and {"actor_login"} <= set(actor_df.columns):
        actor_dups = int(actor_df["actor_login"].astype(str).duplicated(keep=False).sum())

    denom = max(obj_total, 1)
    _write_index_build_report(
        {
            "source_csv": str(source),
            "source_rows": int(len(df)),
            "repo_rows_written": int(len(repo_df)),
            "repo_distinct_ids": int(repo_df["repo_id"].astype(str).nunique()) if "repo_id" in repo_df.columns else 0,
            "repo_duplicate_names_count": repo_dups,
            "repo_missing_id_rate": float(repo_id_missing) / float(denom),
            "repo_missing_name_rate": float(repo_name_missing) / float(denom),
            "actor_rows_written": int(len(actor_df)),
            "actor_distinct_ids": int(actor_df["actor_id"].astype(str).nunique()) if "actor_id" in actor_df.columns else 0,
            "actor_duplicate_names_count": actor_dups,
            "actor_missing_id_rate": float(actor_id_missing) / float(denom),
            "actor_missing_login_rate": float(actor_login_missing) / float(denom),
        }
    )
    return built


def rebuild_local_indexes_safe(clean_csv_path: str | Path | None = None) -> dict[str, Any]:
    """
    Force rebuild both repo/actor indexes from source CSV.
    Safety guarantees:
    - If source CSV is missing/unreadable, returns structured error and leaves old indexes untouched.
    - Writes temp files first, then replaces target files.
    """
    source = Path(clean_csv_path) if clean_csv_path else _default_clean_csv()
    if not source.exists():
        return {
            "ok": False,
            "index_rebuilt": False,
            "error": f"source_csv_missing:{source}",
            "rebuilt_indexes": [],
            "rows_written": {},
        }
    df = _safe_read_csv(source)
    if df.empty:
        return {
            "ok": False,
            "index_rebuilt": False,
            "error": f"source_csv_unreadable_or_empty:{source}",
            "rebuilt_indexes": [],
            "rows_written": {},
        }

    repo_rows: list[dict[str, str]] = []
    actor_rows: list[dict[str, str]] = []
    obj_total = 0
    repo_id_missing = 0
    repo_name_missing = 0
    actor_id_missing = 0
    actor_login_missing = 0

    for _, row in df.iterrows():
        src_id = _s(row.get("src_entity_id"))
        src_type = _s(row.get("src_entity_type"))
        tar_id = _s(row.get("tar_entity_id"))
        tar_type = _s(row.get("tar_entity_type"))
        obj = _parse_obj_dict(row.get("tar_entity_objnt_prop_dict"))
        if obj:
            obj_total += 1

        repo_id = _extract_repo_id_from_any(obj.get("repo_id"))
        repo_name = _normalize_repo_full_name(obj.get("repo_name") or obj.get("repo_full_name"))
        if obj:
            if not repo_id:
                repo_id_missing += 1
            if not repo_name:
                repo_name_missing += 1
        if repo_id and repo_name:
            repo_rows.append({"repo_id": repo_id, "repo_full_name": repo_name})

        actor_id = _extract_actor_id_from_any(obj.get("actor_id"))
        actor_login = _normalize_actor_login(obj.get("actor_login") or obj.get("login"))
        if obj:
            if not actor_id:
                actor_id_missing += 1
            if not actor_login:
                actor_login_missing += 1
        if actor_id and actor_login:
            actor_rows.append({"actor_id": actor_id, "actor_login": actor_login})

        if src_type == "Repo":
            rid = _repo_id_from_canonical(src_id)
            if rid and repo_name:
                repo_rows.append({"repo_id": rid, "repo_full_name": repo_name})
        if tar_type == "Repo":
            rid = _repo_id_from_canonical(tar_id)
            if rid and repo_name:
                repo_rows.append({"repo_id": rid, "repo_full_name": repo_name})
        if src_type == "Actor":
            aid = _actor_id_from_canonical(src_id)
            if aid and actor_login:
                actor_rows.append({"actor_id": aid, "actor_login": actor_login})
        if tar_type == "Actor":
            aid = _actor_id_from_canonical(tar_id)
            if aid and actor_login:
                actor_rows.append({"actor_id": aid, "actor_login": actor_login})

    repo_df = pd.DataFrame(repo_rows, columns=["repo_id", "repo_full_name"])
    actor_df = pd.DataFrame(actor_rows, columns=["actor_id", "actor_login"])
    for c in ["repo_id", "repo_full_name"]:
        if c in repo_df.columns:
            repo_df[c] = repo_df[c].map(_s)
    for c in ["actor_id", "actor_login"]:
        if c in actor_df.columns:
            actor_df[c] = actor_df[c].map(_s)
    if not repo_df.empty:
        repo_df = repo_df[(repo_df["repo_id"] != "") & (repo_df["repo_full_name"] != "")]
        repo_df = repo_df.drop_duplicates(subset=["repo_id", "repo_full_name"], keep="last")
    if not actor_df.empty:
        actor_df = actor_df[(actor_df["actor_id"] != "") & (actor_df["actor_login"] != "")]
        actor_df = actor_df.drop_duplicates(subset=["actor_id", "actor_login"], keep="last")

    repo_index = _repo_index_path()
    actor_index = _actor_index_path()
    repo_index.parent.mkdir(parents=True, exist_ok=True)
    actor_index.parent.mkdir(parents=True, exist_ok=True)
    repo_tmp = repo_index.with_suffix(".csv.tmp")
    actor_tmp = actor_index.with_suffix(".csv.tmp")
    try:
        repo_df.to_csv(repo_tmp, index=False, encoding="utf-8")
        actor_df.to_csv(actor_tmp, index=False, encoding="utf-8")
        repo_tmp.replace(repo_index)
        actor_tmp.replace(actor_index)
    except Exception as exc:
        try:
            if repo_tmp.exists():
                repo_tmp.unlink()
            if actor_tmp.exists():
                actor_tmp.unlink()
        except Exception:
            pass
        return {
            "ok": False,
            "index_rebuilt": False,
            "error": f"rebuild_io_error:{type(exc).__name__}",
            "rebuilt_indexes": [],
            "rows_written": {},
        }

    repo_dups = int(repo_df["repo_full_name"].duplicated(keep=False).sum()) if not repo_df.empty else 0
    actor_dups = int(actor_df["actor_login"].duplicated(keep=False).sum()) if not actor_df.empty else 0
    denom = max(obj_total, 1)
    _write_index_build_report(
        {
            "source_csv": str(source),
            "source_rows": int(len(df)),
            "repo_rows_written": int(len(repo_df)),
            "repo_distinct_ids": int(repo_df["repo_id"].astype(str).nunique()) if not repo_df.empty else 0,
            "repo_duplicate_names_count": repo_dups,
            "repo_missing_id_rate": float(repo_id_missing) / float(denom),
            "repo_missing_name_rate": float(repo_name_missing) / float(denom),
            "actor_rows_written": int(len(actor_df)),
            "actor_distinct_ids": int(actor_df["actor_id"].astype(str).nunique()) if not actor_df.empty else 0,
            "actor_duplicate_names_count": actor_dups,
            "actor_missing_id_rate": float(actor_id_missing) / float(denom),
            "actor_missing_login_rate": float(actor_login_missing) / float(denom),
        }
    )
    return {
        "ok": True,
        "index_rebuilt": True,
        "error": None,
        "rebuilt_indexes": ["repo", "actor"],
        "rows_written": {"repo": int(len(repo_df)), "actor": int(len(actor_df))},
    }


def _try_repo_link_extract(
    input_str: str,
    d_record: dict[str, Any] | None = None,
    pattern_timeout_sec: int = 30,
) -> tuple[str, str]:
    """
    GH_CoRE re_ref_patterns-based extraction for Repo.
    Use matched pattern type ('Repo') directly with get_ent_obj_in_link_text.
    """
    text = _s(input_str)
    if not text:
        return "", ""
    matches = _find_pattern_matches("Repo", text)
    if not matches:
        return "", ""
    timeout_s = max(int(pattern_timeout_sec), 1)
    for m in matches:
        exid, _etype, name_hint, obj_dict = _safe_get_ent_obj_by_pattern(
            "Repo",
            m,
            d_record=d_record,
            timeout_sec=timeout_s,
        )
        repo_id = _extract_repo_id_from_any(obj_dict.get("repo_id")) or _repo_id_from_canonical(exid)
        repo_name = _normalize_repo_full_name(name_hint)
        if repo_id:
            return repo_id, repo_name
    return "", ""


def _try_actor_pattern_extract(
    input_str: str,
    d_record: dict[str, Any] | None = None,
    pattern_timeout_sec: int = 30,
) -> tuple[str, str]:
    """
    GH_CoRE re_ref_patterns-based extraction for Actor.
    Use matched pattern type ('Actor') directly with get_ent_obj_in_link_text.
    """
    text = _s(input_str)
    if not text:
        return "", ""
    matches = _find_pattern_matches("Actor", text)
    if not matches:
        return "", ""
    timeout_s = max(int(pattern_timeout_sec), 1)
    for m in matches:
        exid, _etype, name_hint, obj_dict = _safe_get_ent_obj_by_pattern(
            "Actor",
            m,
            d_record=d_record,
            timeout_sec=timeout_s,
        )
        actor_id = _extract_actor_id_from_any(obj_dict.get("actor_id")) or _actor_id_from_canonical(exid)
        actor_login = _normalize_actor_login(name_hint or m)
        if actor_id or actor_login:
            return actor_id, actor_login
    return "", ""


def _repo_lookup_local(repo_full_name: str) -> str:
    name = _normalize_repo_full_name(repo_full_name)
    if not name:
        return ""
    build_local_indexes_if_missing()
    df = _safe_read_csv(_repo_index_path())
    if df.empty or "repo_full_name" not in df.columns or "repo_id" not in df.columns:
        return ""
    df["repo_full_name"] = df["repo_full_name"].map(_normalize_repo_full_name)
    matched = df[df["repo_full_name"] == name]
    if matched.empty:
        return ""
    return _extract_repo_id_from_any(matched.iloc[-1]["repo_id"])


def _actor_lookup_local(actor_login: str) -> str:
    login = _normalize_actor_login(actor_login)
    if not login:
        return ""
    build_local_indexes_if_missing()
    df = _safe_read_csv(_actor_index_path())
    if df.empty or "actor_login" not in df.columns or "actor_id" not in df.columns:
        return ""
    df["actor_login"] = df["actor_login"].map(_normalize_actor_login)
    matched = df[df["actor_login"] == login]
    if matched.empty:
        return ""
    return _extract_actor_id_from_any(matched.iloc[-1]["actor_id"])


def _api_lookup_repo_id(
    repo_full_name: str,
    timeout_sec: int = 12,
    debug_api: bool = False,
    token_conf_path: str | None = None,
) -> ResolveResult:
    name = _normalize_repo_full_name(repo_full_name)
    if not name:
        return ResolveResult(entity_id=None, provenance="failed", api_called=False, error="invalid_repo_full_name")
    token_diag = _inject_request_api_tokens(token_conf_path=token_conf_path)
    try:
        api_rs = _call_gh_core_api_subprocess(
            kind="repo",
            value=name,
            timeout_s=timeout_sec,
            token_conf_path=token_conf_path,
            debug_api=debug_api,
        )
        repo_id = _extract_repo_id_from_any(api_rs.get("value"))
        if not repo_id:
            return ResolveResult(
                entity_id=None,
                provenance="failed",
                api_called=True,
                error=api_rs.get("error_code") or "api_network_error",
                debug=api_rs if debug_api else None,
            )
        _upsert_index_row(_repo_index_path(), ["repo_id", "repo_full_name"], {"repo_id": repo_id, "repo_full_name": name})
        return ResolveResult(
            entity_id=f"R_{repo_id}",
            provenance="api_fallback",
            api_called=True,
            debug=api_rs if debug_api else None,
        )
    except TimeoutError:
        return ResolveResult(
            entity_id=None,
            provenance="failed",
            api_called=True,
            error="api_timeout",
            debug={
                **token_diag,
                "last_http_status": None,
                "last_error_type": "timeout",
                "elapsed_ms": int(timeout_sec * 1000),
            } if debug_api else None,
        )
    except Exception as exc:
        return ResolveResult(
            entity_id=None,
            provenance="failed",
            api_called=True,
            error="api_network_error",
            debug={
                **token_diag,
                "last_http_status": None,
                "last_error_type": f"exception:{type(exc).__name__}",
                "elapsed_ms": None,
            } if debug_api else None,
        )


def _api_lookup_actor_id(
    actor_login: str,
    timeout_sec: int = 12,
    debug_api: bool = False,
    token_conf_path: str | None = None,
) -> ResolveResult:
    login = _normalize_actor_login(actor_login)
    if not login:
        return ResolveResult(entity_id=None, provenance="failed", api_called=False, error="invalid_actor_login")
    token_diag = _inject_request_api_tokens(token_conf_path=token_conf_path)
    try:
        api_rs = _call_gh_core_api_subprocess(
            kind="actor",
            value=login,
            timeout_s=timeout_sec,
            token_conf_path=token_conf_path,
            debug_api=debug_api,
        )
        actor_id = _extract_actor_id_from_any(api_rs.get("value"))
        if not actor_id:
            return ResolveResult(
                entity_id=None,
                provenance="failed",
                api_called=True,
                error=api_rs.get("error_code") or "api_network_error",
                debug=api_rs if debug_api else None,
            )
        _upsert_index_row(_actor_index_path(), ["actor_id", "actor_login"], {"actor_id": actor_id, "actor_login": login})
        return ResolveResult(
            entity_id=f"A_{actor_id}",
            provenance="api_fallback",
            api_called=True,
            debug=api_rs if debug_api else None,
        )
    except TimeoutError:
        return ResolveResult(
            entity_id=None,
            provenance="failed",
            api_called=True,
            error="api_timeout",
            debug={
                **token_diag,
                "last_http_status": None,
                "last_error_type": "timeout",
                "elapsed_ms": int(timeout_sec * 1000),
            } if debug_api else None,
        )
    except Exception as exc:
        return ResolveResult(
            entity_id=None,
            provenance="failed",
            api_called=True,
            error="api_network_error",
            debug={
                **token_diag,
                "last_http_status": None,
                "last_error_type": f"exception:{type(exc).__name__}",
                "elapsed_ms": None,
            } if debug_api else None,
        )


def _call_gh_core_api_subprocess(
    kind: str,
    value: str,
    timeout_s: int = 15,
    token_conf_path: str | None = None,
    debug_api: bool = False,
) -> dict[str, Any]:
    script = r"""
import hashlib, importlib.util, json, os, sys, time
import requests
from GH_CoRE.model import Attribute_getter
import GH_CoRE.utils.request_api as request_api

kind = sys.argv[1]
value = sys.argv[2]
token_conf = sys.argv[3] if len(sys.argv) > 3 else ""
debug_api = (sys.argv[4] == "1") if len(sys.argv) > 4 else False
timeout_sec = int(sys.argv[5]) if len(sys.argv) > 5 else 12

token_source = "GH_CoRE.utils.request_api default"
tokens = None
if token_conf:
    try:
        conf_path = os.path.abspath(token_conf)
        spec = importlib.util.spec_from_file_location("osdb_auth_conf", conf_path)
        if spec and spec.loader:
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            conf_tokens = getattr(mod, "GITHUB_TOKENS", None)
            if isinstance(conf_tokens, list):
                tokens = [str(t).strip() for t in conf_tokens if str(t).strip()]
                token_source = "authConf.py"
    except Exception:
        pass
if tokens is None:
    try:
        from etc.authConf import GITHUB_TOKENS as LOC_GITHUB_TOKENS
        if isinstance(LOC_GITHUB_TOKENS, list):
            tokens = [str(t).strip() for t in LOC_GITHUB_TOKENS if str(t).strip()]
            token_source = "etc.authConf import path"
    except Exception:
        tokens = None
if tokens is None:
    tokens = [str(t).strip() for t in (getattr(request_api, "GITHUB_TOKENS", []) or []) if str(t).strip()]

if isinstance(tokens, list):
    request_api.GITHUB_TOKENS = tokens
    try:
        request_api.RequestGitHubAPI.token_pool = request_api.GitHubTokenPool(github_tokens=tokens)
        request_api.RequestGitHubAPI.token = request_api.RequestGitHubAPI.token_pool.github_tokens[0] if request_api.RequestGitHubAPI.token_pool.github_tokens else ""
        request_api.RequestGitHubAPI.headers["Authorization"] = f"token {request_api.RequestGitHubAPI.token}"
    except Exception:
        pass

debug = {
    "token_source": token_source,
    "token_count": len(tokens),
    "token_fingerprints": [hashlib.sha256(t.encode("utf-8")).hexdigest()[:6] for t in tokens[:3]],
    "last_http_status": None,
    "last_error_type": None,
    "elapsed_ms": None,
}

_orig_get = requests.get
_orig_post = requests.post
def _wrap(fn, *args, **kwargs):
    if "timeout" not in kwargs:
        kwargs["timeout"] = timeout_sec
    try:
        resp = fn(*args, **kwargs)
        debug["last_http_status"] = getattr(resp, "status_code", None)
        return resp
    except requests.exceptions.Timeout:
        debug["last_error_type"] = "timeout"
        raise
    except requests.exceptions.SSLError:
        debug["last_error_type"] = "ssl_error"
        raise
    except requests.exceptions.ConnectionError:
        debug["last_error_type"] = "connection_error"
        raise
    except requests.exceptions.ProxyError:
        debug["last_error_type"] = "proxy_error"
        raise
    except requests.exceptions.RequestException as e:
        debug["last_error_type"] = f"request_exception:{type(e).__name__}"
        raise
requests.get = lambda *a, **k: _wrap(_orig_get, *a, **k)
requests.post = lambda *a, **k: _wrap(_orig_post, *a, **k)

start = time.time()
out = None
error_code = None
try:
    if kind == "repo":
        out = Attribute_getter.get_repo_id_by_repo_full_name(value)
    elif kind == "actor":
        out = Attribute_getter.get_actor_id_by_actor_login(value)
except Exception as e:
    low = str(e).lower()
    if "401" in low or "bad credential" in low or "unauthorized" in low:
        error_code = "api_unauthorized"
    elif "403" in low or "429" in low or "rate limit" in low:
        error_code = "api_forbidden_or_rate_limited"
    elif "timed out" in low or "timeout" in low:
        error_code = "api_timeout"
        if debug["last_error_type"] is None:
            debug["last_error_type"] = "timeout"
    else:
        error_code = "api_network_error"
        if debug["last_error_type"] is None:
            debug["last_error_type"] = f"exception:{type(e).__name__}"

if error_code is None:
    hs = debug.get("last_http_status")
    if hs == 401:
        error_code = "api_unauthorized"
    elif hs in (403, 429):
        error_code = "api_forbidden_or_rate_limited"
    elif out in (None, "", []):
        error_code = "api_network_error"

debug["elapsed_ms"] = int((time.time() - start) * 1000)
payload = {"value": out, "error_code": error_code}
if debug_api:
    payload.update(debug)
print(json.dumps(payload, ensure_ascii=False))
"""
    try:
        cp = subprocess.run(
            [
                sys.executable,
                "-c",
                script,
                kind,
                value,
                token_conf_path or "",
                "1" if debug_api else "0",
                str(max(int(timeout_s), 1)),
            ],
            capture_output=True,
            text=True,
            timeout=timeout_s,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError(str(exc)) from exc

    if cp.returncode != 0:
        raise RuntimeError((cp.stderr or "").strip() or f"subprocess_exit_{cp.returncode}")

    lines = [ln.strip() for ln in (cp.stdout or "").splitlines() if ln.strip()]
    for ln in reversed(lines):
        try:
            payload = json.loads(ln)
            if isinstance(payload, dict):
                return payload
        except Exception:
            continue
    raise RuntimeError("invalid_api_subprocess_output")


def resolve_repo_entity_id_with_meta(
    input_str: str,
    d_record: dict[str, Any] | None = None,
    api_timeout_sec: int = 12,
    debug_api: bool = False,
    token_conf_path: str | None = None,
) -> ResolveResult:
    text = _s(input_str)
    if not text:
        return ResolveResult(entity_id=None, provenance="failed", api_called=False, error="empty_input")

    # Lightweight KV hints:
    # - repo_id=NUMBER or repo_id:NUMBER
    # - repo_full_name=owner/repo or repo_full_name:owner/repo
    # - repo=owner/repo
    kv = _parse_kv_hint(text)
    if kv is not None:
        key, val = kv
        if key == "repo_id":
            if val.isdigit():
                return ResolveResult(entity_id=f"R_{val}", provenance="direct_kv_id", api_called=False)
            return ResolveResult(
                entity_id=None,
                provenance="failed",
                api_called=False,
                error="invalid_repo_id_kv",
            )
        if key in {"repo_full_name", "repo"}:
            text = val

    # A) direct canonical entity id
    if RE_REPO_CANONICAL.fullmatch(text):
        return ResolveResult(entity_id=text, provenance="direct_id", api_called=False)

    # B) structured hints from d_record (numeric repo_id supported only explicitly)
    rid, rname = _extract_from_d_record_repo(d_record)
    if not rid and text.isdigit():
        rid = text
    if rid:
        entity = f"R_{rid}"
        # best-effort write-through if name available
        if rname:
            _upsert_index_row(_repo_index_path(), ["repo_id", "repo_full_name"], {"repo_id": rid, "repo_full_name": rname})
        return ResolveResult(entity_id=entity, provenance="direct_id", api_called=False)

    # GH_CoRE re_ref_patterns + get_ent_obj_in_link_text parse.
    link_rid, link_rname = _try_repo_link_extract(
        text,
        d_record=d_record,
        pattern_timeout_sec=max(int(api_timeout_sec), 1),
    )
    if link_rid:
        if link_rname:
            _upsert_index_row(_repo_index_path(), ["repo_id", "repo_full_name"], {"repo_id": link_rid, "repo_full_name": link_rname})
        return ResolveResult(entity_id=f"R_{link_rid}", provenance="local_index", api_called=False)
    if link_rname:
        text = link_rname

    # URL -> owner/repo canonical string fast path.
    m_url = RE_GITHUB_REPO_URL.match(text)
    if m_url:
        text = f"{m_url.group(1)}/{m_url.group(2)}"

    # treat remaining text as repo_full_name (owner/repo), local first
    if "/" in text and not text.startswith("R_"):
        local_repo_id = _repo_lookup_local(text)
        if local_repo_id:
            return ResolveResult(entity_id=f"R_{local_repo_id}", provenance="local_index", api_called=False)
        return _api_lookup_repo_id(
            text,
            timeout_sec=max(int(api_timeout_sec), 1),
            debug_api=debug_api,
            token_conf_path=token_conf_path,
        )

    return ResolveResult(
        entity_id=None,
        provenance="failed",
        api_called=False,
        error="unsupported_repo_input_use_repo_full_name_or_structured_repo_id",
    )


def resolve_actor_entity_id_with_meta(
    input_str: str,
    d_record: dict[str, Any] | None = None,
    api_timeout_sec: int = 12,
    debug_api: bool = False,
    token_conf_path: str | None = None,
) -> ResolveResult:
    text = _s(input_str)
    if not text:
        return ResolveResult(entity_id=None, provenance="failed", api_called=False, error="empty_input")

    # Lightweight KV hints:
    # - actor_id=NUMBER or actor_id:NUMBER
    # - actor_login=LOGIN or actor_login:@LOGIN
    # Aliases: user / org / username
    kv = _parse_kv_hint(text)
    if kv is not None:
        key, val = kv
        if key == "actor_id":
            if val.isdigit():
                return ResolveResult(entity_id=f"A_{val}", provenance="direct_kv_id", api_called=False)
            return ResolveResult(
                entity_id=None,
                provenance="failed",
                api_called=False,
                error="invalid_actor_id_kv",
            )
        if key in {"actor_login", "user", "org", "username"}:
            text = val

    if RE_ACTOR_CANONICAL.fullmatch(text):
        return ResolveResult(entity_id=text, provenance="direct_id", api_called=False)

    aid, alogin = _extract_from_d_record_actor(d_record)
    if not aid and text.isdigit():
        aid = text
    if aid:
        entity = f"A_{aid}"
        if alogin:
            _upsert_index_row(_actor_index_path(), ["actor_id", "actor_login"], {"actor_id": aid, "actor_login": alogin})
        return ResolveResult(entity_id=entity, provenance="direct_id", api_called=False)

    # GH_CoRE re_ref_patterns + get_ent_obj_in_link_text parse.
    p_actor_id, p_actor_login = _try_actor_pattern_extract(
        text,
        d_record=d_record,
        pattern_timeout_sec=max(int(api_timeout_sec), 1),
    )
    if p_actor_id:
        if p_actor_login:
            _upsert_index_row(
                _actor_index_path(),
                ["actor_id", "actor_login"],
                {"actor_id": p_actor_id, "actor_login": p_actor_login},
            )
        return ResolveResult(entity_id=f"A_{p_actor_id}", provenance="local_index", api_called=False)
    if p_actor_login:
        # Continue local->API flow using parsed login.
        text = p_actor_login

    # For actor we support login string or @login.
    if text.startswith("@") or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]*", text):
        local_actor_id = _actor_lookup_local(text)
        if local_actor_id:
            return ResolveResult(entity_id=f"A_{local_actor_id}", provenance="local_index", api_called=False)
        return _api_lookup_actor_id(
            text,
            timeout_sec=max(int(api_timeout_sec), 1),
            debug_api=debug_api,
            token_conf_path=token_conf_path,
        )

    return ResolveResult(
        entity_id=None,
        provenance="failed",
        api_called=False,
        error="unsupported_actor_input_use_actor_login_or_structured_actor_id",
    )


def resolve_repo_entity_id(
    input_str: str,
    d_record: dict[str, Any] | None = None,
    api_timeout_sec: int = 12,
) -> dict[str, Any]:
    r = resolve_repo_entity_id_with_meta(
        input_str=input_str,
        d_record=d_record,
        api_timeout_sec=api_timeout_sec,
    )
    return r.to_dict()


def resolve_actor_entity_id(
    input_str: str,
    d_record: dict[str, Any] | None = None,
    api_timeout_sec: int = 12,
) -> dict[str, Any]:
    r = resolve_actor_entity_id_with_meta(
        input_str=input_str,
        d_record=d_record,
        api_timeout_sec=api_timeout_sec,
    )
    return r.to_dict()
