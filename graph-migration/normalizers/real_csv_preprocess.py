from __future__ import annotations

from dataclasses import dataclass
import ast
import importlib.util
import importlib
import json
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable
import sys

import pandas as pd


REQUIRED_AUG_COLS = [
    "src_entity_id_agg",
    "src_entity_type_agg",
    "tar_entity_id_agg",
    "tar_entity_type_agg",
    "tar_entity_type_fine_grained",
]
EXID_REPAIR_STATUS_COL = "tar_entity_id_repair_status"


@dataclass(frozen=True)
class GranularFns:
    granu_agg: Callable[..., Any]
    set_entity_type_fine_grained: Callable[..., Any]


def _load_module_from_path(module_path: Path, module_name: str) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, str(module_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot import module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _configure_github_tokens(granular_script_path: str | None, input_csv_path: str) -> None:
    """
    Ensure GH_CoRE request token pool uses data_scripts/etc/authConf.py.
    Target module: GH_CoRE.utils.request_api.
    """
    input_path = Path(input_csv_path).resolve()
    script_path = Path(granular_script_path).resolve() if granular_script_path else _resolve_default_granular_script(input_path).resolve()
    auth_path = script_path.parent / "etc" / "authConf.py"
    if not auth_path.exists():
        # Try workspace-level data_scripts/etc/authConf.py.
        auth_path = input_path.parent.parent / "data_scripts" / "etc" / "authConf.py"
    if not auth_path.exists():
        return

    tokens: list[str] | None = None
    try:
        auth_mod = _load_module_from_path(auth_path, "osdb_auth_conf")
        raw_tokens = getattr(auth_mod, "GITHUB_TOKENS", None)
        if isinstance(raw_tokens, (list, tuple)):
            tokens = [str(t) for t in raw_tokens if str(t).strip()]
    except Exception:
        tokens = None
    if not tokens:
        return

    try:
        mod = importlib.import_module("GH_CoRE.utils.request_api")
        if hasattr(mod, "GITHUB_TOKENS"):
            setattr(mod, "GITHUB_TOKENS", tokens)
    except Exception:
        pass


def _resolve_default_granular_script(input_csv_path: Path) -> Path:
    candidates = [
        input_csv_path.resolve().parent.parent / "data_scripts" / "granular_aggregation.py",
        Path(__file__).resolve().parents[3] / "data_scripts" / "granular_aggregation.py",
    ]
    for cand in candidates:
        if cand.exists():
            return cand
    return candidates[0]


def _load_granular_fns(granular_script_path: str | None, input_csv_path: str) -> GranularFns:
    script_path = (
        Path(granular_script_path)
        if granular_script_path
        else _resolve_default_granular_script(Path(input_csv_path))
    )
    if not script_path.exists():
        raise FileNotFoundError(
            f"Missing granular_aggregation.py at {script_path}. "
            "Please provide data_scripts/granular_aggregation.py."
        )
    added_paths: list[str] = []
    try:
        for p in (script_path.parent, script_path.parent.parent):
            p_str = str(p.resolve())
            if p_str not in sys.path:
                sys.path.insert(0, p_str)
                added_paths.append(p_str)
        try:
            module = _load_module_from_path(script_path, "granular_aggregation")
        except Exception:
            module = _load_granular_fns_ast_fallback(script_path)
    finally:
        for p in added_paths:
            if p in sys.path:
                sys.path.remove(p)
    if not hasattr(module, "granu_agg") or not hasattr(module, "set_entity_type_fine_grained"):
        raise AttributeError(
            "granular_aggregation.py must define granu_agg and set_entity_type_fine_grained."
        )
    return GranularFns(
        granu_agg=getattr(module, "granu_agg"),
        set_entity_type_fine_grained=getattr(module, "set_entity_type_fine_grained"),
    )


def _load_granular_fns_ast_fallback(script_path: Path) -> Any:
    source = script_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(script_path))
    fn_names = {
        "granu_agg",
        "set_entity_type_fine_grained",
        "parse_tar_entity_objnt_prop_dict",
    }
    body = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in fn_names]
    module_ast = ast.Module(body=body, type_ignores=[])
    code = compile(module_ast, filename=str(script_path), mode="exec")

    class _DummyAttributeGetter:
        pass

    def _infer_issue_type(repo_id: Any, issue_number: Any) -> str:
        # Prefer real gh-core when available in runtime environment.
        try:
            from GH_CoRE.model import Attribute_getter as _RealAttributeGetter  # type: ignore

            return _RealAttributeGetter.__get_issue_type(repo_id, issue_number)
        except Exception:
            pass
        # AST fallback is used when full source module import fails.
        # We cannot query GH API here, so keep a deterministic, local heuristic.
        _ = repo_id, issue_number
        return "Issue"

    setattr(_DummyAttributeGetter, "__get_issue_type", staticmethod(_infer_issue_type))

    class _DummyObjEntity:
        __PK__ = True

        def __init__(self, ent_type):
            self.ent_type = ent_type
            self._val = {}

        def set_val(self, obj):
            if isinstance(obj, dict):
                self._val = obj
            else:
                self._val = {}

        def __repr__(self, brief=True):
            _ = brief
            repo_id = self._val.get("repo_id")
            issue_number = self._val.get("issue_number")
            if repo_id and issue_number:
                if self.ent_type == "PullRequest":
                    return f"PR_{repo_id}#{issue_number}"
                if self.ent_type == "Issue":
                    return f"I_{repo_id}#{issue_number}"
            return f"{self.ent_type}_dummy"

    try:
        import numpy as np  # type: ignore
    except Exception:  # pragma: no cover - numpy is expected in env
        np = None

    try:
        from GH_CoRE.model import ObjEntity as _RealObjEntity  # type: ignore
    except Exception:
        _RealObjEntity = _DummyObjEntity

    globals_dict: dict[str, Any] = {
        "__builtins__": __builtins__,
        "pd": pd,
        "np": np,
        "json": json,
        "Attribute_getter": _DummyAttributeGetter,
        "ObjEntity": _RealObjEntity,
    }
    exec(code, globals_dict)

    # Post-patch fallback behavior for Issue/PR disambiguation without remote lookups.
    original_set_fine = globals_dict.get("set_entity_type_fine_grained")

    def _fallback_set_entity_type_fine_grained(row: pd.Series) -> pd.Series:
        out = row.copy()
        try:
            pattern = str(out.get("tar_entity_match_pattern_type", ""))
            src_type = str(out.get("src_entity_type", ""))
            tar_type = str(out.get("tar_entity_type", ""))
            if pattern == "Issue_PR" and tar_type == "Object":
                # Heuristic: PR-related source entities are much more likely to refer to PR.
                if src_type in {
                    "PullRequest",
                    "PullRequestReview",
                    "PullRequestReviewComment",
                }:
                    out["tar_entity_type"] = "PullRequest"
                elif src_type in {"Issue", "IssueComment"}:
                    out["tar_entity_type"] = "Issue"
        except Exception:
            pass
        result = original_set_fine(out) if callable(original_set_fine) else out
        out2 = result.copy() if isinstance(result, pd.Series) else out
        try:
            # Keep deterministic ID recovery in fallback mode.
            # If Issue_PR got disambiguated to Issue/PullRequest but tar_entity_id is still empty,
            # reconstruct a stable id from parsed object props.
            pattern = str(out2.get("tar_entity_match_pattern_type", ""))
            tar_id = out2.get("tar_entity_id")
            tar_type = str(out2.get("tar_entity_type", ""))
            if pattern == "Issue_PR" and (pd.isna(tar_id) or str(tar_id).strip() == ""):
                parse_fn = globals_dict.get("parse_tar_entity_objnt_prop_dict")
                parser = parse_fn if callable(parse_fn) else None
                props_raw = out2.get("tar_entity_objnt_prop_dict")
                props = parser(props_raw) if parser else None
                if isinstance(props, dict):
                    repo_id = props.get("repo_id")
                    issue_number = props.get("issue_number")
                    if repo_id and issue_number:
                        if tar_type == "PullRequest":
                            out2["tar_entity_id"] = f"PR_{repo_id}#{issue_number}"
                        elif tar_type == "Issue":
                            out2["tar_entity_id"] = f"I_{repo_id}#{issue_number}"
        except Exception:
            pass
        return out2

    globals_dict["set_entity_type_fine_grained"] = _fallback_set_entity_type_fine_grained
    return SimpleNamespace(**globals_dict)


def _resolve_default_exid_utils(input_csv_path: Path) -> Path:
    candidates = [
        input_csv_path.resolve().parent.parent / "data_scripts" / "exid_parse_utils.py",
        input_csv_path.resolve().parent.parent / "data_scripts" / "data_preprocess.py",
        Path(__file__).resolve().parents[3] / "data_scripts" / "exid_parse_utils.py",
        Path(__file__).resolve().parents[3] / "data_scripts" / "data_preprocess.py",
    ]
    for cand in candidates:
        if cand.exists():
            return cand
    return candidates[0]


def _safe_text(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, float) and pd.isna(val):
        return ""
    txt = str(val).strip()
    return "" if txt.lower() in {"nan", "none", "null"} else txt


def _parse_obj_props(raw: Any) -> dict[str, Any]:
    txt = _safe_text(raw)
    if not txt:
        return {}
    try:
        parsed = json.loads(txt)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        pass
    # Many rows are python-dict-like with single quotes.
    try:
        swapped = txt.replace('"', "$").replace("'", '"').replace("$", "'")
        parsed = json.loads(swapped)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        pass
    try:
        literal = ast.literal_eval(txt)
        return literal if isinstance(literal, dict) else {}
    except Exception:
        return {}


def _is_missing_like(value: Any) -> bool:
    return _safe_text(value) == ""


def _is_none_suffix_exid(value: Any) -> bool:
    text = _safe_text(value)
    if not text:
        return False
    if text.endswith("_None"):
        return True
    return bool(re.fullmatch(r"[A-Z]+_None", text))


def _needs_exid_repair(tar_entity_id: Any, tar_entity_type_fine_grained: Any) -> bool:
    fine = _safe_text(tar_entity_type_fine_grained)
    if not fine:
        return False
    return _is_missing_like(tar_entity_id) or _is_none_suffix_exid(tar_entity_id)


def _pick_first_nonempty(*values: Any) -> str:
    for v in values:
        txt = _safe_text(v)
        if txt:
            return txt
    return ""


def _to_int_like_str(value: Any) -> str:
    txt = _safe_text(value)
    if not txt:
        return ""
    if re.fullmatch(r"\d+(\.0+)?", txt):
        return str(int(float(txt)))
    return txt


def _extract_issue_number_from_text(text: str) -> str:
    m = re.search(r"#(\d+)", text or "")
    return m.group(1) if m else ""


def _extract_commit_sha_from_text(text: str) -> str:
    m = re.search(r"\b[0-9a-fA-F]{7,40}\b", text or "")
    return m.group(0) if m else ""


def _collect_exid_parts(row: pd.Series, repo_id: int | None) -> dict[str, str]:
    props = _parse_obj_props(row.get("tar_entity_objnt_prop_dict"))
    match_text = _safe_text(row.get("tar_entity_match_text"))
    tar_agg_id = _safe_text(row.get("tar_entity_id_agg"))
    tar_agg_type = _safe_text(row.get("tar_entity_type_agg"))

    out: dict[str, str] = {}
    out["repo_id"] = _to_int_like_str(
        _pick_first_nonempty(
            props.get("repo_id"),
            repo_id,
            row.get("repo_id"),
            re.sub(r"^R_", "", tar_agg_id) if tar_agg_type == "Repo" else "",
        )
    )
    out["actor_id"] = _to_int_like_str(
        _pick_first_nonempty(props.get("actor_id"), row.get("actor_id"))
    )
    out["issue_number"] = _to_int_like_str(
        _pick_first_nonempty(
            props.get("issue_number"),
            props.get("number"),
            row.get("issue_number"),
            _extract_issue_number_from_text(match_text),
        )
    )
    out["comment_id"] = _to_int_like_str(
        _pick_first_nonempty(
            props.get("comment_id"),
            props.get("issue_comment_id"),
            props.get("pull_review_comment_id"),
            props.get("pull_request_review_comment_id"),
            props.get("review_comment_id"),
            props.get("commit_comment_id"),
            row.get("comment_id"),
        )
    )
    out["review_id"] = _to_int_like_str(
        _pick_first_nonempty(
            props.get("review_id"),
            props.get("pull_review_id"),
            props.get("pull_request_review_id"),
            row.get("review_id"),
        )
    )
    out["commit_sha"] = _pick_first_nonempty(
        props.get("commit_sha"),
        props.get("sha"),
        row.get("commit_sha"),
        _extract_commit_sha_from_text(match_text),
    ).lower()
    out["branch_name"] = _pick_first_nonempty(
        props.get("branch_name"),
        row.get("branch_name"),
    )
    out["tag_name"] = _pick_first_nonempty(
        props.get("tag_name"),
        row.get("tag_name"),
    )
    out["release_id"] = _to_int_like_str(
        _pick_first_nonempty(
            props.get("release_id"),
            props.get("id"),
            row.get("release_id"),
        )
    )
    out["push_id"] = _to_int_like_str(
        _pick_first_nonempty(
            props.get("push_id"),
            row.get("push_id"),
        )
    )
    return out


def _build_exid_by_type(entity_type: str, parts: dict[str, str]) -> tuple[str, list[str]]:
    t = _safe_text(entity_type)

    if t == "Actor":
        missing = [k for k in ["actor_id"] if not parts.get(k)]
        return (f"A_{parts['actor_id']}" if not missing else "", missing)
    if t == "Repo":
        missing = [k for k in ["repo_id"] if not parts.get(k)]
        return (f"R_{parts['repo_id']}" if not missing else "", missing)
    if t == "Issue":
        missing = [k for k in ["repo_id", "issue_number"] if not parts.get(k)]
        return (f"I_{parts['repo_id']}#{parts['issue_number']}" if not missing else "", missing)
    if t == "PullRequest":
        missing = [k for k in ["repo_id", "issue_number"] if not parts.get(k)]
        return (f"PR_{parts['repo_id']}#{parts['issue_number']}" if not missing else "", missing)
    if t == "IssueComment":
        missing = [k for k in ["repo_id", "issue_number", "comment_id"] if not parts.get(k)]
        return (
            f"IC_{parts['repo_id']}#{parts['issue_number']}#{parts['comment_id']}" if not missing else "",
            missing,
        )
    if t == "PullRequestReview":
        missing = [k for k in ["repo_id", "issue_number", "review_id"] if not parts.get(k)]
        return (
            f"PRR_{parts['repo_id']}#{parts['issue_number']}#prr-{parts['review_id']}" if not missing else "",
            missing,
        )
    if t == "PullRequestReviewComment":
        missing = [k for k in ["repo_id", "issue_number", "comment_id"] if not parts.get(k)]
        return (
            f"PRRC_{parts['repo_id']}#{parts['issue_number']}#r{parts['comment_id']}" if not missing else "",
            missing,
        )
    if t == "Commit":
        missing = [k for k in ["repo_id", "commit_sha"] if not parts.get(k)]
        return (f"C_{parts['repo_id']}@{parts['commit_sha']}" if not missing else "", missing)
    if t == "CommitComment":
        missing = [k for k in ["repo_id", "commit_sha", "comment_id"] if not parts.get(k)]
        return (
            f"CC_{parts['repo_id']}@{parts['commit_sha']}#r{parts['comment_id']}" if not missing else "",
            missing,
        )
    if t == "Branch":
        missing = [k for k in ["repo_id", "branch_name"] if not parts.get(k)]
        return (f"B_{parts['repo_id']}:{parts['branch_name']}" if not missing else "", missing)
    if t == "Tag":
        missing = [k for k in ["repo_id", "tag_name"] if not parts.get(k)]
        return (f"T_{parts['repo_id']}-{parts['tag_name']}" if not missing else "", missing)
    if t == "Release":
        missing = [k for k in ["repo_id", "release_id"] if not parts.get(k)]
        return (f"RE_{parts['repo_id']}-{parts['release_id']}" if not missing else "", missing)
    if t == "Push":
        missing = [k for k in ["repo_id", "push_id"] if not parts.get(k)]
        return (f"P_{parts['repo_id']}.{parts['push_id']}" if not missing else "", missing)
    return ("", ["unsupported_type"])


def repair_exid_after_fine_grained(row: pd.Series, repo_id: int | None = None) -> pd.Series:
    out = row.copy()
    fine = _safe_text(out.get("tar_entity_type_fine_grained"))
    current_id = _safe_text(out.get("tar_entity_id"))
    if not fine:
        out[EXID_REPAIR_STATUS_COL] = "skipped_no_fine_grained"
        return out
    if not _needs_exid_repair(current_id, fine):
        out[EXID_REPAIR_STATUS_COL] = "skipped_not_needed"
        return out

    parts = _collect_exid_parts(out, repo_id=repo_id)
    rebuilt, missing = _build_exid_by_type(fine, parts)
    if rebuilt:
        out["tar_entity_id"] = rebuilt
        out[EXID_REPAIR_STATUS_COL] = "repaired"
    else:
        if missing == ["unsupported_type"]:
            out[EXID_REPAIR_STATUS_COL] = f"failed_unsupported_type:{fine}"
        else:
            out[EXID_REPAIR_STATUS_COL] = "failed_missing_fields:" + ",".join(sorted(set(missing)))
    return out


def _none_suffix_counts_by_fine(df: pd.DataFrame) -> dict[str, int]:
    if df.empty:
        return {}
    fine = df.get("tar_entity_type_fine_grained", pd.Series(dtype=str)).astype(str).map(_safe_text)
    bad = df.get("tar_entity_id", pd.Series(dtype=str)).astype(str).map(_is_none_suffix_exid)
    stats = (
        pd.DataFrame({"fine": fine, "bad": bad})
        .query("bad == True and fine != ''")
        .groupby("fine", dropna=False)
        .size()
        .sort_values(ascending=False)
    )
    return {str(k): int(v) for k, v in stats.items()}


def write_exid_repair_report(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    out_path: str | Path,
) -> None:
    before_stats = _none_suffix_counts_by_fine(before_df)
    after_stats = _none_suffix_counts_by_fine(after_df)
    status_counts: dict[str, int] = {}
    if EXID_REPAIR_STATUS_COL in after_df.columns:
        vc = after_df[EXID_REPAIR_STATUS_COL].astype(str).value_counts(dropna=False)
        status_counts = {str(k): int(v) for k, v in vc.items()}

    all_types = sorted(set(before_stats) | set(after_stats))
    lines = [
        "# Exid Repair Report",
        "",
        "## _None Counts By tar_entity_type_fine_grained",
        "",
        "| type | before_none_suffix | after_none_suffix | delta |",
        "|---|---:|---:|---:|",
    ]
    if all_types:
        for t in all_types:
            b = before_stats.get(t, 0)
            a = after_stats.get(t, 0)
            lines.append(f"| {t} | {b} | {a} | {b-a} |")
    else:
        lines.append("| (none) | 0 | 0 | 0 |")

    lines.extend(["", "## Repair Status Counts", ""])
    if status_counts:
        for k, v in status_counts.items():
            lines.append(f"- {k}: {v}")
    else:
        lines.append("- none")

    # Tail analysis (report only, no repair logic changes).
    def _status_prefix(status: str) -> str:
        s = _safe_text(status)
        if not s:
            return "EMPTY"
        s_low = s.lower()
        if s_low.startswith("failed_unsupported_type"):
            return "FAILED_UNSUPPORTED_TYPE"
        if s_low.startswith("failed_missing_fields"):
            return "FAILED_MISSING_FIELDS"
        return s.split(":", 1)[0].upper()

    def _status_payload(status: str) -> str:
        s = _safe_text(status)
        if ":" in s:
            return s.split(":", 1)[1]
        if "|" in s:
            return s.split("|", 1)[1]
        return ""

    def _looks_recoverable_missing_field(row: pd.Series, field_name: str) -> bool:
        props = _parse_obj_props(row.get("tar_entity_objnt_prop_dict"))
        match_text = _safe_text(row.get("tar_entity_match_text"))
        tar_agg_id = _safe_text(row.get("tar_entity_id_agg"))
        tar_agg_type = _safe_text(row.get("tar_entity_type_agg"))
        field = field_name.strip()
        if field == "repo_id":
            return bool(
                _safe_text(props.get("repo_id"))
                or (tar_agg_type == "Repo" and tar_agg_id.startswith("R_"))
                or _safe_text(row.get("repo_id"))
            )
        if field == "issue_number":
            return bool(
                _safe_text(props.get("issue_number"))
                or _extract_issue_number_from_text(match_text)
                or _safe_text(row.get("issue_number"))
            )
        if field == "comment_id":
            return bool(
                _safe_text(props.get("comment_id"))
                or _safe_text(props.get("issue_comment_id"))
                or _safe_text(props.get("pull_review_comment_id"))
                or _safe_text(props.get("pull_request_review_comment_id"))
                or _safe_text(props.get("review_comment_id"))
                or _safe_text(props.get("commit_comment_id"))
            )
        if field == "commit_sha":
            return bool(
                _safe_text(props.get("commit_sha"))
                or _safe_text(props.get("sha"))
                or _extract_commit_sha_from_text(match_text)
            )
        if field == "review_id":
            return bool(
                _safe_text(props.get("review_id"))
                or _safe_text(props.get("pull_review_id"))
                or _safe_text(props.get("pull_request_review_id"))
            )
        if field == "actor_id":
            return bool(_safe_text(props.get("actor_id")) or _safe_text(row.get("actor_id")))
        if field == "branch_name":
            return bool(_safe_text(props.get("branch_name")) or _safe_text(row.get("branch_name")))
        if field == "tag_name":
            return bool(_safe_text(props.get("tag_name")) or _safe_text(row.get("tag_name")))
        if field == "release_id":
            return bool(_safe_text(props.get("release_id")) or _safe_text(props.get("id")))
        if field == "push_id":
            return bool(_safe_text(props.get("push_id")) or _safe_text(row.get("push_id")))
        return False

    fail_df = after_df.copy()
    if EXID_REPAIR_STATUS_COL not in fail_df.columns:
        fail_df[EXID_REPAIR_STATUS_COL] = ""
    fail_df["__status_prefix"] = fail_df[EXID_REPAIR_STATUS_COL].astype(str).map(_status_prefix)
    fail_df["__fine"] = fail_df.get("tar_entity_type_fine_grained", pd.Series(dtype=str)).astype(str).map(_safe_text)
    fail_rows = fail_df[fail_df["__status_prefix"].isin({"FAILED_UNSUPPORTED_TYPE", "FAILED_MISSING_FIELDS"})].copy()

    lines.extend(["", "## Tail Analysis (failed_unsupported_type / failed_missing_fields)", ""])
    lines.extend(
        [
            "### Top 20 tar_entity_type_fine_grained by failure count",
            "",
            "| tar_entity_type_fine_grained | failure_count |",
            "|---|---:|",
        ]
    )
    if not fail_rows.empty:
        top20 = (
            fail_rows[fail_rows["__fine"] != ""]
            .groupby("__fine", dropna=False)
            .size()
            .sort_values(ascending=False)
            .head(20)
        )
        for t, c in top20.items():
            lines.append(f"| {t} | {int(c)} |")
    else:
        lines.append("| (none) | 0 |")

    lines.extend(
        [
            "",
            "### Failure status prefix breakdown",
            "",
            "| failure_prefix | count |",
            "|---|---:|",
        ]
    )
    if not fail_rows.empty:
        for k, v in fail_rows["__status_prefix"].value_counts().items():
            lines.append(f"| {k} | {int(v)} |")
    else:
        lines.append("| (none) | 0 |")

    lines.extend(
        [
            "",
            "### Top missing fields (FAILED_MISSING_FIELDS)",
            "",
            "| missing_field | count |",
            "|---|---:|",
        ]
    )
    missing_df = fail_rows[fail_rows["__status_prefix"] == "FAILED_MISSING_FIELDS"].copy()
    missing_counter: dict[str, int] = {}
    if not missing_df.empty:
        for s in missing_df[EXID_REPAIR_STATUS_COL].astype(str).tolist():
            payload = _status_payload(s).replace("|", ",")
            for part in payload.split(","):
                field = part.strip()
                if not field:
                    continue
                missing_counter[field] = missing_counter.get(field, 0) + 1
    if missing_counter:
        for f, c in sorted(missing_counter.items(), key=lambda kv: (-kv[1], kv[0]))[:20]:
            lines.append(f"| {f} | {c} |")
    else:
        lines.append("| (none) | 0 |")

    lines.extend(["", "## Repair Candidate Priorities", ""])
    lines.extend(
        [
            "| tar_entity_type_fine_grained | failures_total | missing_fields_ratio | recoverable_missing_ratio | priority | rationale |",
            "|---|---:|---:|---:|---|---|",
        ]
    )

    priority_rows: list[tuple[str, int, float, float, str, str]] = []
    if not fail_rows.empty:
        for fine, grp in fail_rows[fail_rows["__fine"] != ""].groupby("__fine", dropna=False):
            total = int(len(grp))
            miss_grp = grp[grp["__status_prefix"] == "FAILED_MISSING_FIELDS"]
            miss_ratio = (len(miss_grp) / total) if total > 0 else 0.0
            recoverable = 0
            if len(miss_grp) > 0:
                for _, r in miss_grp.iterrows():
                    payload = _status_payload(str(r.get(EXID_REPAIR_STATUS_COL, ""))).replace("|", ",")
                    fields = [x.strip() for x in payload.split(",") if x.strip()]
                    if fields and all(_looks_recoverable_missing_field(r, f) for f in fields):
                        recoverable += 1
            recoverable_ratio = (recoverable / len(miss_grp)) if len(miss_grp) > 0 else 0.0

            priority = "LOW"
            rationale = "Likely requires external resolution/API or unsupported type handling."
            if miss_ratio > 0.5 and recoverable_ratio >= 0.5:
                priority = "HIGH"
                rationale = "Most failures are missing fields and are likely recoverable from row-local evidence."
            elif miss_ratio > 0.5:
                priority = "MEDIUM"
                rationale = "Missing-fields dominate, but row-local recoverability is partial."

            fine_lower = str(fine).lower()
            if any(x in fine_lower for x in ["sha", "branch_tag_ghdir", "external", "other_service"]):
                if priority == "HIGH":
                    priority = "MEDIUM"
                elif priority == "MEDIUM":
                    priority = "LOW"
                rationale = "Ambiguous/external pattern; usually needs API or broader disambiguation policy."

            priority_rows.append((str(fine), total, miss_ratio, recoverable_ratio, priority, rationale))

    if priority_rows:
        priority_rank = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        for fine, total, miss_ratio, rec_ratio, priority, rationale in sorted(
            priority_rows,
            key=lambda x: (priority_rank.get(x[4], 9), -x[1], x[0]),
        )[:30]:
            lines.append(
                f"| {fine} | {total} | {miss_ratio:.2f} | {rec_ratio:.2f} | {priority} | {rationale} |"
            )
    else:
        lines.append("| (none) | 0 | 0.00 | 0.00 | LOW | No failed rows. |")

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _infer_entity_type_from_id(entity_id: str) -> str:
    prefix = entity_id.split("_", 1)[0] if "_" in entity_id else ""
    mapping = {
        "A": "Actor",
        "B": "Branch",
        "C": "Commit",
        "CC": "CommitComment",
        "G": "Gollum",
        "I": "Issue",
        "IC": "IssueComment",
        "PR": "PullRequest",
        "PRR": "PullRequestReview",
        "PRRC": "PullRequestReviewComment",
        "P": "Push",
        "RE": "Release",
        "R": "Repo",
        "T": "Tag",
        "OBJ": "Object",
    }
    return mapping.get(prefix, "")


def _derive_agg_fields_from_prefix(row: pd.Series, repo_id: int) -> dict[str, Any]:
    out: dict[str, Any] = {}
    src_type = _safe_text(row.get("src_entity_type"))
    src_id = _safe_text(row.get("src_entity_id"))
    if src_type == "Actor" and src_id:
        out["src_entity_id_agg"] = src_id
        out["src_entity_type_agg"] = "Actor"
    else:
        out["src_entity_id_agg"] = f"R_{repo_id}"
        out["src_entity_type_agg"] = "Repo"

    tar_id = _safe_text(row.get("tar_entity_id"))
    tar_type = _safe_text(row.get("tar_entity_type"))
    tar_obj = _parse_obj_props(row.get("tar_entity_objnt_prop_dict"))

    tar_entity_id_agg: Any = ""
    tar_entity_type_agg = "Object"
    if tar_id:
        id_type = _infer_entity_type_from_id(tar_id)
        if id_type == "Actor":
            tar_entity_id_agg = tar_id
            tar_entity_type_agg = "Actor"
        elif id_type:
            tar_entity_id_agg = f"R_{repo_id}"
            tar_entity_type_agg = "Repo"
    elif isinstance(tar_obj, dict):
        obj_repo_id = tar_obj.get("repo_id")
        obj_actor_id = tar_obj.get("actor_id")
        if obj_repo_id:
            tar_entity_id_agg = f"R_{obj_repo_id}"
            tar_entity_type_agg = "Repo"
        elif obj_actor_id:
            tar_entity_id_agg = f"A_{obj_actor_id}"
            tar_entity_type_agg = "Actor"
        elif tar_type and tar_type != "Object":
            tar_entity_id_agg = f"R_{repo_id}"
            tar_entity_type_agg = "Repo"

    out["tar_entity_id_agg"] = tar_entity_id_agg
    out["tar_entity_type_agg"] = tar_entity_type_agg
    return out


def _fallback_set_fine_grained_locally(row: pd.Series) -> pd.Series:
    out = row.copy()
    tar_type = _safe_text(out.get("tar_entity_type"))
    pattern = _safe_text(out.get("tar_entity_match_pattern_type"))
    src_type = _safe_text(out.get("src_entity_type"))
    tar_id = _safe_text(out.get("tar_entity_id"))
    props = _parse_obj_props(out.get("tar_entity_objnt_prop_dict"))

    ent_type = "GitHub_Service_External_Links"
    if tar_type and tar_type != "Object":
        ent_type = tar_type
    elif pattern in {"GitHub_Other_Service", "GitHub_Service_External_Links"}:
        ent_type = pattern
    elif pattern == "Issue_PR":
        if src_type in {"PullRequest", "PullRequestReview", "PullRequestReviewComment"}:
            ent_type = "PullRequest"
        elif src_type in {"Issue", "IssueComment"}:
            ent_type = "Issue"
        else:
            ent_type = "Issue"
        out["tar_entity_type"] = ent_type
        if not tar_id and isinstance(props, dict):
            repo_id = props.get("repo_id")
            issue_number = props.get("issue_number")
            if repo_id and issue_number:
                if ent_type == "PullRequest":
                    out["tar_entity_id"] = f"PR_{repo_id}#{issue_number}"
                else:
                    out["tar_entity_id"] = f"I_{repo_id}#{issue_number}"
    else:
        ent_type = pattern or "GitHub_Service_External_Links"
    out["tar_entity_type_fine_grained"] = ent_type
    return out


def _force_issue_pr_recheck(row: pd.Series) -> pd.Series:
    """
    Force fine-grained recheck for Issue_PR rows with empty tar_entity_id.
    This is required even if tar_entity_type already has a value.
    """
    out = row.copy()
    pattern = _safe_text(out.get("tar_entity_match_pattern_type"))
    tar_id = _safe_text(out.get("tar_entity_id"))
    if pattern != "Issue_PR" or tar_id:
        return out

    props = _parse_obj_props(out.get("tar_entity_objnt_prop_dict"))
    match_text = _safe_text(out.get("tar_entity_match_text")).lower()
    src_type = _safe_text(out.get("src_entity_type"))
    issue_type = ""

    # 1) URL cue has highest priority for deterministic disambiguation.
    if "/pull/" in match_text:
        issue_type = "PullRequest"
    elif "/issues/" in match_text:
        issue_type = "Issue"

    # 2) Try GH_CoRE API classification if cues are absent.
    if not issue_type and isinstance(props, dict):
        repo_id = props.get("repo_id")
        issue_number = props.get("issue_number")
        if repo_id and issue_number:
            try:
                from GH_CoRE.model import Attribute_getter  # type: ignore

                issue_type = str(Attribute_getter.__get_issue_type(repo_id, issue_number))
            except Exception:
                issue_type = ""

    # 3) Final deterministic local fallback by source context.
    if not issue_type:
        if src_type in {"PullRequest", "PullRequestReview", "PullRequestReviewComment"}:
            issue_type = "PullRequest"
        else:
            issue_type = "Issue"

    out["tar_entity_type"] = issue_type
    out["tar_entity_type_fine_grained"] = issue_type
    if isinstance(props, dict):
        repo_id = props.get("repo_id")
        issue_number = props.get("issue_number")
        if repo_id and issue_number:
            prefix = "PR" if issue_type == "PullRequest" else "I"
            out["tar_entity_id"] = f"{prefix}_{repo_id}#{issue_number}"
    return out


def _parse_exid_fast(
    df_ref: pd.DataFrame, repo_id: int, exid_utils_path: str | None, input_csv_path: str
) -> pd.DataFrame:
    exid_path = (
        Path(exid_utils_path)
        if exid_utils_path
        else _resolve_default_exid_utils(Path(input_csv_path))
    )
    if not exid_path.exists():
        # Allow deterministic local fallback from entity_id prefixes.
        exid_path = None
    module = None
    if exid_path:
        try:
            module = _load_module_from_path(exid_path, "exid_parser_utils")
        except Exception:
            module = None
    fn_candidates = [
        "derive_agg_fields",
        "parse_exid_agg",
        "parse_exid_rules_for_row",
        "fast_exid_agg",
        "derive_agg_from_exid",
    ]
    parser_fn = None
    if module is not None:
        for fn_name in fn_candidates:
            if hasattr(module, fn_name):
                parser_fn = getattr(module, fn_name)
                break

    def _apply_row(row: pd.Series) -> pd.Series:
        out = row.copy()
        parsed: Any = None
        if parser_fn is not None:
            try:
                parsed = parser_fn(row, repo_id=repo_id)
            except Exception:
                parsed = None
        if isinstance(parsed, pd.Series):
            out.update(parsed)
        elif isinstance(parsed, dict):
            for col in (
                "src_entity_id_agg",
                "src_entity_type_agg",
                "tar_entity_id_agg",
                "tar_entity_type_agg",
            ):
                if col in parsed:
                    out[col] = parsed[col]
        else:
            fallback = _derive_agg_fields_from_prefix(row, repo_id=repo_id)
            for k, v in fallback.items():
                out[k] = v
        return out

    return df_ref.apply(_apply_row, axis=1)


def _ensure_augmented_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in REQUIRED_AUG_COLS:
        if col not in out.columns:
            out[col] = pd.NA
    return out


def preprocess_osdb_csv(
    input_csv_path: str,
    repo_id: int,
    out_csv_path: str,
    mode: str = "full",
    granular_script_path: str | None = None,
    exid_utils_path: str | None = None,
    exid_repair_report_path: str | None = None,
) -> pd.DataFrame:
    normalized_mode = mode.strip().lower()
    if normalized_mode not in {"full", "fast_exid"}:
        raise ValueError("mode must be one of: full, fast_exid")
    _configure_github_tokens(
        granular_script_path=granular_script_path,
        input_csv_path=input_csv_path,
    )

    df = pd.read_csv(input_csv_path, dtype=str, keep_default_na=False)
    # Preserve original columns and values as-is.
    df_out = _ensure_augmented_columns(df)

    ref_mask = df_out.get("relation_type", pd.Series([], dtype=str)).astype(str).eq("Reference")
    if ref_mask.any():
        df_ref = df_out.loc[ref_mask].copy()

        if normalized_mode == "full":
            granular_fns = _load_granular_fns(granular_script_path, input_csv_path)

            def _apply_granu(row: pd.Series) -> pd.Series:
                out = row.copy()
                try:
                    result = granular_fns.granu_agg(row, repo_id=repo_id)
                    if isinstance(result, pd.Series):
                        out = result
                    elif isinstance(result, dict):
                        for k, v in result.items():
                            out[k] = v
                except Exception:
                    # Never crash single-row processing.
                    fallback = _derive_agg_fields_from_prefix(row, repo_id=repo_id)
                    for k, v in fallback.items():
                        out[k] = v
                try:
                    result2 = granular_fns.set_entity_type_fine_grained(out)
                    if isinstance(result2, pd.Series):
                        out = result2
                    elif isinstance(result2, dict):
                        for k, v in result2.items():
                            out[k] = v
                except Exception:
                    out = _fallback_set_fine_grained_locally(out)
                out = _force_issue_pr_recheck(out)
                return out

            df_ref_proc = df_ref.apply(_apply_granu, axis=1)
        else:
            df_ref_fast = _parse_exid_fast(
                df_ref=df_ref,
                repo_id=repo_id,
                exid_utils_path=exid_utils_path,
                input_csv_path=input_csv_path,
            )
            granular_fns = _load_granular_fns(granular_script_path, input_csv_path)

            def _apply_fine_grained(row: pd.Series) -> pd.Series:
                out = row.copy()
                try:
                    result = granular_fns.set_entity_type_fine_grained(out)
                    if isinstance(result, pd.Series):
                        out = result
                    elif isinstance(result, dict):
                        for k, v in result.items():
                            out[k] = v
                except Exception:
                    out = _fallback_set_fine_grained_locally(out)
                out = _force_issue_pr_recheck(out)
                return out

            df_ref_proc = df_ref_fast.apply(_apply_fine_grained, axis=1)

        before_repair = df_ref_proc.copy()
        df_ref_proc = df_ref_proc.apply(
            lambda r: repair_exid_after_fine_grained(r, repo_id=repo_id),
            axis=1,
        )

        df_ref_proc = _ensure_augmented_columns(df_ref_proc)
        df_out.loc[ref_mask, df_ref_proc.columns] = df_ref_proc
        if exid_repair_report_path:
            write_exid_repair_report(
                before_df=before_repair,
                after_df=df_ref_proc,
                out_path=exid_repair_report_path,
            )

    df_out = _ensure_augmented_columns(df_out)
    if EXID_REPAIR_STATUS_COL not in df_out.columns:
        df_out[EXID_REPAIR_STATUS_COL] = pd.NA
    Path(out_csv_path).parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_csv_path, index=False, encoding="utf-8")
    return df_out
