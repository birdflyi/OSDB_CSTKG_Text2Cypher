from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import importlib

from normalizers.eventaction_service import (
    build_service_verbs_payload,
    canonical_service_rel_name,
    load_event_trigger_triples_dict,
)


NATIVE_NODE_TYPES = {
    "Actor",
    "Repo",
    "Issue",
    "PullRequest",
    "Commit",
    "Branch",
    "Tag",
    "IssueComment",
    "PullRequestReview",
    "PullRequestReviewComment",
    "CommitComment",
    "Gollum",
    "Release",
    "Push",
}

REFERENCE_INTERNAL_PATTERNS = {
    "Repo",
    "Actor",
    "CommitComment",
    "Gollum",
    "Release",
    "GitHub_Files_FileChanges",
    "GitHub_GenSer_Other_Links",
    "Issue_PR",
    "SHA",
    "Branch_Tag_GHDir",
}

AMBIGUOUS_PATTERNS = {"Issue_PR", "SHA", "Branch_Tag_GHDir"}
URL_RE = re.compile(r"https?://[^\s\]\)\"'>,;]+", re.IGNORECASE)


def _s(val: Any) -> str:
    if val is None:
        return ""
    txt = str(val).strip()
    return "" if txt.lower() in {"nan", "none", "null"} else txt


def _parse_obj_dict(raw: Any) -> dict[str, Any]:
    text = _s(raw)
    if not text:
        return {}
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        pass
    try:
        swapped = text.replace('"', '$').replace("'", '"').replace('$', "'")
        obj = json.loads(swapped)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        pass
    return {}


def _extract_url_info(match_text: str) -> dict[str, Any]:
    text = _s(match_text)
    found = URL_RE.findall(text)
    if not found:
        return {
            "url_raw": "",
            "url_host": "",
            "url_domain_etld1": "",
            "heuristic_eTLD1": False,
            "url_is_github": False,
        }
    url_raw = found[0]
    host = ""
    domain = ""
    heuristic = False
    try:
        parsed = urlparse(url_raw)
        host = (parsed.hostname or "").lower()
        labels = [p for p in host.split(".") if p]
        if len(labels) >= 2:
            domain = ".".join(labels[-2:])
            heuristic = True
        else:
            domain = host
            heuristic = bool(host)
    except Exception:
        pass
    url_is_github = False
    try:
        h = host.lower()
        url_is_github = (
            h == "github.com"
            or h.endswith(".github.com")
            or h.endswith(".githubusercontent.com")
            or h == "github-redirect.dependabot.com"
        )
    except Exception:
        url_is_github = False
    return {
        "url_raw": url_raw,
        "url_host": host,
        "url_domain_etld1": domain,
        "heuristic_eTLD1": heuristic,
        "url_is_github": url_is_github,
    }


def _normalize_node_type(raw_type: str, source: str) -> tuple[str, str, str, int, str]:
    t = _s(raw_type)
    if not t:
        return "UnknownObject", "unknown", "missing_or_empty_type", 1000, ""
    if t in NATIVE_NODE_TYPES:
        return t, "map", "native_gh_core_type", 10, ""

    if source == "tar_entity_type_fine_grained" and t in {"GitHub_Files_FileChanges", "GitHub_GenSer_Other_Links"}:
        return "UnknownObject", "unknown", "ambiguous_github_internal_path", 20, "external_type_hint=" + t

    if t in {"GitHub_Service_External_Links", "GitHub_Other_Service", "GitHub_Files_FileChanges", "GitHub_GenSer_Other_Links"}:
        return "ExternalResource", "external", "link_or_service_pattern", 20, "external_type_hint=" + t

    if source == "tar_entity_type_fine_grained" and t in {"Issue_PR", "SHA", "Branch_Tag_GHDir"}:
        return "UnknownObject", "unknown", "ambiguous_fine_grained_type", 30, "external_type_hint=" + t

    if t == "Object":
        return "UnknownObject", "unknown", "generic_object_type", 40, ""

    return "UnknownObject", "unknown", "unrecognized_raw_type", 50, "external_type_hint=" + t


def _target_kind(pattern: str, match_text: str) -> tuple[str, str, str]:
    p = _s(pattern)
    mt = _s(match_text)
    if p == "GitHub_Service_External_Links" or ("http://" in mt.lower() or "https://" in mt.lower()) and "github" not in mt.lower():
        return "EXTERNAL", "high", "rule_a1"
    if p in {"GitHub_Other_Service"}:
        return "GH_SEMI", "med", "rule_a2"
    if p in REFERENCE_INTERNAL_PATTERNS:
        conf = "high" if p in {"Repo", "Actor", "Issue_PR", "SHA"} else "med"
        return "GH_INTERNAL", conf, "rule_a3"
    return "GH_SEMI", "low", "rule_a4_unknown_pattern"


def _service_hint(pattern: str, target_kind: str) -> tuple[str, str, str]:
    p = _s(pattern)
    if target_kind == "EXTERNAL":
        return "LINKS_TO", "high", "rule_b1"
    if p in {"Repo", "Actor"}:
        return "MENTIONS", "high", "rule_b2"
    if p in {
        "Issue_PR",
        "SHA",
        "CommitComment",
        "Gollum",
        "Release",
        "GitHub_Files_FileChanges",
        "GitHub_GenSer_Other_Links",
        "Branch_Tag_GHDir",
    }:
        return "REFERENCES", "med", "rule_b3"
    if p in {"GitHub_Other_Service"}:
        return "LINKS_TO", "med", "rule_b35"
    return "REFERENCES", "low", "rule_b4"


def _normalize_type_from_row(raw_type: str, pattern: str, match_text: str) -> str:
    t = _s(raw_type)
    if t in NATIVE_NODE_TYPES:
        return t
    p = _s(pattern)
    mt = _s(match_text).lower()
    if p == "GitHub_Service_External_Links" or ("http://" in mt or "https://" in mt) and "github" not in mt:
        return "ExternalResource"
    if p in {"GitHub_Other_Service", "GitHub_Files_FileChanges", "GitHub_GenSer_Other_Links"}:
        return "ExternalResource"
    if t in {"", "Object"}:
        return "UnknownObject"
    return "UnknownObject"


def _evidence_fields_used(row: pd.Series) -> list[str]:
    fields: list[str] = []
    for key in [
        "tar_entity_match_pattern_type",
        "tar_entity_match_text",
        "event_time",
        "tar_entity_objnt_prop_dict",
        "tar_entity_type_fine_grained",
        "tar_entity_type",
    ]:
        if _s(row.get(key)):
            fields.append(key)
    return fields


def _build_node_mapping(df: pd.DataFrame, outdir: Path) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    src_cols = [
        "src_entity_type",
        "tar_entity_type",
        "src_entity_type_agg",
        "tar_entity_type_agg",
        "tar_entity_type_fine_grained",
    ]
    for col in src_cols:
        if col not in df.columns:
            continue
        for raw_type in sorted(set(df[col].astype(str).tolist())):
            normalized_type, action, reason, priority, notes = _normalize_node_type(raw_type, col)
            rows.append(
                {
                    "raw_type": _s(raw_type),
                    "raw_type_source": col,
                    "normalized_type": normalized_type,
                    "action": action,
                    "reason": reason,
                    "priority": priority,
                    "notes": notes,
                }
            )
    node_df = pd.DataFrame(rows).drop_duplicates().sort_values(
        by=["raw_type_source", "priority", "raw_type"], kind="stable"
    )
    node_csv = outdir / "node_type_mapping.csv"
    node_df.to_csv(node_csv, index=False, encoding="utf-8")

    native_unknown = node_df[~node_df["raw_type"].isin(sorted(NATIVE_NODE_TYPES))]
    fg_counts = (
        df[df.get("relation_type", "").astype(str) == "Reference"]
        .assign(tar_entity_type_fine_grained=df.get("tar_entity_type_fine_grained", "").astype(str).str.strip())
    )
    fg_counts = fg_counts[fg_counts["tar_entity_type_fine_grained"] != ""]
    fg_vc = fg_counts["tar_entity_type_fine_grained"].value_counts().reset_index()
    fg_vc.columns = ["fine_grained", "count"]

    report = outdir / "node_type_mapping_report.md"
    lines = ["# Node Type Mapping Report", "", "## Unrecognized Raw Types", ""]
    if native_unknown.empty:
        lines.append("- None")
    else:
        for _, r in native_unknown[["raw_type", "raw_type_source", "normalized_type", "reason"]].head(200).iterrows():
            lines.append(f"- `{r['raw_type']}` from `{r['raw_type_source']}` -> `{r['normalized_type']}` ({r['reason']})")
    lines.extend(["", "## tar_entity_type_fine_grained Counts (Top 30)", ""])
    top30 = fg_vc.head(30)
    if top30.empty:
        lines.append("- None")
    else:
        for _, r in top30.iterrows():
            lines.append(f"- `{r['fine_grained']}`: {int(r['count'])}")
    tail_total = int(fg_vc.iloc[30:]["count"].sum()) if len(fg_vc) > 30 else 0
    lines.extend(["", "## Tail Summary", "", f"- tail_count_sum: {tail_total}"])
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return {
        "node_type_mapping_csv": str(node_csv),
        "node_type_mapping_report": str(report),
        "unrecognized_count": int(native_unknown.shape[0]),
    }


def _build_relation_mapping(df: pd.DataFrame, outdir: Path) -> dict[str, Any]:
    rel_rows: list[dict[str, Any]] = []

    event_df = df[df.get("relation_type", "").astype(str) == "EventAction"].copy()
    event_df["source_norm_type"] = event_df.get("src_entity_type", "").astype(str).map(lambda x: _normalize_type_from_row(x, "", ""))
    event_df["target_norm_type"] = event_df.get("tar_entity_type", "").astype(str).map(lambda x: _normalize_type_from_row(x, "", ""))
    event_unique = event_df[["relation_type", "event_type", "relation_label_repr", "source_norm_type", "target_norm_type"]].drop_duplicates()
    for _, row in event_unique.iterrows():
        rel_rows.append(
            {
                "raw_relation_type": "EventAction",
                "raw_event_type": _s(row["event_type"]),
                "raw_label_repr": _s(row["relation_label_repr"]),
                "source_norm_type": _s(row["source_norm_type"]) or "UnknownObject",
                "target_norm_type": _s(row["target_norm_type"]) or "UnknownObject",
                "normalized_rel": "EVENT_ACTION",
                "dir": "S2T",
                "confidence": "high",
                "service_hint": "",
                "target_kind": "",
                "service_confidence": "",
                "action": "map",
                "notes": json.dumps({"native_layer": True}, ensure_ascii=False),
            }
        )

    ref_df = df[df.get("relation_type", "").astype(str) == "Reference"].copy()
    for _, row in ref_df.iterrows():
        pattern = _s(row.get("tar_entity_match_pattern_type"))
        match_text = _s(row.get("tar_entity_match_text"))
        target_kind, tk_conf, tk_rule = _target_kind(pattern, match_text)
        service_hint, svc_conf, svc_rule = _service_hint(pattern, target_kind)

        notes_flags: list[str] = []
        if pattern in AMBIGUOUS_PATTERNS:
            notes_flags.append("ambiguous_pattern_requires_disambiguation")

        obj = _parse_obj_dict(row.get("tar_entity_objnt_prop_dict"))
        if obj:
            if ("repo_id" in obj or "actor_id" in obj) and service_hint == "REFERENCES":
                svc_conf = "high"
            if pattern in {"Repo", "Actor"} and ("repo_id" in obj or "actor_id" in obj):
                service_hint = "MENTIONS"
                svc_conf = "high"
        # Confidence coherence for ambiguous patterns:
        # default med; only upgrade to high when fine-grained type is deterministic native type.
        if pattern in AMBIGUOUS_PATTERNS:
            svc_conf = "med"
            fine_type = _s(row.get("tar_entity_type_fine_grained"))
            if fine_type in NATIVE_NODE_TYPES:
                svc_conf = "high"

        url_info = _extract_url_info(match_text)
        source_norm = _normalize_type_from_row(_s(row.get("src_entity_type")), "", "")
        target_raw = _s(row.get("tar_entity_type_fine_grained")) or _s(row.get("tar_entity_type"))
        target_norm = _normalize_type_from_row(target_raw, pattern, match_text)
        if target_kind == "EXTERNAL":
            target_norm = "ExternalResource"

        notes_obj = {
            "service_hint": service_hint,
            "target_kind": target_kind,
            "service_confidence": svc_conf,
            "target_kind_confidence": tk_conf,
            "decision_rules": [tk_rule, svc_rule],
            "evidence_fields_used": _evidence_fields_used(row),
            "tar_entity_match_pattern_type": pattern,
            "url_raw": url_info["url_raw"],
            "url_host": url_info["url_host"],
            "url_domain_etld1": url_info["url_domain_etld1"],
            "heuristic_eTLD1": url_info["heuristic_eTLD1"],
            "url_is_github": url_info["url_is_github"],
            "ref_evidence": {
                "tar_entity_match_text": match_text,
                "tar_entity_match_pattern_type": pattern,
                "event_time": _s(row.get("event_time")),
            },
        }
        if notes_flags:
            notes_obj["flags"] = notes_flags

        rel_rows.append(
            {
                "raw_relation_type": "Reference",
                "raw_event_type": _s(row.get("event_type")),
                "raw_label_repr": _s(row.get("relation_label_repr")),
                "source_norm_type": source_norm,
                "target_norm_type": target_norm,
                "normalized_rel": "REFERENCE",
                "dir": "S2T",
                "confidence": "high",
                "service_hint": service_hint,
                "target_kind": target_kind,
                "service_confidence": svc_conf,
                "action": "map",
                "notes": json.dumps(notes_obj, ensure_ascii=False),
            }
        )

    rel_rows.append(
        {
            "raw_relation_type": "PLACEHOLDER",
            "raw_event_type": "",
            "raw_label_repr": "",
            "source_norm_type": "PullRequest",
            "target_norm_type": "Issue",
            "normalized_rel": "RESOLVES",
            "dir": "S2T",
            "confidence": "low",
            "service_hint": "",
            "target_kind": "",
            "service_confidence": "",
            "action": "placeholder",
            "notes": "strict task semantic from Chapter 6, injected later",
        }
    )
    rel_rows.append(
        {
            "raw_relation_type": "PLACEHOLDER",
            "raw_event_type": "",
            "raw_label_repr": "",
            "source_norm_type": "Repo",
            "target_norm_type": "Repo",
            "normalized_rel": "COUPLES_WITH",
            "dir": "S2T",
            "confidence": "low",
            "service_hint": "",
            "target_kind": "",
            "service_confidence": "",
            "action": "placeholder",
            "notes": "structural analysis from Chapter 5, injected later",
        }
    )

    relation_df = pd.DataFrame(rel_rows)
    expanded_csv = outdir / "relation_mapping_eventaction_expanded.csv"
    relation_df.to_csv(expanded_csv, index=False, encoding="utf-8")
    # Backward-compatible alias
    legacy_csv = outdir / "relation_mapping.csv"
    relation_df.to_csv(legacy_csv, index=False, encoding="utf-8")

    # Native mapping view:
    # - keep Reference rows as-is
    # - keep placeholders
    # - collapse EventAction rows to native EVENT_ACTION interfaces with descriptive raw fields.
    event_rows = relation_df[relation_df["normalized_rel"] == "EVENT_ACTION"].copy()
    ref_rows = relation_df[relation_df["normalized_rel"] == "REFERENCE"].copy()
    placeholder_rows = relation_df[relation_df["action"] == "placeholder"].copy()
    native_rows: list[dict[str, Any]] = []

    event_group_cols = ["source_norm_type", "target_norm_type", "normalized_rel", "dir", "confidence", "action"]
    if not event_rows.empty:
        grouped = event_rows.groupby(event_group_cols, dropna=False, as_index=False)
        for _, g in grouped:
            raw_event_vals = sorted({_s(v) for v in g["raw_event_type"].tolist() if _s(v)})
            raw_label_vals = sorted({_s(v) for v in g["raw_label_repr"].tolist() if _s(v)})
            notes_obj = {
                "native_layer": True,
                "expanded_eventaction_count": int(g.shape[0]),
                "raw_event_type_values": raw_event_vals,
                "raw_label_repr_values": raw_label_vals,
            }
            native_rows.append(
                {
                    "raw_relation_type": "EventAction",
                    "raw_event_type": "|".join(raw_event_vals),
                    "raw_label_repr": "|".join(raw_label_vals),
                    "source_norm_type": _s(g.iloc[0]["source_norm_type"]) or "UnknownObject",
                    "target_norm_type": _s(g.iloc[0]["target_norm_type"]) or "UnknownObject",
                    "normalized_rel": "EVENT_ACTION",
                    "dir": "S2T",
                    "confidence": "high",
                    "service_hint": "",
                    "target_kind": "",
                    "service_confidence": "",
                    "action": "map",
                    "notes": json.dumps(notes_obj, ensure_ascii=False),
                }
            )
    native_df = pd.DataFrame(native_rows)
    if not ref_rows.empty:
        native_df = pd.concat([native_df, ref_rows], axis=0, ignore_index=True)
    if not placeholder_rows.empty:
        native_df = pd.concat([native_df, placeholder_rows], axis=0, ignore_index=True)
    native_agg_csv = outdir / "relation_mapping_native_aggregated.csv"
    native_df.to_csv(native_agg_csv, index=False, encoding="utf-8")

    cols = [
        "raw_relation_type",
        "raw_event_type",
        "raw_label_repr",
        "source_norm_type",
        "target_norm_type",
        "normalized_rel",
        "dir",
        "confidence",
        "service_hint",
        "target_kind",
        "service_confidence",
        "action",
        "notes",
    ]
    minimal_rows = [
        {
            "raw_relation_type": "EventAction",
            "raw_event_type": "*",
            "raw_label_repr": "*",
            "source_norm_type": "*",
            "target_norm_type": "*",
            "normalized_rel": "EVENT_ACTION",
            "dir": "S2T",
            "confidence": "high",
            "service_hint": "",
            "target_kind": "",
            "service_confidence": "",
            "action": "map",
            "notes": json.dumps(
                {
                    "native_layer": True,
                    "meaning": "Native fact entry. See expanded/aggregated files for event_type/label_repr details.",
                },
                ensure_ascii=False,
            ),
        },
        {
            "raw_relation_type": "Reference",
            "raw_event_type": "*",
            "raw_label_repr": "*",
            "source_norm_type": "*",
            "target_norm_type": "*",
            "normalized_rel": "REFERENCE",
            "dir": "S2T",
            "confidence": "high",
            "service_hint": "",
            "target_kind": "",
            "service_confidence": "",
            "action": "map",
            "notes": json.dumps(
                {
                    "native_layer": True,
                    "meaning": "Native fact entry. Per-row service_hint (MENTIONS/REFERENCES/LINKS_TO) is computed in expanded mappings.",
                },
                ensure_ascii=False,
            ),
        },
        {
            "raw_relation_type": "DERIVED_TASK",
            "raw_event_type": "",
            "raw_label_repr": "",
            "source_norm_type": "PullRequest",
            "target_norm_type": "Issue",
            "normalized_rel": "RESOLVES",
            "dir": "S2T",
            "confidence": "placeholder",
            "service_hint": "",
            "target_kind": "",
            "service_confidence": "",
            "action": "placeholder",
            "notes": json.dumps(
                {
                    "service_layer": True,
                    "meaning": "Strict task semantic from Chapter 6, injected later.",
                },
                ensure_ascii=False,
            ),
        },
        {
            "raw_relation_type": "DERIVED_STRUCT",
            "raw_event_type": "",
            "raw_label_repr": "",
            "source_norm_type": "Repo",
            "target_norm_type": "Repo",
            "normalized_rel": "COUPLES_WITH",
            "dir": "S2T",
            "confidence": "placeholder",
            "service_hint": "",
            "target_kind": "",
            "service_confidence": "",
            "action": "placeholder",
            "notes": json.dumps(
                {
                    "service_layer": True,
                    "meaning": "Structural relation from Chapter 5, injected later.",
                },
                ensure_ascii=False,
            ),
        },
    ]
    native_min_df = pd.DataFrame(minimal_rows, columns=cols)
    native_min_csv = outdir / "relation_mapping_native_minimal.csv"
    native_min_df.to_csv(native_min_csv, index=False, encoding="utf-8")

    rel_type_dist = df.get("relation_type", pd.Series(dtype=str)).astype(str).value_counts()
    expanded_event_df = relation_df[relation_df["raw_relation_type"] == "EventAction"].copy()
    event_type_dist = expanded_event_df["raw_event_type"].astype(str).value_counts().head(30)
    label_dist = expanded_event_df["raw_label_repr"].astype(str).value_counts().head(30)
    pattern_dist = (
        ref_df.get("tar_entity_match_pattern_type", pd.Series(dtype=str)).astype(str).value_counts().head(30)
    )

    report = outdir / "relation_mapping_report.md"
    lines = ["# Relation Mapping Report", "", "## relation_type Distribution", ""]
    for k, v in rel_type_dist.items():
        lines.append(f"- `{k}`: {int(v)}")
    lines.extend(["", "## Expanded EventAction summary", "", "### raw_event_type Top 30"])
    for k, v in event_type_dist.items():
        lines.append(f"- `{k}`: {int(v)}")
    lines.extend(["", "### raw_label_repr Top 30"])
    for k, v in label_dist.items():
        lines.append(f"- `{k}`: {int(v)}")
    native_rel_dist = native_df["normalized_rel"].astype(str).value_counts()
    lines.extend(["", "## Native mapping summary", ""])
    lines.extend(
        [
            "",
            "- `relation_mapping_native_minimal.csv`: 4-row minimal schema contracts for validator/generator constraints.",
            "- `relation_mapping_native_aggregated.csv`: aggregated native audit/coverage view.",
            "- `relation_mapping_eventaction_expanded.csv`: detailed traceability view.",
        ]
    )
    for k, v in native_rel_dist.items():
        lines.append(f"- `{k}`: {int(v)}")
    lines.extend(["", "### Native EVENT_ACTION by source/target", ""])
    native_event = native_df[native_df["normalized_rel"] == "EVENT_ACTION"]
    if native_event.empty:
        lines.append("- none")
    else:
        native_event_grp = (
            native_event.groupby(["source_norm_type", "target_norm_type"], dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        for _, r in native_event_grp.head(50).iterrows():
            lines.append(
                f"- `{_s(r['source_norm_type'])}` -> `{_s(r['target_norm_type'])}`: {int(r['count'])}"
            )
    lines.extend(["", "## tar_entity_match_pattern_type Top 30", ""])
    for k, v in pattern_dist.items():
        lines.append(f"- `{k}`: {int(v)}")
    svc_dist = (
        relation_df[(relation_df["raw_relation_type"] == "Reference")]
        .groupby(["service_hint", "target_kind", "service_confidence"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    lines.extend(["", "## Reference Service Decision Distribution", ""])
    for _, r in svc_dist.iterrows():
        lines.append(
            f"- service_hint=`{_s(r['service_hint'])}`, target_kind=`{_s(r['target_kind'])}`, "
            f"service_confidence=`{_s(r['service_confidence'])}`: {int(r['count'])}"
        )

    lines.extend(["", "## Pattern Examples (3 each)", ""])
    for pattern in ref_df.get("tar_entity_match_pattern_type", pd.Series(dtype=str)).astype(str).value_counts().index.tolist():
        lines.append(f"### {pattern or '__EMPTY__'}")
        subset = ref_df[ref_df.get("tar_entity_match_pattern_type", "").astype(str) == pattern].head(3)
        if subset.empty:
            lines.append("- none")
            continue
        for _, r in subset.iterrows():
            tk, _, _ = _target_kind(_s(r.get("tar_entity_match_pattern_type")), _s(r.get("tar_entity_match_text")))
            sh, sc, _ = _service_hint(_s(r.get("tar_entity_match_pattern_type")), tk)
            if _s(r.get("tar_entity_match_pattern_type")) in AMBIGUOUS_PATTERNS:
                sc = "med"
                if _s(r.get("tar_entity_type_fine_grained")) in NATIVE_NODE_TYPES:
                    sc = "high"
            lines.append(
                "- "
                + json.dumps(
                    {
                        "tar_entity_match_text": _s(r.get("tar_entity_match_text"))[:200],
                        "tar_entity_type_fine_grained": _s(r.get("tar_entity_type_fine_grained")),
                        "service_hint": sh,
                        "target_kind": tk,
                        "service_confidence": sc,
                    },
                    ensure_ascii=False,
                )
            )
        lines.append("")
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return {
        "relation_mapping_csv": str(legacy_csv),
        "relation_mapping_eventaction_expanded_csv": str(expanded_csv),
        "relation_mapping_native_aggregated_csv": str(native_agg_csv),
        "relation_mapping_native_minimal_csv": str(native_min_csv),
        "relation_mapping_report": str(report),
        "relation_rows": int(relation_df.shape[0]),
        "relation_native_rows": int(native_df.shape[0]),
    }


def _write_schema_readme(outdir: Path) -> str:
    path = outdir / "schema_readme.md"
    lines = [
        "# Schema Readme",
        "",
        "## Two-Track Semantics",
        "",
        "- Track A (native fact track): `rel_type` is the native boundary and must be `EVENT_ACTION` or `REFERENCE`.",
        "- Track B (service action interface): optional `service_rel_type` projection for query usability.",
        "- Long-tail EventAction patterns remain queryable through native `EVENT_ACTION` + evidence fields such as `raw_relation_label_repr`.",
        "",
        "## Which mapping to use",
        "",
        "- `relation_mapping_native_minimal.csv`: use for schema validator + controlled generator constraints (stable minimal relation interfaces).",
        "- `relation_mapping_native_aggregated.csv`: use for audit and coverage summaries over native interfaces.",
        "- `relation_mapping_eventaction_expanded.csv`: use for traceability, evidence drill-down, and detailed event/action analysis.",
        "",
        "## Explicit Service Fields",
        "",
        "- `service_rel_type`: query-facing projection; never replaces native `rel_type`.",
        "- `service_hint`: derived service-view intent for `REFERENCE` rows (`MENTIONS` / `REFERENCES` / `LINKS_TO`).",
        "- `target_kind`: coarse target category (`GH_INTERNAL` / `GH_SEMI` / `EXTERNAL`).",
        "- `service_confidence`: confidence for service hint assignment.",
        "",
        "## URL Evidence",
        "",
        "- `notes` JSON keeps URL evidence (`url_raw`, `url_host`, `url_domain_etld1`, `heuristic_eTLD1`).",
        "- `url_is_github` is `true` when host best-effort matches one of:",
        "  - `github.com`",
        "  - `*.github.com`",
        "  - `*.githubusercontent.com`",
        "  - `github-redirect.dependabot.com`",
        "",
        "## Evidence Policy",
        "",
        "- `evidence_fields_by_relation` is trace/report-only and not a required validator property set.",
        "- `forbidden_relation_labels` blocks legacy labels (e.g., `REFERS_TO`) from new exports.",
        "- `alias_relations` is read-time backward compatibility only (e.g., `REFERS_TO -> REFERENCE`).",
        "",
        "## Time Fields (Dual Preservation)",
        "",
        "- `event_time`: raw source timestamp string.",
        "- `source_event_time`: normalized canonical timestamp for query-time filtering/sorting.",
        "",
        "## Boundary Reminder",
        "",
        "- Native relation interfaces stay minimal: `EVENT_ACTION` and `REFERENCE` (+ placeholders `RESOLVES`, `COUPLES_WITH`).",
        "- Do not infer `RESOLVES` or `COUPLES_WITH` from generic EventAction/Reference facts in this step.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)


def _find_latest_edges_csv() -> Path | None:
    candidates = [
        Path("data_real") / "pilot_output" / "csv" / "edges.csv",
        Path("graph-migration") / "fixtures" / "real_pilot_redis" / "pilot_output" / "csv" / "edges.csv",
    ]
    for c in candidates:
        abs_c = Path(__file__).resolve().parents[2] / c
        if abs_c.exists():
            return abs_c
    data_real = Path(__file__).resolve().parents[2] / "data_real"
    pool = list(data_real.glob("**/csv/edges.csv")) if data_real.exists() else []
    if not pool:
        return None
    pool.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return pool[0]


def _extract_eventaction_verb_from_raw_label(raw_label: str) -> str:
    text = _s(raw_label)
    if not text or "_" not in text:
        return ""
    parts = [p for p in text.split("_") if p]
    if len(parts) < 3:
        return ""
    return "_".join(parts[1:-1])


def _write_eventaction_service_verbs(outdir: Path) -> dict[str, Any]:
    payload = build_service_verbs_payload()
    path = outdir / "eventaction_service_verbs.yaml"
    lines = [
        "verbs_universe:",
    ]
    for v in payload.get("verbs_universe", []):
        lines.append(f"  - {v}")
    lines.extend(["service_rel_map:"])
    for k, v in payload.get("service_rel_map", {}).items():
        lines.append(f"  {k}: {v}")
    lines.extend(["S_core:"])
    for v in payload.get("S_core", []):
        lines.append(f"  - {v}")
    lines.extend(["S_core_25:"])
    for v in payload.get("S_core_25", []):
        lines.append(f"  - {v}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"path": str(path), "payload": payload}


def _write_eventaction_service_coverage_report(outdir: Path, service_payload: dict[str, Any]) -> str:
    path = outdir / "eventaction_service_coverage_report.md"
    edges_path = _find_latest_edges_csv()
    lines = [
        "# EventAction Service Coverage Report",
        "",
    ]
    if edges_path is None or not edges_path.exists():
        lines.append("- edges.csv not found; coverage report skipped.")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return str(path)

    df = pd.read_csv(edges_path, dtype=str, keep_default_na=False)
    lines.append(f"- edges_csv: `{edges_path}`")
    lines.append(f"- row_count: `{len(df)}`")
    lines.append("")

    if "native_rel_type" not in df.columns:
        lines.append("- `native_rel_type` column not found in edges.csv. Run migration after two-track export update.")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return str(path)

    ev_df = df[df["native_rel_type"].astype(str) == "EVENT_ACTION"].copy()
    lines.append(f"- event_action_rows: `{len(ev_df)}`")
    lines.append("")

    if ev_df.empty:
        lines.append("- no EVENT_ACTION rows found.")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return str(path)

    def parse_props(s: str) -> dict[str, Any]:
        try:
            obj = json.loads(s or "{}")
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    ev_df["__props"] = ev_df.get("properties_json", "").astype(str).map(parse_props)
    ev_df["verb_label"] = ev_df["__props"].map(lambda d: _s(d.get("eventaction_verb_label")) or _extract_eventaction_verb_from_raw_label(_s(d.get("raw_relation_label_repr"))))
    ev_df["raw_relation_label_repr"] = ev_df["__props"].map(lambda d: _s(d.get("raw_relation_label_repr")))
    ev_df["service_rel_type"] = ev_df.get("service_rel_type", pd.Series(dtype=str)).astype(str).map(_s)
    ev_df["src_type"] = ev_df["raw_relation_label_repr"].map(lambda x: x.split("_")[0] if "_" in x else "")
    ev_df["dst_type"] = ev_df["raw_relation_label_repr"].map(lambda x: x.split("_")[-1] if "_" in x else "")

    verb_vc = ev_df["verb_label"].value_counts()
    lines.extend(["## Verb Frequency (EVENT_ACTION)", "", "| verb_label | count | ratio |", "|---|---:|---:|"])
    total = len(ev_df) or 1
    for k, v in verb_vc.head(200).items():
        lines.append(f"| {k or '__EMPTY__'} | {int(v)} | {v/total:.4f} |")

    pair_grp = (
        ev_df.groupby(["verb_label", "src_type", "dst_type"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    lines.extend(["", "## (src_type, dst_type) Pair Counts Per Verb (Top 100)", "", "| verb_label | src_type | dst_type | count |", "|---|---|---|---:|"])
    for _, r in pair_grp.head(100).iterrows():
        lines.append(f"| {_s(r['verb_label']) or '__EMPTY__'} | {_s(r['src_type'])} | {_s(r['dst_type'])} | {int(r['count'])} |")

    s_core_25 = set(service_payload.get("S_core_25", []))
    service_map = service_payload.get("service_rel_map", {})
    if s_core_25:
        mapped = ev_df["verb_label"].map(lambda v: service_map.get(_s(v), ""))
        hit = mapped.map(lambda x: x in s_core_25)
        coverage_curve = []
        sorted_core = list(service_payload.get("S_core_25", []))
        for k in range(1, len(sorted_core) + 1):
            subset = set(sorted_core[:k])
            cov = mapped.map(lambda x: x in subset).mean()
            coverage_curve.append((k, cov))
        lines.extend(["", "## Cumulative Coverage Curve (S_core_25 prefix)", "", "| k | coverage |", "|---:|---:|"])
        for k, cov in coverage_curve:
            lines.append(f"| {k} | {cov:.4f} |")

    blank_share = (ev_df["service_rel_type"] == "").mean()
    lines.extend(["", "## Uncovered Share", "", f"- blank_service_rel_type_ratio: `{blank_share:.4f}`"])

    uncovered = ev_df[ev_df["service_rel_type"] == ""]
    top_uncovered = uncovered["raw_relation_label_repr"].value_counts().head(20)
    lines.extend(["", "### Top 20 raw_relation_label_repr among uncovered EVENT_ACTION edges", "", "| raw_relation_label_repr | count |", "|---|---:|"])
    if top_uncovered.empty:
        lines.append("| (none) | 0 |")
    else:
        for k, v in top_uncovered.items():
            lines.append(f"| {k or '__EMPTY__'} | {int(v)} |")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)


def _update_schema_metadata_with_service_set(service_payload: dict[str, Any]) -> str:
    schema_path = Path(__file__).resolve().parents[2] / "data_real" / "pilot_queries" / "schema_metadata.yaml"
    if not schema_path.exists():
        return ""

    base_nodes = "Actor, Repo, Issue, PullRequest, Commit, Branch, Tag, IssueComment, PullRequestReview, PullRequestReviewComment, CommitComment, Gollum, Release, Push, UnknownObject, ExternalResource"
    s_core = list(service_payload.get("S_core", []))
    service_view = ["MENTIONS", "REFERENCES", "LINKS_TO"] + [x for x in s_core if x not in {"MENTIONS", "REFERENCES", "LINKS_TO"}]
    triples = load_event_trigger_triples_dict()
    service_map = service_payload.get("service_rel_map", {})
    dirs: set[str] = set()

    def _base_label(entity_spec: str) -> str:
        text = _s(entity_spec)
        if not text:
            return ""
        # Keep only type-level label for schema constraints.
        # Examples:
        # - Repo::repo_id=fork_forkee_id -> Repo
        # - Branch::branch_name=_trim_refs_heads(push_ref) -> Branch
        return text.split("::", 1)[0].strip()

    for rels in triples.values():
        if not isinstance(rels, list):
            continue
        for tri in rels:
            if not (isinstance(tri, (list, tuple)) and len(tri) == 3):
                continue
            src, rel, dst = str(tri[0]), str(tri[1]), str(tri[2])
            if not rel.startswith("EventAction::label="):
                continue
            verb = rel.split("=", 1)[1]
            mapped = service_map.get(verb) or canonical_service_rel_name(verb)
            src_label = _base_label(src)
            dst_label = _base_label(dst)
            if mapped and src_label and dst_label:
                dirs.add(f"{src_label}-[:{mapped}]->{dst_label}")

    lines = [
        "# schema_metadata.yaml (Pilot, A-mode, Two-track)",
        "",
        "mode:",
        "  allow_service_view_rel_in_cypher: true",
        "  allow_placeholders_in_cypher: false",
        "",
        f"allowed_node_labels: [{base_nodes}]",
        "allowed_relationship_types: [EVENT_ACTION, REFERENCE]",
        "service_view_candidates: [" + ", ".join(service_view) + "]",
        "placeholder_relation_types: [COUPLES_WITH, RESOLVES]",
        "",
        "# Backward-compatible top-level flags for existing runner path.",
        "# NOTE: duplicated flags are kept for backward compatibility with runner;",
        "# keep them consistent with mode.*.",
        "allow_service_view_rel_in_cypher: true",
        "allow_placeholders_in_cypher: false",
        "",
        "two_track_fields:",
        "  native_rel_type_field: rel_type",
        "  service_rel_type_field: service_rel_type",
        "",
        "allowed_properties: [entity_id, raw_event_type, raw_relation_type, raw_relation_label_repr, raw_event_trigger, source_event_id]",
        "properties_by_relation:",
        "  EVENT_ACTION: [event_time, source_event_time]",
        "  REFERENCE: [event_time, source_event_time]",
        "",
        "# Trace/report only; not a required validator property set.",
        "evidence_fields_by_relation:",
        "  REFERENCE:",
        "    - tar_entity_match_text",
        "    - tar_entity_match_pattern_type",
        "    - url_raw",
        "    - url_domain_etld1",
        "",
        "forbidden_relation_labels: [REFERS_TO]",
        "alias_relations:",
        "  REFERS_TO: REFERENCE",
        "",
        "derivation_notes:",
        "  - \"direction_constraints are schema-level type constraints only; no field bindings or helper functions are included.\"",
        "  - \"Some EventAction edge realizations depend on row fields (e.g., pull_head_ref, release_tag_name) in implementation.\"",
        "  - \"Online API helper functions (e.g., __get_*) belong to data derivation logic, not schema constraints.\"",
        "",
        "direction_constraints:",
    ]
    for d in sorted(dirs):
        lines.append(f"  - \"{d}\"")
    schema_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(schema_path)


def _collect_missing_sentinels(df: pd.DataFrame) -> list[Any]:
    observed = {"nan", "NaN", "", "None", None}
    for col in df.columns:
        vals = df[col].astype(str).head(20000)
        for v in vals:
            t = v.strip()
            if t in {"", "nan", "NaN", "None", "null", "NULL", "N/A", "na"}:
                observed.add(t)
    ordered = ["nan", "NaN", "", "None", None]
    extras = [x for x in sorted(observed, key=lambda x: "" if x is None else str(x)) if x not in ordered]
    return ordered + extras


def _write_placeholder_rules(df: pd.DataFrame, outdir: Path) -> str:
    sentinels = _collect_missing_sentinels(df)
    path = outdir / "placeholder_rules.yaml"
    lines = [
        "missing_sentinels:",
    ]
    for s in sentinels:
        if s is None:
            lines.append("  - null")
        else:
            lines.append(f"  - \"{s}\"")
    lines.extend(
        [
            "scope:",
            "  apply_only_when_relation_type_in:",
            "    - \"Reference\"",
            "object_fallback:",
            "  url_evidence_field: tar_entity_match_text",
            "  url_detect_regex: \"https?://\"",
            "  reference_target_missing_with_url: ExternalResource",
            "  reference_target_missing_without_url: UnknownObject",
            "evidence_preservation:",
            "  reference_fields:",
            "    - tar_entity_match_text",
            "    - tar_entity_match_pattern_type",
            "    - event_time",
            "url_processing:",
            "  enabled: true",
            "  extract_first_url: true",
            "  extract_all_urls_if_easy: true",
            "  extract_host: true",
            "  extract_domain_etld1_fallback_last_two_labels: true",
            "  heuristic_eTLD1: true",
            "  url_is_github_rules:",
            "    - \"github.com\"",
            "    - \"*.github.com\"",
            "    - \"*.githubusercontent.com\"",
            "    - \"github-redirect.dependabot.com\"",
            "  never_crash: true",
            "stringified_object_parse:",
            "  field: tar_entity_objnt_prop_dict",
            "  best_effort: true",
            "  keep_original_string_on_fail: true",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)


def build_real_mappings(input_csv_path: str, outdir: str) -> dict[str, Any]:
    out_path = Path(outdir)
    out_path.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(input_csv_path, dtype=str, keep_default_na=False)

    node_outputs = _build_node_mapping(df, out_path)
    rel_outputs = _build_relation_mapping(df, out_path)
    service_payload_out = _write_eventaction_service_verbs(out_path)
    coverage_report = _write_eventaction_service_coverage_report(out_path, service_payload_out["payload"])
    schema_metadata_path = _update_schema_metadata_with_service_set(service_payload_out["payload"])
    placeholder_yaml = _write_placeholder_rules(df, out_path)
    schema_readme = _write_schema_readme(out_path)

    return {
        "input": input_csv_path,
        "outdir": str(out_path),
        **node_outputs,
        **rel_outputs,
        "eventaction_service_verbs_yaml": service_payload_out["path"],
        "eventaction_service_coverage_report_md": coverage_report,
        "schema_metadata_yaml": schema_metadata_path,
        "placeholder_rules_yaml": placeholder_yaml,
        "schema_readme_md": schema_readme,
    }
