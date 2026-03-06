from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


def _strip_comment(line: str) -> str:
    in_single = False
    in_double = False
    out_chars: list[str] = []
    for ch in line:
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        if ch == "#" and not in_single and not in_double:
            break
        out_chars.append(ch)
    return "".join(out_chars).rstrip()


def _parse_scalar(text: str) -> Any:
    t = text.strip()
    if not t:
        return ""
    if t in {"null", "Null", "NULL", "~"}:
        return None
    if t in {"true", "True"}:
        return True
    if t in {"false", "False"}:
        return False
    if (t.startswith('"') and t.endswith('"')) or (t.startswith("'") and t.endswith("'")):
        return t[1:-1]
    if t.startswith("[") and t.endswith("]"):
        inner = t[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar(part.strip()) for part in inner.split(",")]
    if re.fullmatch(r"-?\d+", t):
        try:
            return int(t)
        except Exception:
            return t
    if re.fullmatch(r"-?\d+\.\d+", t):
        try:
            return float(t)
        except Exception:
            return t
    return t


def _simple_yaml_load(text: str) -> Any:
    raw_lines = [_strip_comment(line) for line in text.splitlines()]
    lines = [line for line in raw_lines if line.strip()]
    if not lines:
        return {}

    idx = 0

    def indent_of(s: str) -> int:
        return len(s) - len(s.lstrip(" "))

    def parse_block(base_indent: int) -> Any:
        nonlocal idx
        if idx >= len(lines):
            return {}

        if lines[idx].lstrip().startswith("- "):
            arr: list[Any] = []
            while idx < len(lines):
                line = lines[idx]
                ind = indent_of(line)
                if ind < base_indent or not line.lstrip().startswith("- "):
                    break
                content = line[ind + 2 :].strip()
                if not content:
                    idx += 1
                    arr.append(parse_block(ind + 2))
                    continue
                if ":" in content and not content.startswith(("'", '"')):
                    key, val = content.split(":", 1)
                    item: dict[str, Any] = {key.strip(): _parse_scalar(val.strip()) if val.strip() else None}
                    idx += 1
                    while idx < len(lines):
                        nxt = lines[idx]
                        nxt_ind = indent_of(nxt)
                        if nxt_ind <= ind:
                            break
                        if nxt.lstrip().startswith("- "):
                            break
                        k, v = nxt.strip().split(":", 1)
                        if v.strip():
                            item[k.strip()] = _parse_scalar(v.strip())
                            idx += 1
                        else:
                            idx += 1
                            item[k.strip()] = parse_block(nxt_ind + 2)
                    arr.append(item)
                else:
                    arr.append(_parse_scalar(content))
                    idx += 1
            return arr

        obj: dict[str, Any] = {}
        while idx < len(lines):
            line = lines[idx]
            ind = indent_of(line)
            if ind < base_indent:
                break
            if line.lstrip().startswith("- "):
                break
            if ":" not in line:
                idx += 1
                continue
            key, val = line.strip().split(":", 1)
            if val.strip():
                obj[key.strip()] = _parse_scalar(val.strip())
                idx += 1
            else:
                idx += 1
                obj[key.strip()] = parse_block(ind + 2)
        return obj

    return parse_block(0)


def load_structured_file(path: str | Path) -> Any:
    text = Path(path).read_text(encoding="utf-8")
    try:
        return json.loads(text)
    except Exception:
        pass
    try:
        import yaml  # type: ignore

        return yaml.safe_load(text)
    except Exception:
        return _simple_yaml_load(text)


@dataclass
class PilotQueryRecord:
    id: str
    nl_query: str
    query_type_raw: str
    query_type: str
    gold_cypher: str
    predicted_query_type: str
    extracted_slot_candidates: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    intent_constraints: Any = None
    raw: dict[str, Any] = field(default_factory=dict)
    valid: bool = True
    issues: list[str] = field(default_factory=list)


@dataclass
class PilotQueryLoadResult:
    valid_records: list[PilotQueryRecord]
    all_records: list[PilotQueryRecord]
    invalid_examples: list[dict[str, Any]]
    counts_by_query_type: dict[str, int]
    missing_field_issues: list[dict[str, Any]]
    unknown_query_types: list[dict[str, Any]]


def _build_taxonomy_maps(taxonomy_payload: Any) -> tuple[set[str], dict[str, str]]:
    canonical: set[str] = set()
    alias_to_canonical: dict[str, str] = {}

    if isinstance(taxonomy_payload, dict):
        direct = taxonomy_payload.get("query_types")
        if isinstance(direct, list):
            for item in direct:
                if isinstance(item, str):
                    label = item.strip()
                    if label:
                        canonical.add(label)
                        alias_to_canonical[label.lower()] = label
                elif isinstance(item, dict):
                    label = str(item.get("label") or item.get("name") or "").strip()
                    if not label:
                        continue
                    canonical.add(label)
                    alias_to_canonical[label.lower()] = label
                    aliases = item.get("aliases", [])
                    if isinstance(aliases, list):
                        for a in aliases:
                            alias_to_canonical[str(a).strip().lower()] = label

        aliases_map = taxonomy_payload.get("aliases")
        if isinstance(aliases_map, dict):
            for alias, label in aliases_map.items():
                label_str = str(label).strip()
                if not label_str:
                    continue
                canonical.add(label_str)
                alias_to_canonical[str(alias).strip().lower()] = label_str
                alias_to_canonical[label_str.lower()] = label_str

        canon_alias = taxonomy_payload.get("canonical_to_aliases")
        if isinstance(canon_alias, dict):
            for label, aliases in canon_alias.items():
                label_str = str(label).strip()
                if not label_str:
                    continue
                canonical.add(label_str)
                alias_to_canonical[label_str.lower()] = label_str
                if isinstance(aliases, list):
                    for a in aliases:
                        alias_to_canonical[str(a).strip().lower()] = label_str

    return canonical, alias_to_canonical


def load_real_pilot_queries(
    queries_jsonl_path: str | Path,
    query_taxonomy_path: str | Path,
) -> PilotQueryLoadResult:
    taxonomy_payload = load_structured_file(query_taxonomy_path)
    canonical_types, alias_to_canonical = _build_taxonomy_maps(taxonomy_payload)

    required_fields = ("id", "nl_query", "query_type", "gold_cypher")
    all_records: list[PilotQueryRecord] = []
    valid_records: list[PilotQueryRecord] = []
    missing_field_issues: list[dict[str, Any]] = []
    unknown_query_types: list[dict[str, Any]] = []
    invalid_examples: list[dict[str, Any]] = []
    counts_by_query_type: dict[str, int] = {}

    for line_no, line in enumerate(
        Path(queries_jsonl_path).read_text(encoding="utf-8").splitlines(), start=1
    ):
        line = line.lstrip("\ufeff")
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except Exception as exc:
            invalid_examples.append(
                {"line": line_no, "id": None, "issues": [f"INVALID_JSONL:{type(exc).__name__}"]}
            )
            continue
        if not isinstance(payload, dict):
            invalid_examples.append(
                {"line": line_no, "id": None, "issues": ["INVALID_JSON_OBJECT"]}
            )
            continue

        issues: list[str] = []
        missing = [f for f in required_fields if not str(payload.get(f, "")).strip()]
        if missing:
            issues.append("MISSING_REQUIRED_FIELDS:" + ",".join(missing))
            missing_field_issues.append(
                {"line": line_no, "id": payload.get("id"), "missing_fields": missing}
            )

        raw_query_type = str(payload.get("query_type", "")).strip()
        canonical_query_type = alias_to_canonical.get(raw_query_type.lower(), raw_query_type)
        if canonical_types and canonical_query_type not in canonical_types:
            issues.append("UNKNOWN_QUERY_TYPE:" + raw_query_type)
            unknown_query_types.append(
                {
                    "line": line_no,
                    "id": payload.get("id"),
                    "query_type_raw": raw_query_type,
                    "query_type_normalized": canonical_query_type,
                }
            )

        slots = payload.get("extracted_slot_candidates")
        if not isinstance(slots, dict):
            slots = {}

        rec = PilotQueryRecord(
            id=str(payload.get("id", "")).strip(),
            nl_query=str(payload.get("nl_query", "")),
            query_type_raw=raw_query_type,
            query_type=canonical_query_type,
            gold_cypher=str(payload.get("gold_cypher", "")),
            predicted_query_type=canonical_query_type,
            extracted_slot_candidates={k: v for k, v in slots.items() if isinstance(v, list)},
            intent_constraints=payload.get("intent_constraints"),
            raw=dict(payload),
            valid=(len(issues) == 0),
            issues=issues,
        )
        all_records.append(rec)
        counts_by_query_type[canonical_query_type] = counts_by_query_type.get(canonical_query_type, 0) + 1
        if rec.valid:
            valid_records.append(rec)
        else:
            invalid_examples.append({"line": line_no, "id": rec.id, "issues": list(issues)})

    return PilotQueryLoadResult(
        valid_records=valid_records,
        all_records=all_records,
        invalid_examples=invalid_examples,
        counts_by_query_type=counts_by_query_type,
        missing_field_issues=missing_field_issues,
        unknown_query_types=unknown_query_types,
    )


def write_pilot_queries_report(
    result: PilotQueryLoadResult,
    out_path: str | Path,
) -> None:
    lines = [
        "# Pilot Queries Report",
        "",
        f"- total_count: {len(result.all_records)}",
        f"- valid_count: {len(result.valid_records)}",
        f"- invalid_count: {len(result.invalid_examples)}",
        "",
        "## Counts by query_type",
        "",
    ]
    if not result.counts_by_query_type:
        lines.append("- none")
    else:
        for qt, cnt in sorted(result.counts_by_query_type.items(), key=lambda kv: kv[0]):
            lines.append(f"- `{qt}`: {cnt}")

    lines.extend(["", "## Unknown/Invalid query_type", ""])
    if not result.unknown_query_types:
        lines.append("- none")
    else:
        for item in result.unknown_query_types:
            lines.append(
                f"- id=`{item.get('id')}` line={item.get('line')} raw=`{item.get('query_type_raw')}` "
                f"normalized=`{item.get('query_type_normalized')}`"
            )

    lines.extend(["", "## Missing field issues", ""])
    if not result.missing_field_issues:
        lines.append("- none")
    else:
        for item in result.missing_field_issues:
            lines.append(
                f"- id=`{item.get('id')}` line={item.get('line')} missing={item.get('missing_fields')}"
            )

    Path(out_path).write_text("\n".join(lines) + "\n", encoding="utf-8")
