from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from loaders.real_query_loader import load_structured_file


@dataclass
class Group3Template:
    template_id: str
    family: str
    intent: str
    cypher_skeleton: str
    covered_queries: list[str] = field(default_factory=list)
    required_slots: list[dict[str, Any]] = field(default_factory=list)
    optional_slots: list[dict[str, Any]] = field(default_factory=list)
    derived_slots: list[dict[str, Any]] = field(default_factory=list)
    constraints: dict[str, Any] = field(default_factory=dict)
    property_whitelist: dict[str, Any] = field(default_factory=dict)
    repo_scope_policy: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class Group3TemplateInventory:
    templates_by_id: dict[str, Group3Template]
    query_to_template_id: dict[str, str]
    injection_pending: list[dict[str, Any]]
    raw_payload: dict[str, Any] = field(default_factory=dict)


def _to_list(v: Any) -> list[Any]:
    return v if isinstance(v, list) else []


def load_group3_template_inventory(path: str | Path) -> Group3TemplateInventory:
    payload = load_structured_file(path)
    if not isinstance(payload, dict):
        payload = {}

    templates_by_id: dict[str, Group3Template] = {}
    query_to_template_id: dict[str, str] = {}

    for item in _to_list(payload.get("templates")):
        if not isinstance(item, dict):
            continue
        template_id = str(item.get("template_id") or "").strip()
        if not template_id:
            continue
        template = Group3Template(
            template_id=template_id,
            family=str(item.get("family") or "").strip(),
            intent=str(item.get("intent") or "").strip(),
            cypher_skeleton=str(item.get("cypher_skeleton") or "").strip(),
            covered_queries=[str(x).strip() for x in _to_list(item.get("covered_queries")) if str(x).strip()],
            required_slots=[
                x for x in _to_list(item.get("required_slots")) if isinstance(x, dict)
            ],
            optional_slots=[
                x for x in _to_list(item.get("optional_slots")) if isinstance(x, dict)
            ],
            derived_slots=[
                x for x in _to_list(item.get("derived_slots")) if isinstance(x, dict)
            ],
            constraints=item.get("constraints", {}) if isinstance(item.get("constraints"), dict) else {},
            property_whitelist=item.get("property_whitelist", {}) if isinstance(item.get("property_whitelist"), dict) else {},
            repo_scope_policy=[
                x for x in _to_list(item.get("repo_scope_policy")) if isinstance(x, dict)
            ],
        )
        templates_by_id[template_id] = template
        for qid in template.covered_queries:
            query_to_template_id[qid] = template_id

    injection_pending = [
        x for x in _to_list(payload.get("injection_pending_templates")) if isinstance(x, dict)
    ]

    return Group3TemplateInventory(
        templates_by_id=templates_by_id,
        query_to_template_id=query_to_template_id,
        injection_pending=injection_pending,
        raw_payload=payload,
    )

