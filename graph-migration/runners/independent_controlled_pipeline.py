from __future__ import annotations

"""Bounded, annotation-free NL -> ControlledQueryIR -> Cypher path.

This module intentionally accepts only a request id and natural-language text.
Evaluation annotations are loaded by a separate post-run evaluator.
"""

import json
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import yaml

from normalizers.derived_slot_builder import build_repo_scope_prefixes
from validators.pilot_cypher_validator import (
    StaticSchemaSpec,
    ValidationError,
    validate_cypher_static,
)


ENTITY_PATTERNS: list[tuple[str, str, re.Pattern[str]]] = [
    ("PullRequestReviewComment", "PRRC", re.compile(r"(?<![A-Za-z0-9_])PRRC_\d+#[^\s,.;!?]+")),
    ("PullRequestReview", "PRR", re.compile(r"(?<![A-Za-z0-9_])PRR_\d+#[^\s,.;!?]+")),
    ("IssueComment", "IC", re.compile(r"(?<![A-Za-z0-9_])IC_\d+#[^\s,.;!?]+")),
    ("PullRequest", "PR", re.compile(r"(?<![A-Za-z0-9_])PR_\d+#[^\s,.;!?]+")),
    ("Issue", "I", re.compile(r"(?<![A-Za-z0-9_])I_\d+#[^\s,.;!?]+")),
    ("Commit", "C", re.compile(r"(?<![A-Za-z0-9_])C_\d+[@#][^\s,.;!?]+")),
    ("Repo", "R", re.compile(r"(?<![A-Za-z0-9_])R_\d+(?![A-Za-z0-9_])")),
    ("Actor", "A", re.compile(r"(?<![A-Za-z0-9_])A_\d+(?![A-Za-z0-9_])")),
]

LABEL_FROM_PREFIX = {
    "R": "Repo",
    "A": "Actor",
    "I": "Issue",
    "PR": "PullRequest",
    "IC": "IssueComment",
    "PRR": "PullRequestReview",
    "PRRC": "PullRequestReviewComment",
    "C": "Commit",
}

SERVICE_LEXICON = {
    "OPENED_BY": ("opened", "open", "opened by", "owner"),
    "REFERENCES": ("referenced", "references", "reference", "referencing", "objects are referenced"),
    "MENTIONS": ("mentioned", "mentions", "mention"),
    "LINKS_TO": ("external link", "external links", "links to", "linked to", "link to"),
}

TOKEN_PATTERN = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")
DATE_PATTERN = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")
YEAR_PATTERN = re.compile(r"\b(20\d{2}|2100)\b")
LIMIT_PATTERN = re.compile(r"\b(?:top|limit)\s+(\d+)\b", re.IGNORECASE)

ENTITY_SLOT_BY_LABEL = {
    "Issue": "issue_entity_id",
    "PullRequest": "pr_entity_id",
    "Repo": "repo_entity_id",
    "Commit": "commit_entity_id",
    "IssueComment": "issuecomment_entity_id",
    "PullRequestReview": "prreview_entity_id",
    "PullRequestReviewComment": "prreviewcomment_entity_id",
    "Actor": "actor_entity_id",
}
ARTIFACT_LABELS = set(ENTITY_SLOT_BY_LABEL) - {"Repo", "Actor"}


@dataclass
class IRField:
    value: Any
    provenance: str
    confidence: float = 1.0
    bounded: bool = True


@dataclass
class ControlledQueryIR:
    request_id: str
    nl_query: str
    entity_mentions: list[dict[str, Any]] = field(default_factory=list)
    aligned_entities: list[dict[str, Any]] = field(default_factory=list)
    relation_semantics: list[dict[str, Any]] = field(default_factory=list)
    repo_scope: dict[str, Any] | None = None
    time_range: dict[str, Any] | None = None
    projection: dict[str, Any] = field(default_factory=dict)
    aggregation: list[dict[str, Any]] = field(default_factory=list)
    sort: list[dict[str, Any]] = field(default_factory=list)
    limit: int | None = None
    explicit_limit: int | None = None
    source_entity: dict[str, Any] | None = None
    target_labels: list[str] = field(default_factory=list)
    intent_key: str = "unknown"
    output_entity_or_property: dict[str, Any] = field(default_factory=dict)
    provenance: dict[str, list[str]] = field(default_factory=dict)
    parser_confidence: float = 0.0
    bounded_status: str = "UNRESOLVED"
    abstention_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class IndependentTemplate:
    template_id: str
    family: str
    intent: str
    skeleton: str
    required_slots: list[dict[str, Any]]
    constraints: dict[str, Any]
    property_whitelist: dict[str, Any]
    repo_scope_policy: list[dict[str, Any]]
    selection: dict[str, Any] = field(default_factory=dict)
    default_limit: int | None = None


@dataclass
class IndependentGenerationResult:
    request_id: str
    nl_query: str
    ir: ControlledQueryIR
    template_id: str | None
    rendered_cypher: str | None
    validation: dict[str, Any]
    failure_stage: str | None
    repair: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def load_independent_templates(path: str | Path) -> list[IndependentTemplate]:
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    out: list[IndependentTemplate] = []
    for item in payload.get("templates", []) if isinstance(payload, dict) else []:
        if not isinstance(item, dict):
            continue
        template_id = str(item.get("template_id") or "").strip()
        if not template_id:
            continue
        out.append(
            IndependentTemplate(
                template_id=template_id,
                family=str(item.get("family") or "").strip(),
                intent=str(item.get("intent") or "").strip(),
                skeleton=str(item.get("cypher_skeleton") or "").strip(),
                required_slots=[x for x in item.get("required_slots", []) if isinstance(x, dict)],
                constraints=item.get("constraints", {}) if isinstance(item.get("constraints"), dict) else {},
                property_whitelist=item.get("property_whitelist", {})
                if isinstance(item.get("property_whitelist"), dict)
                else {},
                repo_scope_policy=[x for x in item.get("repo_scope_policy", []) if isinstance(x, dict)],
                selection=item.get("selection", {}) if isinstance(item.get("selection"), dict) else {},
                default_limit=int(item["default_limit"]) if item.get("default_limit") is not None else None,
            )
        )
    return out


def load_independent_schema(path: str | Path) -> StaticSchemaSpec:
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    allowed_props = set(str(x) for x in payload.get("allowed_properties", []))
    props_by_rel = {
        str(k): set(str(v) for v in vals)
        for k, vals in (payload.get("properties_by_relation", {}) or {}).items()
        if isinstance(vals, list)
    }
    for vals in props_by_rel.values():
        allowed_props.update(vals)
    allowed_props.update({"service_rel_type", "event_time", "source_event_time"})
    return StaticSchemaSpec(
        allowed_node_labels=set(payload.get("allowed_node_labels", [])) | {"EVENT_ACTION", "REFERENCE"},
        allowed_relationship_types=set(payload.get("allowed_relationship_types", [])),
        allowed_properties=allowed_props,
        properties_by_relation=props_by_rel,
        direction_constraints=set(payload.get("direction_constraints", [])),
        service_view_candidates=set(payload.get("service_view_candidates", [])),
        placeholders=set(payload.get("placeholder_relation_types", [])),
    )


def _append_provenance(ir: ControlledQueryIR, field_name: str, value: str) -> None:
    ir.provenance.setdefault(field_name, []).append(value)


def _entity_label_for_id(entity_id: str) -> str | None:
    for prefix, label in sorted(LABEL_FROM_PREFIX.items(), key=lambda kv: -len(kv[0])):
        if entity_id.startswith(prefix + "_"):
            return label
    return None


def _extract_entity_mentions(text: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    mentions: list[dict[str, Any]] = []
    aligned: list[dict[str, Any]] = []
    seen: set[tuple[int, int]] = set()
    for label, prefix, pattern in ENTITY_PATTERNS:
        for match in pattern.finditer(text):
            raw = match.group(0).rstrip(".,;!?\")'")
            start = match.start()
            end = start + len(raw)
            if (start, end) in seen:
                continue
            seen.add((start, end))
            mention = {
                "raw_text": raw,
                "normalized_text": raw,
                "hint_type": f"{label}_canonical_id",
                "entity_label": label,
                "span": [start, end],
                "provenance": "canonical_id_from_nl",
            }
            mentions.append(mention)
            aligned.append(
                {
                    "entity_id": raw,
                    "entity_label": label,
                    "provenance": "canonical_id_from_nl",
                    "alignment": "direct_canonical_id",
                    "confidence": 1.0,
                }
            )
    mentions.sort(key=lambda x: x["span"])
    return mentions, aligned


def _add_typed_mention(mentions: list[dict[str, Any]], text: str, label: str, cue: str) -> None:
    lower = text.lower()
    if cue.lower() in lower and not any(m.get("entity_label") == label and m.get("entity_id") for m in mentions):
        mentions.append(
            {
                "raw_text": cue,
                "normalized_text": cue,
                "hint_type": "typed_noun",
                "entity_label": label,
                "span": [lower.find(cue.lower()), lower.find(cue.lower()) + len(cue)],
                "provenance": "regex_mention",
            }
        )


def _infer_target_labels(text: str, aligned: list[dict[str, Any]]) -> list[str]:
    lower = text.lower()
    labels: list[str] = []
    if any(phrase in lower for phrase in ("external links", "external domains", "external resources", "by domain")):
        labels.append("ExternalResource")
    if any(phrase in lower for phrase in ("referenced objects", "which objects", "objects are referenced")):
        labels.append("UnknownObject")
    if "mentioned repos" in lower or "which repos" in lower:
        labels.append("Repo")
    if any(phrase in lower for phrase in ("which actors", "actors commented", "involved actors", "interacted with", "mention actor", "opened")):
        labels.append("Actor")
    if "commits" in lower and "reference" in lower:
        labels.append("Commit")
    if "list prs" in lower or "find prs" in lower or "list pull requests" in lower or "find pull requests" in lower:
        labels.append("PullRequest")
    # A canonical entity with a role-bearing phrase is stronger than a generic noun.
    if not labels and aligned:
        labels.append(str(aligned[0].get("entity_label")))
    return list(dict.fromkeys(x for x in labels if x and x != "None"))


def _infer_intent_key(text: str, relation_semantics: list[str], ir: ControlledQueryIR) -> str:
    lower = text.lower()
    semantic_set = set(relation_semantics)
    if "comprehensive" in lower and "domain" in lower and "involved actors" in lower:
        return "comprehensive_external_actor_aggregation"
    if "count" in lower and "domain" in lower and "references" in lower:
        return "narrow_domain_aggregation"
    if "mentioned repos" in lower and "external links" in lower:
        return "actor_multi_target_reference"
    if "mention actor" in lower and "also link" in lower and ir.repo_scope and "LINKS_TO" in semantic_set:
        return "repo_actor_external_lower_bound"
    if "review comment" in lower and "reference" in lower and "COMMENTED_ON_REVIEW" in semantic_set:
        return "review_reference"
    if "issue" in lower and "comment" in lower and "commit" in lower and "REFERENCES" in semantic_set:
        return "issue_comment_commit"
    if "pr" in lower and "referenced objects" in lower and ir.repo_scope and ir.time_range:
        return "repo_pr_reference_window"
    if "commented on issue" in lower or ("actors" in lower and "commented on issue" in lower):
        return "issue_comment_actor"
    if "opened" in lower and "issue" in lower and "OPENED_BY" in semantic_set:
        return "issue_opened_by"
    if "external links" in lower and ("pull request" in lower or re.search(r"\bpr\b", lower)) and "LINKS_TO" in semantic_set:
        return "typed_reference_external_property"
    if "objects" in lower and "REFERENCES" in semantic_set:
        return "typed_reference_object"
    if "actors" in lower and "issue comment" in lower and "MENTIONS" in semantic_set:
        return "typed_reference_actor"
    if "PullRequest" in ir.target_labels and ir.repo_scope and not semantic_set:
        return "repo_pull_request_filter"
    return "unknown"


def _parse_time(text: str) -> dict[str, Any] | None:
    dates = DATE_PATTERN.findall(text)
    years = YEAR_PATTERN.findall(text)
    lower = text.lower()
    if dates:
        if "after" in lower or "since" in lower:
            return {
                "start": f"{dates[0]}T00:00:00Z",
                "end": None,
                "provenance": "iso_date_from_nl",
                "bounded": True,
            }
        if "before" in lower:
            return {
                "start": None,
                "end": f"{dates[0]}T00:00:00Z",
                "provenance": "iso_date_from_nl",
                "bounded": True,
            }
    if years:
        year = int(years[0])
        if re.search(r"\b(in|within|during)\s+" + re.escape(years[0]), lower) or len(years) == 1:
            return {
                "start": f"{year:04d}-01-01T00:00:00Z",
                "end": f"{year + 1:04d}-01-01T00:00:00Z",
                "provenance": "year_range_from_nl",
                "year": year,
                "bounded": True,
            }
    return None


def parse_nl_to_ir(request_id: str, nl_query: str) -> ControlledQueryIR:
    text = str(nl_query or "").strip()
    lower = text.lower()
    ir = ControlledQueryIR(request_id=request_id, nl_query=text)
    mentions, aligned = _extract_entity_mentions(text)
    for label, cue in [
        ("Issue", "issue"),
        ("PullRequest", "pull request"),
        ("Actor", "actor"),
        ("IssueComment", "issue comment"),
        ("PullRequestReviewComment", "review comment"),
        ("PullRequestReview", "review"),
        ("Commit", "commit"),
        ("ExternalResource", "external"),
        ("UnknownObject", "object"),
        ("Repo", "repo"),
    ]:
        _add_typed_mention(mentions, text, label, cue)
    _add_typed_mention(mentions, text, "PullRequest", "prs")
    ir.entity_mentions = sorted(mentions, key=lambda x: (x.get("span", [0])[0], x.get("entity_label", "")))
    ir.aligned_entities = aligned
    for item in aligned:
        _append_provenance(ir, "aligned_entities", str(item["provenance"]))

    # The current controlled templates expose one actor slot. Abstain when a
    # request names multiple canonical actors instead of silently retaining
    # only the last ID during slot materialization.
    actor_ids = {
        str(item.get("entity_id"))
        for item in aligned
        if item.get("entity_label") == "Actor" and item.get("entity_id")
    }

    relation_semantics: list[str] = []
    if "structurally coupled" in lower or "coupled with" in lower:
        relation_semantics.append("COUPLES_WITH")
    if "resolve" in lower or "resolves" in lower:
        relation_semantics.append("RESOLVES")
    if any(cue in lower for cue in SERVICE_LEXICON["OPENED_BY"]):
        relation_semantics.append("OPENED_BY")
    if "comment" in lower and "review" in lower:
        relation_semantics.append("COMMENTED_ON_REVIEW")
        if "pr " in lower or "pull request" in lower:
            relation_semantics.append("CREATED_IN")
        if "actor" in lower:
            relation_semantics.append("MENTIONS")
    elif "comment" in lower and "issue" in lower:
        relation_semantics.append("COMMENTED_ON_ISSUE")
        if "actor" in lower:
            relation_semantics.append("OPENED_BY")
    if any(cue in lower for cue in SERVICE_LEXICON["LINKS_TO"]):
        relation_semantics.append("LINKS_TO")
    if any(cue in lower for cue in SERVICE_LEXICON["MENTIONS"]):
        relation_semantics.append("MENTIONS")
    if any(cue in lower for cue in SERVICE_LEXICON["REFERENCES"]):
        relation_semantics.append("REFERENCES")
    if ("by domain" in lower or "external" in lower) and "REFERENCES" in relation_semantics:
        relation_semantics = [x for x in relation_semantics if x != "REFERENCES"]
        relation_semantics.append("LINKS_TO")
    if "involved actors" in lower and "LINKS_TO" in relation_semantics:
        relation_semantics.extend(["MENTIONS", "REFERENCES"])
    relation_semantics = list(dict.fromkeys(relation_semantics))
    ir.relation_semantics = [
        {"semantic": value, "provenance": "bounded_semantic_rule", "confidence": 0.92}
        for value in relation_semantics
    ]
    for value in relation_semantics:
        _append_provenance(ir, "relation_semantics", "bounded_semantic_rule")

    repo_entity = next((x for x in aligned if x["entity_label"] == "Repo"), None)
    if repo_entity is None:
        for item in aligned:
            if item["entity_label"] in {"PullRequest", "Issue", "Commit", "IssueComment", "PullRequestReview", "PullRequestReviewComment"}:
                match = re.match(r"(?:PRRC|PRR|IC|PR|I|C)_(\d+)[#@]", item["entity_id"])
                if match:
                    repo_entity = {
                        "entity_id": f"R_{match.group(1)}",
                        "entity_label": "Repo",
                        "provenance": "repo_scope_from_entity_prefix",
                    }
                    aligned.append(repo_entity)
                    _append_provenance(ir, "repo_scope", "derived_repo_scope")
                    break
    if repo_entity:
        ir.repo_scope = {
            "repo_entity_id": repo_entity["entity_id"],
            "provenance": "canonical_id_from_nl" if "canonical_id" in str(repo_entity.get("provenance")) else "derived_repo_scope",
            "labels": sorted({x.get("entity_label") for x in ir.entity_mentions if x.get("entity_label")}),
        }

    time_range = _parse_time(text)
    if time_range:
        ir.time_range = time_range
        _append_provenance(ir, "time_range", str(time_range["provenance"]))

    limit_match = LIMIT_PATTERN.search(text)
    ir.explicit_limit = int(limit_match.group(1)) if limit_match else None
    ir.limit = ir.explicit_limit
    _append_provenance(ir, "limit", "explicit_limit_from_nl" if limit_match else "template_contract_default")

    if any(word in lower for word in ["count", "group by", "by domain", "latest interaction"]):
        ir.aggregation = [{"function": "count", "field": "*", "provenance": "bounded_semantic_rule"}]
        _append_provenance(ir, "aggregation", "bounded_semantic_rule")
    if "domain" in lower:
        ir.projection["property"] = "url_domain_etld1"
        _append_provenance(ir, "projection", "bounded_semantic_rule")
    if any(word in lower for word in ["latest", "sorted by time", "sort by", "over time"]):
        ir.sort = [{"field": "source_event_time", "order": "desc", "provenance": "bounded_semantic_rule"}]
        _append_provenance(ir, "sort", "bounded_semantic_rule")
    elif "ascending" in lower:
        ir.sort = [{"field": "source_event_time", "order": "asc", "provenance": "bounded_semantic_rule"}]
        _append_provenance(ir, "sort", "bounded_semantic_rule")

    ir.target_labels = _infer_target_labels(text, aligned)
    source_entity = None
    canonical = [x for x in aligned if x.get("entity_id")]
    if canonical:
        if canonical[0].get("entity_label") == "Repo" and len(canonical) > 1:
            source_entity = next((x for x in canonical if x.get("entity_label") != "Repo"), None)
        else:
            source_entity = canonical[0]
    ir.source_entity = dict(source_entity) if source_entity else None
    ir.intent_key = _infer_intent_key(text, relation_semantics, ir)
    ir.output_entity_or_property = {
        "target_labels": ir.target_labels,
        "external_resource": "ExternalResource" in ir.target_labels,
        "projected_properties": [ir.projection["property"]] if ir.projection.get("property") else [],
    }
    ir.parser_confidence = 0.9 if aligned and relation_semantics else 0.65 if aligned else 0.35
    if "COUPLES_WITH" in relation_semantics or "RESOLVES" in relation_semantics:
        ir.bounded_status = "ABSTAIN_PLACEHOLDER"
        ir.abstention_reason = "placeholder relation is outside the executable native contract"
    elif len(actor_ids) > 1:
        ir.bounded_status = "ABSTAIN_MULTIPLE_ACTOR_IDS"
        ir.abstention_reason = "current actor-targeted contract supports one canonical actor slot"
    elif not aligned:
        ir.bounded_status = "ABSTAIN_UNALIGNED_ENTITY"
        ir.abstention_reason = "no canonical or alignable entity mention"
    else:
        ir.bounded_status = "BOUNDED_PARSED"
    return ir


def _template_slot_names(template: IndependentTemplate) -> set[str]:
    required = {
        str(item.get("name"))
        for item in template.required_slots
        if item.get("name")
    }
    return required | set(TOKEN_PATTERN.findall(template.skeleton))


def _repo_number(entity_id: str) -> str | None:
    match = re.match(r"(?:PRRC|PRR|IC|PR|I|C|R)_(\d+)(?:[#@]|$)", entity_id)
    return match.group(1) if match else None


def _entity_constraint_detail(item: dict[str, Any], *, status: str, reason: str | None = None) -> dict[str, Any]:
    detail = {
        "entity_id": str(item.get("entity_id")),
        "entity_label": str(item.get("entity_label")),
        "provenance": str(item.get("provenance")),
        "status": status,
    }
    if reason:
        detail["reason"] = reason
    return detail


def audit_entity_constraint_coverage(ir: ControlledQueryIR, template: IndependentTemplate) -> dict[str, Any]:
    """Audit direct canonical entity constraints against one template contract.

    Derived repository prefixes are implementation helpers, while only direct
    canonical IDs from NL are user constraints. A candidate is admissible only
    when every direct constraint is consumed or directionally entailed.
    """

    slot_names = _template_slot_names(template)
    direct: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in ir.aligned_entities:
        if item.get("provenance") != "canonical_id_from_nl" or not item.get("entity_id"):
            continue
        key = (str(item.get("entity_label")), str(item.get("entity_id")))
        if key in seen:
            continue
        seen.add(key)
        direct.append(item)

    consumed: list[dict[str, Any]] = []
    entailed: list[dict[str, Any]] = []
    unconsumed: list[dict[str, Any]] = []
    conflicting: list[dict[str, Any]] = []

    # A singular entity slot cannot silently choose one of multiple distinct
    # direct IDs carrying the same label.
    direct_by_label: dict[str, list[dict[str, Any]]] = {}
    for item in direct:
        direct_by_label.setdefault(str(item.get("entity_label")), []).append(item)

    source_id = str(ir.source_entity.get("entity_id")) if ir.source_entity and ir.source_entity.get("entity_id") else None
    repo_slot_consumed = bool(
        "repo_entity_id" in slot_names
        or any(name.endswith("_base_prefix") for name in slot_names)
        or bool(template.selection.get("requires_repo_scope"))
    )
    consumed_artifact_repo_numbers: set[str] = set()

    for label, items in direct_by_label.items():
        slot = ENTITY_SLOT_BY_LABEL.get(label)
        if label == "Repo":
            if "source_entity_id" in slot_names and len(items) == 1 and str(items[0].get("entity_id")) == source_id:
                consumed.append(
                    _entity_constraint_detail(items[0], status="consumed", reason="source_entity_id contract")
                )
                continue
            if repo_slot_consumed:
                if len(items) > 1:
                    conflicting.extend(
                        _entity_constraint_detail(item, status="conflicting", reason="multiple direct Repo IDs for one scope contract")
                        for item in items
                    )
                else:
                    consumed.append(
                        _entity_constraint_detail(items[0], status="consumed", reason="repo scope contract")
                    )
            else:
                unconsumed.extend(
                    _entity_constraint_detail(item, status="unconsumed", reason="template has no repo scope slot")
                    for item in items
                )
            continue

        if slot and slot in slot_names:
            if len(items) > 1:
                conflicting.extend(
                    _entity_constraint_detail(item, status="conflicting", reason=f"multiple direct {label} IDs for singular slot {slot}")
                    for item in items
                )
            else:
                consumed.append(
                    _entity_constraint_detail(items[0], status="consumed", reason=f"slot {slot}")
                )
                if label in ARTIFACT_LABELS:
                    repo_number = _repo_number(str(items[0].get("entity_id")))
                    if repo_number:
                        consumed_artifact_repo_numbers.add(repo_number)
            continue

        if "source_entity_id" in slot_names and len(items) == 1 and str(items[0].get("entity_id")) == source_id:
            consumed.append(
                _entity_constraint_detail(items[0], status="consumed", reason="source_entity_id contract")
            )
            if label in ARTIFACT_LABELS:
                repo_number = _repo_number(str(items[0].get("entity_id")))
                if repo_number:
                    consumed_artifact_repo_numbers.add(repo_number)
            continue

        unconsumed.extend(
            _entity_constraint_detail(item, status="unconsumed", reason="no compatible singular entity slot")
            for item in items
        )

    # A consumed concrete artifact can entail an explicitly named repository
    # scope in the same repository. The implication is intentionally one-way.
    for item in direct_by_label.get("Repo", []):
        repo_number = _repo_number(str(item.get("entity_id")))
        if repo_number and repo_number in consumed_artifact_repo_numbers:
            matching = next((x for x in consumed if x.get("entity_id") == item.get("entity_id")), None)
            if matching:
                consumed.remove(matching)
            existing = next((x for x in unconsumed if x.get("entity_id") == item.get("entity_id")), None)
            if existing:
                unconsumed.remove(existing)
            entailed.append(
                _entity_constraint_detail(item, status="entailed", reason="matching consumed artifact implies repository scope")
            )

    # Explicit artifact/repository IDs from different repositories conflict
    # whenever both are direct constraints in one request.
    direct_repo_numbers = {
        _repo_number(str(item.get("entity_id")))
        for item in direct_by_label.get("Repo", [])
        if _repo_number(str(item.get("entity_id")))
    }
    artifact_repo_numbers = {
        _repo_number(str(item.get("entity_id")))
        for label, items in direct_by_label.items()
        if label in ARTIFACT_LABELS
        for item in items
        if _repo_number(str(item.get("entity_id")))
    }
    if direct_repo_numbers and artifact_repo_numbers and not direct_repo_numbers.intersection(artifact_repo_numbers):
        conflicting.extend(
            _entity_constraint_detail(item, status="conflicting", reason="explicit artifact and repo IDs identify different repositories")
            for item in direct
            if item.get("entity_label") == "Repo" or item.get("entity_label") in ARTIFACT_LABELS
        )

    classified_direct: list[dict[str, Any]] = []
    for item in direct:
        entity_id = str(item.get("entity_id"))
        classification = "unconsumed"
        for category, entries in (
            ("conflicting", conflicting),
            ("unconsumed", unconsumed),
            ("entailed", entailed),
            ("consumed", consumed),
        ):
            if any(str(entry.get("entity_id")) == entity_id for entry in entries):
                classification = category
                break
        classified_direct.append(
            _entity_constraint_detail(item, status=classification, reason="candidate-level classification")
        )

    accepted = not unconsumed and not conflicting
    return {
        "accepted": accepted,
        "direct_constraints": classified_direct,
        "consumed": consumed,
        "entailed": entailed,
        "unconsumed": unconsumed,
        "conflicting": conflicting,
    }


def _template_score(template: IndependentTemplate, ir: ControlledQueryIR) -> tuple[int, list[str]]:
    coverage = audit_entity_constraint_coverage(ir, template)
    if not coverage["accepted"]:
        reasons: list[str] = []
        if coverage["unconsumed"]:
            reasons.append("unconsumed direct entity constraint")
        if coverage["conflicting"]:
            reasons.append("conflicting direct entity constraint")
        return -100, reasons or ["entity constraint coverage failed"]
    if template.selection:
        selection = template.selection
        expected_intent = str(selection.get("intent_key") or "")
        if expected_intent and expected_intent != ir.intent_key:
            return -100, [f"intent mismatch: expected {expected_intent}, observed {ir.intent_key}"]
        if selection.get("requires_repo_scope") and not ir.repo_scope:
            return -100, ["missing repo scope"]
        if selection.get("requires_time_start") and not (ir.time_range and ir.time_range.get("start")):
            return -100, ["missing time start"]
        if selection.get("requires_time_end") and not (ir.time_range and ir.time_range.get("end")):
            return -100, ["missing time end"]
        if selection.get("requires_aggregation") and not ir.aggregation:
            return -100, ["missing aggregation intent"]
        required_targets = {str(x) for x in selection.get("target_labels", [])}
        if required_targets and not required_targets.issubset(set(ir.target_labels)):
            return -100, ["target label contract mismatch"]
        return 100 + len(selection), ["independent semantic contract match"]

    semantics = {x["semantic"] for x in ir.relation_semantics}
    labels = {x.get("entity_label") for x in ir.entity_mentions}
    required = {str(x.get("name")) for x in template.required_slots}
    score = 0
    reasons: list[str] = []
    intent = template.intent.lower()
    if template.family == "EntityFilter" and "PullRequest" in labels and ir.repo_scope and not semantics and not ir.aggregation:
        score += 8
        reasons.append("repo-scoped pull-request entity filter")
    if template.family == "OneHopEA" and "OPENED_BY" in semantics and "Issue" in labels and "comment" not in ir.nl_query.lower():
        score += 8
        reasons.append("issue opened-by event action")
    if template.family == "Aggregation" and ir.aggregation and ir.repo_scope:
        score += 8
        reasons.append("repo-scoped aggregation")
    if "review comment" in intent and "COMMENTED_ON_REVIEW" in semantics and template.family == "TwoHopComposite":
        score += 9
        reasons.append("review-comment composite")
    if "issue comment" in intent and "actor" in intent and "COMMENTED_ON_ISSUE" in semantics and "REFERENCES" not in semantics and template.family == "TwoHopComposite":
        score += 8
        reasons.append("issue-comment actor composite")
    if "issue comment" in intent and "commit" in intent and "COMMENTED_ON_ISSUE" in semantics and "REFERENCES" in semantics and template.family == "TwoHopComposite":
        score += 8
        reasons.append("issue-comment reference composite")
    if "repo-scoped pr reference window" in intent and ir.repo_scope and ir.time_range and "REFERENCES" in semantics and "COMMENTED_ON_REVIEW" not in semantics:
        score += 9
        reasons.append("repo-scoped reference time window")
    if template.family == "OneHopRef" and semantics.intersection({"LINKS_TO", "MENTIONS", "REFERENCES"}):
        score += 5
        reasons.append("reference semantic")
    if template.family == "OneHopRef" and ir.repo_scope and ir.time_range and {"LINKS_TO", "MENTIONS"}.issubset(semantics):
        return (-100, ["multi-semantic repo window requires a bounded composite contract"])
    if "pr_base_prefix" in required and not ir.repo_scope:
        return (-100, ["missing repo scope"])
    if "time_start" in required and not ir.time_range:
        return (-100, ["missing time range"])
    if "time_end" in required and not (ir.time_range and ir.time_range.get("end")):
        return (-100, ["missing bounded time end"])
    if "issue_entity_id" in required and not any(x.get("entity_label") == "Issue" and x.get("entity_id") for x in ir.aligned_entities):
        return (-100, ["missing issue entity"])
    if "pr_entity_id" in required and not any(x.get("entity_label") == "PullRequest" and x.get("entity_id") for x in ir.aligned_entities):
        return (-100, ["missing pull-request entity"])
    if "source_entity_id" in required and not ir.aligned_entities:
        return (-100, ["missing source entity"])
    if not score:
        if template.family == "OneHopRef" and semantics:
            score = 2
            reasons.append("generic bounded reference template")
        else:
            return (-100, ["contract mismatch"])
    if "service" in intent:
        score += 1
    return score, reasons


def select_template(ir: ControlledQueryIR, templates: list[IndependentTemplate]) -> tuple[IndependentTemplate | None, dict[str, Any]]:
    if ir.bounded_status.startswith("ABSTAIN"):
        return None, {
            "status": "abstain",
            "reason": ir.abstention_reason,
            "candidate_entity_constraint_coverage": {
                template.template_id: audit_entity_constraint_coverage(ir, template)
                for template in templates
            },
        }
    scored: list[tuple[int, IndependentTemplate, list[str], dict[str, Any]]] = []
    coverage_by_template: dict[str, dict[str, Any]] = {}
    for template in templates:
        coverage = audit_entity_constraint_coverage(ir, template)
        coverage_by_template[template.template_id] = coverage
        score, reasons = _template_score(template, ir)
        if score >= 0:
            scored.append((score, template, reasons, coverage))
    scored.sort(key=lambda item: (-item[0], item[1].family, item[1].template_id))
    if not scored:
        has_conflict = any(item["conflicting"] for item in coverage_by_template.values())
        has_unconsumed = any(item["unconsumed"] for item in coverage_by_template.values())
        reason = (
            "conflicting direct entity scope"
            if has_conflict
            else "unconsumed direct entity constraint"
            if has_unconsumed
            else "no compatible template contract"
        )
        return None, {
            "status": "abstain",
            "reason": reason,
            "candidate_entity_constraint_coverage": coverage_by_template,
        }
    best_score = scored[0][0]
    tied = [x for x in scored if x[0] == best_score]
    if len(tied) > 1 and best_score <= 2:
        return None, {
            "status": "abstain",
            "reason": "ambiguous low-confidence template contract",
            "candidates": [x[1].template_id for x in tied],
            "candidate_entity_constraint_coverage": coverage_by_template,
        }
    chosen = tied[0]
    return chosen[1], {
        "status": "selected",
        "score": chosen[0],
        "reasons": chosen[2],
        "candidates": [{"template_id": x[1].template_id, "score": x[0]} for x in scored],
        "entity_constraint_coverage": chosen[3],
        "candidate_entity_constraint_coverage": coverage_by_template,
    }


def _slot_values(ir: ControlledQueryIR, template: IndependentTemplate) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for item in ir.aligned_entities:
        label = item.get("entity_label")
        entity_id = item.get("entity_id")
        if not entity_id:
            continue
        if label == "Issue":
            values["issue_entity_id"] = entity_id
        elif label == "PullRequest":
            values["pr_entity_id"] = entity_id
        elif label == "Repo":
            values["repo_entity_id"] = entity_id
        elif label == "Commit":
            values["commit_entity_id"] = entity_id
        elif label == "IssueComment":
            values["issuecomment_entity_id"] = entity_id
        elif label == "PullRequestReview":
            values["prreview_entity_id"] = entity_id
        elif label == "PullRequestReviewComment":
            values["prreviewcomment_entity_id"] = entity_id
        elif label == "Actor":
            values["actor_entity_id"] = entity_id
        if "source_entity_id" not in values and label in {"PullRequest", "IssueComment", "PullRequest", "Actor", "Issue"}:
            values["source_entity_id"] = entity_id
    if ir.time_range:
        if ir.time_range.get("start"):
            values["time_start"] = ir.time_range["start"]
        if ir.time_range.get("end"):
            values["time_end"] = ir.time_range["end"]
    semantics = [x["semantic"] for x in ir.relation_semantics]
    ref = next((x for x in ["LINKS_TO", "MENTIONS", "REFERENCES"] if x in semantics), None)
    if ref:
        values["ref_semantic"] = ref
    if ir.repo_scope:
        labels = list(ir.repo_scope.get("labels", []))
        scope = build_repo_scope_prefixes(ir.repo_scope["repo_entity_id"], labels)
        prefixes = scope.get("base_prefixes", {})
        values.update(
            {
                "pr_base_prefix": prefixes.get("PullRequest"),
                "issue_base_prefix": prefixes.get("Issue"),
                "commit_base_prefix": prefixes.get("Commit"),
            }
        )
    return {k: v for k, v in values.items() if v is not None}


def _render(skeleton: str, values: dict[str, Any], limit: int | None) -> tuple[str | None, list[str]]:
    missing: list[str] = []

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in values:
            missing.append(key)
            return match.group(0)
        value = str(values[key]).replace("\\", "\\\\").replace("'", "\\'")
        return f"'{value}'"

    rendered = TOKEN_PATTERN.sub(replace, skeleton)
    if missing:
        return None, sorted(set(missing))
    if limit is not None:
        rendered = re.sub(r"\bLIMIT\s+\d+\b", f"LIMIT {int(limit)}", rendered, flags=re.IGNORECASE)
    return " ".join(rendered.split()), []


def generate_independent(request_id: str, nl_query: str, templates: list[IndependentTemplate], schema: StaticSchemaSpec) -> IndependentGenerationResult:
    ir = parse_nl_to_ir(request_id, nl_query)
    template, selection_trace = select_template(ir, templates)
    if template is None:
        return IndependentGenerationResult(
            request_id=request_id,
            nl_query=nl_query,
            ir=ir,
            template_id=None,
            rendered_cypher=None,
            validation={"valid": False, "errors": [], "selection": selection_trace},
            failure_stage="template_selection_or_abstention",
        )
    values = _slot_values(ir, template)
    effective_limit = ir.explicit_limit if ir.explicit_limit is not None else template.default_limit
    rendered, missing = _render(template.skeleton, values, effective_limit)
    if rendered is None:
        return IndependentGenerationResult(
            request_id=request_id,
            nl_query=nl_query,
            ir=ir,
            template_id=template.template_id,
            rendered_cypher=None,
            validation={"valid": False, "errors": [{"code": "MISSING_REQUIRED_SLOT", "detail": {"slots": missing}}], "selection": selection_trace},
            failure_stage="slot_render",
        )
    result = validate_cypher_static(rendered, schema)
    errors = [{"code": e.code, "message": e.message, "detail": e.detail} for e in result.errors]
    return IndependentGenerationResult(
        request_id=request_id,
        nl_query=nl_query,
        ir=ir,
        template_id=template.template_id,
        rendered_cypher=rendered,
        validation={"valid": result.valid, "errors": errors, "selection": selection_trace},
        failure_stage=None if result.valid else "static_validation",
    )


def run_independent_requests(
    requests: list[dict[str, str]],
    templates_path: str | Path,
    schema_path: str | Path,
) -> list[IndependentGenerationResult]:
    templates = load_independent_templates(templates_path)
    schema = load_independent_schema(schema_path)
    results: list[IndependentGenerationResult] = []
    for request in requests:
        request_id = str(request.get("id") or "").strip()
        nl_query = str(request.get("nl_query") or "")
        results.append(generate_independent(request_id, nl_query, templates, schema))
    return results


def write_independent_traces(path: str | Path, results: list[IndependentGenerationResult]) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as handle:
        for result in results:
            handle.write(json.dumps(result.to_dict(), ensure_ascii=False) + "\n")
