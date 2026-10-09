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
    ("PullRequestReviewComment", "PRRC", re.compile(r"(?<![A-Za-z0-9_])PRRC_\d+#[^\s,.;!?)[\]}]+")),
    ("PullRequestReview", "PRR", re.compile(r"(?<![A-Za-z0-9_])PRR_\d+#[^\s,.;!?)[\]}]+")),
    ("IssueComment", "IC", re.compile(r"(?<![A-Za-z0-9_])IC_\d+#[^\s,.;!?)[\]}]+")),
    ("PullRequest", "PR", re.compile(r"(?<![A-Za-z0-9_])PR_\d+#[^\s,.;!?)[\]}]+")),
    ("Issue", "I", re.compile(r"(?<![A-Za-z0-9_])I_\d+#[^\s,.;!?)[\]}]+")),
    ("Commit", "C", re.compile(r"(?<![A-Za-z0-9_])C_\d+[@#][^\s,.;!?)[\]}]+")),
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

TYPED_PREFIX_LABELS = {"PR": "PullRequest", "I": "Issue"}
TYPED_PREFIX_PATTERN = re.compile(
    r"(?<![A-Za-z0-9_])(?P<prefix>PR|I)_(?P<number>\d+)(?![A-Za-z0-9_#@])",
    re.IGNORECASE,
)
_PREFIX_OPERATOR_SUFFIX = re.compile(
    r"(?:\bprefix|\bstarts?\s+with|\bbegins?\s+with|"
    r"\bstarting\s+with|\bbeginning\s+with|"
    r"\b(?:ids?|identifiers?)\s+(?:that\s+)?(?:start|begin)(?:s|ing|ning)?\s+with|"
    r"\bwhose\s+(?:entity\s+)?(?:ids?|identifiers?)\s+"
    r"(?:start|begin)(?:s|ing|ning)?\s+with|"
    r"\bscope(?:d)?\s+to\s+(?:issues?|pull\s+requests?)?\s*prefix)\s*$",
    re.IGNORECASE,
)
_PREFIX_TOKEN_SUFFIX = re.compile(r"^\s+prefix\b", re.IGNORECASE)
_TYPED_PREFIX_NEGATION_CUE = r"(?:excluding|exclude|omitting|omit|without|except(?:\s+for)?|but\s+not)"
_POST_TOKEN_NEGATION_CUE_SUFFIX = re.compile(
    rf"\b{_TYPED_PREFIX_NEGATION_CUE}\s+"
    r"(?:(?:the|this|that|these|those)\s+)?$",
    re.IGNORECASE,
)
_ENDS_WITH_OPERATOR_SUFFIX = re.compile(
    r"\b(?:whose\s+)?(?:entity\s+)?(?:ids?|identifiers?)\s+"
    r"(?:that\s+)?end(?:s|ing)?\s+with\s*$",
    re.IGNORECASE,
)
_CONTAINS_OPERATOR_SUFFIX = re.compile(
    r"\b(?:whose\s+)?(?:entity\s+)?(?:ids?|identifiers?)\s+"
    r"(?:that\s+)?contain(?:s|ing)?\s*$",
    re.IGNORECASE,
)
_UNSPECIFIED_TYPED_SCOPE_SUFFIX = re.compile(
    r"\b(?:whose\s+)?(?:entity\s+)?(?:ids?|identifiers?)\s+(?:are|equal\s+to|match)\s*$",
    re.IGNORECASE,
)

ENTITY_NOUN_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("PullRequestReviewComment", re.compile(r"\bpull\s+request\s+review\s+comments?\b|\breview\s+comments?\b", re.I)),
    ("PullRequestReview", re.compile(r"\bpull\s+request\s+reviews?\b|\breviews?\b", re.I)),
    ("IssueComment", re.compile(r"\bissue\s+comments?\b", re.I)),
    ("ExternalResource", re.compile(r"\b(?:external[- ]|outside\s+|linked\s+)?resources?\b", re.I)),
    ("UnknownObject", re.compile(r"\bunknown(?:[- ,]+(?:type|untyped))?\s+objects?\b|\buntyped(?:,?\s+unknown)?\s+objects?\b|\breferenced\s+objects?\b|\bobjects?\b", re.I)),
    ("PullRequest", re.compile(r"\bpull\s+requests?\b|\bprs?\b", re.I)),
    ("Issue", re.compile(r"\bissues?\b", re.I)),
    ("Actor", re.compile(r"\bactors?\b|\busers?\b|\bpeople\b", re.I)),
    ("Commit", re.compile(r"\bcommits?\b", re.I)),
    ("Repo", re.compile(r"\brepositories\b|\brepos?\b", re.I)),
]

SERVICE_LEXICON = {
    "OPENED_BY": ("opened", "open", "opened by", "owner"),
    "REFERENCES": ("referenced", "references", "reference", "referencing", "objects are referenced"),
    "MENTIONS": ("mentioned", "mentions", "mention"),
    "LINKS_TO": ("external link", "external links", "links to", "linked to", "linked resource", "link to"),
}

TOKEN_PATTERN = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")
DATE_PATTERN = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")
YEAR_PATTERN = re.compile(r"\b(20\d{2}|2100)\b")
LIMIT_PATTERN = re.compile(r"\b(?:top|limit)\s+(\d+)\b", re.IGNORECASE)
UNNORMALIZED_LIMIT_PATTERNS = [
    re.compile(r"\b(?:up\s+to|at\s+most|no\s+more\s+than|limited\s+to|capped\s+at)\s+(\d+)\s*(?:rows?|results?|entries?|domains?)\b", re.I),
    re.compile(r"\b(\d+)\s*(?:rows?|results?|entries?|domains?)\s+(?:max(?:imum)?|at\s+most)\b", re.I),
    re.compile(r"\b(?:limit\s+to|stop\s+at)\s+(\d+)\s*(?:rows?|results?|entries?)?\b", re.I),
]

UNSUPPORTED_EXPLICIT_CONSTRAINT_REASON = "UNSUPPORTED_EXPLICIT_NL_CONSTRAINT"
_UNSUPPORTED_EXCLUSION_PATTERN = re.compile(
    r"\b(?:other\s+than|apart\s+from)\s+(?:(?:the|these|those)\s+)?"
    r"(?:(?:issues?|pull\s+requests?|prs?)\s+)?(?:those\s+)?whose\s+"
    r"(?:entity\s+)?(?:ids?|identifiers?)\s+(?:that\s+)?"
    r"(?:start|begin)(?:s|ing|ning)?\s+with\s+(?P<prefix>(?:I|PR)_\d+)\b"
    r"|\bunless\s+(?:(?:the|those|their)\s+)?"
    r"(?:(?:issues?|pull\s+requests?|prs?)\s+)?(?:whose\s+)?"
    r"(?:entity\s+)?(?:ids?|identifiers?)\s+(?:that\s+)?"
    r"(?:start|begin)(?:s|ing|ning)?\s+with\s+(?P<prefix_unless>(?:I|PR)_\d+)\b"
    r"|\bbut\s+those\s+whose\s+(?:entity\s+)?(?:ids?|identifiers?)\s+"
    r"(?:that\s+)?(?:start|begin)(?:s|ing|ning)?\s+with\s+(?P<prefix_but>(?:I|PR)_\d+)\b",
    re.IGNORECASE,
)
_TRAILING_TYPED_EXCLUSION_PATTERN = re.compile(
    r"(?:,?\s+)"
    r"(?P<cue>but\s+not|except(?:\s+for)?|excluding|exclude|omitting|omit|without)\s+"
    r"(?P<prefix>(?:I|PR)_\d+)\b",
    re.IGNORECASE,
)
_UNSUPPORTED_POSTFIX_UNIQUENESS_PATTERN = re.compile(
    r"\b(?:without\s+duplicates|with\s+no\s+duplicates|no\s+duplicate\s+results?)\b",
    re.IGNORECASE,
)
_UNSUPPORTED_CARDINALITY_PATTERN = re.compile(
    r"\b(?:at\s+most|no\s+more\s+than|up\s+to|limited\s+to|capped\s+at)\s+"
    r"(?P<limit>\d+)\s+(?P<noun>rows?|results?|entries?|domains?|"
    r"people|persons?|actors?|users?|issues?|pull\s+requests?|prs?|"
    r"comments?|reviews?|commits?|repos?|repositories|resources?|objects?)\b"
    r"(?:\s+(?:IDs?|identifiers?))?"
    r"|\b(?P<reverse_limit>\d+)\s+(?P<reverse_noun>rows?|results?|entries?|domains?|"
    r"people|persons?|actors?|users?|issues?|pull\s+requests?|prs?|"
    r"comments?|reviews?|commits?|repos?|repositories|resources?|objects?)\s+"
    r"(?:max(?:imum)?|at\s+most)\b(?:\s+(?:IDs?|identifiers?))?",
    re.IGNORECASE,
)

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
class EntityScope:
    label: str
    property: str
    operator: str
    value: str
    provenance: str
    source_span: list[int]


@dataclass
class ProjectionItem:
    role: str
    label: str | None
    property: str
    distinct: bool = False
    nullable: bool = False
    alias: str | None = None
    provenance: str = "bounded_projection_rule"
    source_span: list[int] = field(default_factory=list)
    function: str | None = None
    distinct_source_spans: list[list[int]] = field(default_factory=list)


class ScopeSlotConflictError(ValueError):
    """Raised when distinct explicit scope values target one singular slot."""

    reason_code = "MULTIPLE_DISTINCT_VALUES_FOR_SINGULAR_SCOPE_SLOT"

    def __init__(self, slot: str, values: list[tuple[str, str, str, str]]) -> None:
        self.slot = slot
        self.values = values
        super().__init__(f"{self.reason_code}: {slot} cannot consume {len(values)} distinct values")


class RepoScopeTypedPrefixConflictError(ValueError):
    """Raised when an explicit typed prefix disagrees with repo context."""

    reason_code = "REPO_SCOPE_TYPED_PREFIX_CONFLICT"

    def __init__(
        self,
        *,
        repo_entity_id: str,
        scope_label: str,
        expected_repo_prefix: str,
        explicit_scope_value: str,
        scope_operator: str,
        source_span: list[int],
    ) -> None:
        self.repo_entity_id = repo_entity_id
        self.scope_label = scope_label
        self.expected_repo_prefix = expected_repo_prefix
        self.explicit_scope_value = explicit_scope_value
        self.scope_operator = scope_operator
        self.source_span = source_span
        super().__init__(
            f"{self.reason_code}: {scope_label} {explicit_scope_value} conflicts "
            f"with {repo_entity_id} ({expected_repo_prefix})"
        )


@dataclass
class ControlledQueryIR:
    request_id: str
    nl_query: str
    entity_mentions: list[dict[str, Any]] = field(default_factory=list)
    aligned_entities: list[dict[str, Any]] = field(default_factory=list)
    entity_scopes: list[EntityScope] = field(default_factory=list)
    relation_semantics: list[dict[str, Any]] = field(default_factory=list)
    repo_scope: dict[str, Any] | None = None
    time_range: dict[str, Any] | None = None
    projection: dict[str, Any] = field(default_factory=dict)
    projection_items: list[ProjectionItem] = field(default_factory=list)
    projection_distinct: bool = False
    aggregation: list[dict[str, Any]] = field(default_factory=list)
    sort: list[dict[str, Any]] = field(default_factory=list)
    limit: int | None = None
    explicit_limit: int | None = None
    unnormalized_limit_values: list[int] = field(default_factory=list)
    source_entity: dict[str, Any] | None = None
    target_labels: list[str] = field(default_factory=list)
    target_label_provenance: dict[str, str] = field(default_factory=dict)
    intent_key: str = "unknown"
    output_entity_or_property: dict[str, Any] = field(default_factory=dict)
    provenance: dict[str, list[str]] = field(default_factory=dict)
    parser_confidence: float = 0.0
    bounded_status: str = "UNRESOLVED"
    abstention_reason: str | None = None
    unsupported_explicit_constraints: list[dict[str, Any]] = field(default_factory=list)

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
    scope_slots: list[dict[str, Any]] = field(default_factory=list)
    projection_contract: list[dict[str, Any]] = field(default_factory=list)
    projection_options: dict[str, Any] = field(default_factory=dict)


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


def load_independent_templates(
    path: str | Path, _active_paths: tuple[Path, ...] = ()
) -> list[IndependentTemplate]:
    template_path = Path(path)
    resolved_path = template_path.resolve()
    if resolved_path in _active_paths:
        chain = " -> ".join(str(item) for item in (*_active_paths, resolved_path))
        raise ValueError(f"template dependency cycle detected: {chain}")
    active_paths = (*_active_paths, resolved_path)
    payload = yaml.safe_load(template_path.read_text(encoding="utf-8")) or {}
    extends = payload.get("extends") if isinstance(payload, dict) else None
    if isinstance(extends, list):
        raise ValueError("MULTIPLE_TEMPLATE_BASES_NOT_SUPPORTED")
    if extends is not None and not isinstance(extends, str):
        raise ValueError("INVALID_TEMPLATE_EXTENDS: expected a single path string")
    if isinstance(extends, str) and not extends.strip():
        raise ValueError("INVALID_TEMPLATE_EXTENDS: expected a non-empty path string")
    if isinstance(payload, dict) and extends:
        base_path = template_path.parent / extends
        base_templates = load_independent_templates(base_path, active_paths)
        contracts = payload.get("template_contracts", {})
        contracts = contracts if isinstance(contracts, dict) else {}
        extended: list[IndependentTemplate] = []
        for template in base_templates:
            contract = contracts.get(template.template_id, {})
            contract = contract if isinstance(contract, dict) else {}
            selection_override = contract.get("selection")
            selection = {
                **template.selection,
                **(selection_override if isinstance(selection_override, dict) else {}),
            }
            projection_options_override = contract.get("projection_options")
            projection_options = {
                **template.projection_options,
                **(
                    projection_options_override
                    if isinstance(projection_options_override, dict)
                    else {}
                ),
            }
            scope_slots = (
                [x for x in contract.get("scope_slots", []) if isinstance(x, dict)]
                if "scope_slots" in contract
                else template.scope_slots
            )
            projection_contract = (
                [x for x in contract.get("projection_contract", []) if isinstance(x, dict)]
                if "projection_contract" in contract
                else template.projection_contract
            )
            extended.append(
                IndependentTemplate(
                    **{
                        **asdict(template),
                        "selection": selection,
                        "scope_slots": scope_slots,
                        "projection_contract": projection_contract,
                        "projection_options": projection_options,
                    }
                )
            )
        for item in payload.get("templates", []) if isinstance(payload.get("templates"), list) else []:
            if not isinstance(item, dict) or not str(item.get("template_id") or "").strip():
                continue
            extended.append(
                IndependentTemplate(
                    template_id=str(item["template_id"]).strip(),
                    family=str(item.get("family") or "").strip(),
                    intent=str(item.get("intent") or "").strip(),
                    skeleton=str(item.get("cypher_skeleton") or "").strip(),
                    required_slots=[x for x in item.get("required_slots", []) if isinstance(x, dict)],
                    constraints=item.get("constraints", {}) if isinstance(item.get("constraints"), dict) else {},
                    property_whitelist=item.get("property_whitelist", {}) if isinstance(item.get("property_whitelist"), dict) else {},
                    repo_scope_policy=[x for x in item.get("repo_scope_policy", []) if isinstance(x, dict)],
                    selection=item.get("selection", {}) if isinstance(item.get("selection"), dict) else {},
                    default_limit=int(item["default_limit"]) if item.get("default_limit") is not None else None,
                    scope_slots=[x for x in item.get("scope_slots", []) if isinstance(x, dict)],
                    projection_contract=[x for x in item.get("projection_contract", []) if isinstance(x, dict)],
                    projection_options=item.get("projection_options", {})
                    if isinstance(item.get("projection_options"), dict)
                    else {},
                )
            )
        template_ids = [template.template_id for template in extended]
        if len(template_ids) != len(set(template_ids)):
            raise ValueError(f"duplicate template_id in layered pack {template_path}")
        return extended
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
                scope_slots=[x for x in item.get("scope_slots", []) if isinstance(x, dict)],
                projection_contract=[x for x in item.get("projection_contract", []) if isinstance(x, dict)],
                projection_options=item.get("projection_options", {})
                if isinstance(item.get("projection_options"), dict)
                else {},
            )
        )
    template_ids = [template.template_id for template in out]
    if len(template_ids) != len(set(template_ids)):
        raise ValueError(f"duplicate template_id in pack {template_path}")
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


def _unsupported_constraint_entry(
    text: str,
    match: re.Match[str],
    *,
    kind: str,
) -> dict[str, Any]:
    start, end = match.span()
    return {
        "kind": kind,
        "reason_code": UNSUPPORTED_EXPLICIT_CONSTRAINT_REASON,
        "surface_text": text[start:end].strip(),
        "source_span": [start, end],
        "provenance": "unsupported_explicit_constraint_from_nl",
    }


def _detect_unsupported_explicit_constraints(
    text: str,
    ir: ControlledQueryIR,
) -> list[dict[str, Any]]:
    """Record explicit constraints outside the frozen controlled grammar.

    This detector is deliberately a safety boundary, not a semantic parser:
    recognized surfaces are represented as unsupported metadata and force
    fail-closed selection rather than being normalized into executable IR.
    """
    entries: list[dict[str, Any]] = []
    # Negative typed-prefix operators are represented for audit visibility but
    # are outside the executable template contract.  Keep them fail-closed;
    # never allow a NOT_STARTS_WITH scope to fall through as a positive query.
    for scope in ir.entity_scopes:
        if scope.operator != "NOT_STARTS_WITH" or not scope.source_span:
            continue
        start, end = scope.source_span
        before_scope = text[max(0, start - 100) : start]
        if not re.search(
            rf"\b{_TYPED_PREFIX_NEGATION_CUE}\s+"
            r"(?:(?:the|this|that|these|those)\s+)?$",
            before_scope,
            re.IGNORECASE,
        ):
            continue
        entries.append(
            {
                "kind": "unsupported_exclusion_surface",
                "reason_code": UNSUPPORTED_EXPLICIT_CONSTRAINT_REASON,
                "surface_text": text[max(0, start - 80) : end].strip(),
                "source_span": [start, end],
                "provenance": "unsupported_explicit_constraint_from_nl",
            }
        )
    entries.extend(
        _unsupported_constraint_entry(
            text,
            match,
            kind="unsupported_exclusion_surface",
        )
        for match in _UNSUPPORTED_EXCLUSION_PATTERN.finditer(text)
    )

    # A typed prefix followed by a direct negative/exclusion suffix is not a
    # supported conjunction.  Without this bounded detector the parser would
    # execute only the positive prefix and silently drop the explicit suffix.
    # Keep the rule local to an earlier positive typed-prefix scope; this is a
    # safety firewall, not a general Boolean-negation parser.
    positive_prefix_scopes = [
        scope
        for scope in ir.entity_scopes
        if scope.operator == "STARTS_WITH"
        and scope.property == "entity_id"
        and scope.source_span
    ]
    for match in _TRAILING_TYPED_EXCLUSION_PATTERN.finditer(text):
        prior_scopes = [
            scope for scope in positive_prefix_scopes if scope.source_span[1] <= match.start()
        ]
        if not prior_scopes:
            continue
        previous = max(prior_scopes, key=lambda scope: scope.source_span[1])
        between = text[previous.source_span[1] : match.start()]
        # Do not turn unrelated prose containing “except”/“without” into a
        # global blacklist.  The suffix must be attached to the same local
        # clause and must not cross a sentence boundary.
        if re.search(r"[.!?;]", between):
            continue
        prefix_match = re.match(r"(?:I|PR)_", match.group("prefix"), re.IGNORECASE)
        if not prefix_match:
            continue
        entries.append(
            _unsupported_constraint_entry(
                text,
                match,
                kind="unsupported_exclusion_surface",
            )
        )

    for match in _UNSUPPORTED_POSTFIX_UNIQUENESS_PATTERN.finditer(text):
        sentence_start = max(
            text.rfind(char, 0, match.start()) for char in ".!?;"
        ) + 1
        prefix = text[sentence_start : match.start()]
        # Keep this local to output/result wording. A generic prose use such
        # as "the process ran without duplicates" is not an output contract.
        if not re.search(
            r"\b(?:show|list|return|display|give|find|tell|which|what)\b",
            prefix,
            re.IGNORECASE,
        ):
            continue
        if ir.projection_distinct or any(item.distinct for item in ir.projection_items):
            continue
        entries.append(
            _unsupported_constraint_entry(
                text,
                match,
                kind="unsupported_postfix_uniqueness_surface",
            )
        )

    existing_limit_value_spans = {
        match.span(1)
        for pattern in UNNORMALIZED_LIMIT_PATTERNS
        for match in pattern.finditer(text)
    }
    for match in _UNSUPPORTED_CARDINALITY_PATTERN.finditer(text):
        number_group = "limit" if match.group("limit") else "reverse_limit"
        # Preserve the already-frozen bounded limit forms. Those surfaces are
        # handled by the existing limit contract audit (including its default
        # entailment rule); this firewall closes the new entity-noun gap.
        if match.span(number_group) in existing_limit_value_spans:
            continue
        # Preserve only the already-frozen typed PullRequest ID-list family.
        # A later ID/identifier mention alone does not prove that the cap is
        # represented. Retain its numeric value in the ordinary limit audit:
        # an equal contract default may entail it, while a conflicting cap
        # must fail closed instead of inheriting that default.
        noun = re.sub(
            r"\s+", " ", str(match.group("noun") or match.group("reverse_noun") or "")
        ).strip().lower()
        is_pull_request_cap = noun in {"pull request", "pull requests", "pr", "prs"}
        has_typed_pull_request_prefix = any(
            scope.label == "PullRequest"
            and scope.property == "entity_id"
            and scope.operator == "STARTS_WITH"
            for scope in ir.entity_scopes
        )
        is_pull_request_id_projection = (
            len(ir.projection_items) == 1
            and ir.projection_items[0].label == "PullRequest"
            and ir.projection_items[0].property == "entity_id"
        )
        if (
            is_pull_request_cap
            and has_typed_pull_request_prefix
            and is_pull_request_id_projection
        ):
            value = int(match.group(number_group))
            if value not in ir.unnormalized_limit_values:
                ir.unnormalized_limit_values.append(value)
            continue
        entries.append(
            _unsupported_constraint_entry(
                text,
                match,
                kind="unsupported_cardinality_surface",
            )
        )

    unique: list[dict[str, Any]] = []
    seen: set[tuple[str, tuple[int, int]]] = set()
    for entry in entries:
        key = (str(entry["kind"]), tuple(entry["source_span"]))
        if key not in seen:
            seen.add(key)
            unique.append(entry)
    return unique


def _entity_label_for_id(entity_id: str) -> str | None:
    for prefix, label in sorted(LABEL_FROM_PREFIX.items(), key=lambda kv: -len(kv[0])):
        if entity_id.startswith(prefix + "_"):
            return label
    return None


def _extract_typed_entity_scopes(text: str) -> list[EntityScope]:
    def negated_prefix_scope(match: re.Match[str], before_text: str | None = None) -> bool:
        """Recognize only the bounded negations of this scope grammar."""
        before = (
            before_text
            if before_text is not None
            else text[max(0, match.start() - 180) : match.start()]
        )
        if re.search(
            r"(?:do|does|did)\s+not\s+(?:start|begin)(?:s|ing)?\s+with\s*$",
            before,
            re.IGNORECASE,
        ):
            return True
        if re.search(
            r"(?:do|does|did)n['’]t\s+(?:start|begin)(?:s|ing)?\s+with\s*$",
            before,
            re.IGNORECASE,
        ):
            return True
        if re.search(
            r"not\s+(?:start(?:s|ing)?|begin(?:s|ning)?)\s+with\s*$",
            before,
            re.IGNORECASE,
        ):
            return True
        # Command-level negation is scoped only to a nearby typed ID phrase;
        # this is deliberately not a global ``not in text`` test.
        if re.search(
            r"\b(?:excluding|omit|omitting|without)\s+(?:(?:the|these|those)\s+)?"
            r"(?:ids?|identifiers?)\s+(?:that\s+)?"
            r"(?:start(?:s|ing)?|begin(?:s|ning)?)\s+with\s*$",
            before,
            re.IGNORECASE,
        ):
            return True
        if re.search(
            r"\b(?:excluding|omit|omitting|without)\s+(?:issues?|pull\s+requests?)\s+"
            r"whose\s+(?:ids?|identifiers?)\s+"
            r"(?:start(?:s|ing)?|begin(?:s|ning)?)\s+with\s*$",
            before,
            re.IGNORECASE,
        ):
            return True
        if re.search(
            rf"\b{_TYPED_PREFIX_NEGATION_CUE}\s+"
            r"(?:(?:the|these|those)\s+)?"
            r"(?:(?:issues?|pull\s+requests?)\s+)?prefix\s*$",
            before,
            re.IGNORECASE,
        ):
            return True
        if re.search(
            rf"\b(?:except(?:\s+for)?|but\s+not)\s+"
            r"(?:(?:those|issues?|pull\s+requests?)\s+whose\s+)?"
            r"(?:ids?|identifiers?)\s+(?:that\s+)?"
            r"(?:start(?:s|ing)?|begin(?:s|ning)?)\s+with\s*$",
            before,
            re.IGNORECASE,
        ):
            return True
        return bool(
            re.search(
                r"(?:^|[.!?;])\s*(?:exclude|omit)\b[^.!?;]{0,150}"
                r"\b(?:ids?|identifiers?)\s+(?:that\s+)?"
                r"(?:start(?:s|ing)?|begin(?:s|ning)?)\s+with\s*$",
                before,
                re.IGNORECASE,
            )
        )

    scopes: list[EntityScope] = []
    for match in TYPED_PREFIX_PATTERN.finditer(text):
        before = text[max(0, match.start() - 180) : match.start()]
        # Keep the operator structurally adjacent to the typed token. A
        # nearby but unrelated operator elsewhere in the sentence must not
        # manufacture a typed-prefix constraint.
        operator_window = re.split(r"[.!?;]", before)[-1]
        after = text[match.end() : match.end() + 32]
        operator_source = operator_window
        repeated_operator = False
        if not (
            _PREFIX_OPERATOR_SUFFIX.search(operator_source)
            or _ENDS_WITH_OPERATOR_SUFFIX.search(operator_source)
            or _CONTAINS_OPERATOR_SUFFIX.search(operator_source)
            or _UNSPECIFIED_TYPED_SCOPE_SUFFIX.search(operator_source)
        ):
            repeated_operator = bool(re.search(r"\b(?:or|and)\s*$", operator_source, flags=re.IGNORECASE))
            operator_source = re.sub(r"\b(?:or|and)\s*$", "", operator_source, flags=re.IGNORECASE).strip()
        if repeated_operator and re.search(
            r"\b(?:whose\s+(?:entity\s+)?(?:ids?|identifiers?)\s+)?"
            r"(?:start|begin)(?:s|ing|ning)?\s+with\b",
            operator_source,
            re.IGNORECASE,
        ):
            operator = "STARTS_WITH"
            provenance = "typed_prefix_scope_from_nl"
        elif repeated_operator and re.search(
            r"\b(?:whose\s+(?:entity\s+)?(?:ids?|identifiers?)\s+)?"
            r"end(?:s|ing)?\s+with\b",
            operator_source,
            re.IGNORECASE,
        ):
            operator = "ENDS_WITH"
            provenance = "unsupported_typed_scope_operator_from_nl"
        elif repeated_operator and re.search(
            r"\b(?:whose\s+(?:entity\s+)?(?:ids?|identifiers?)\s+)?contain(?:s|ing)?\b",
            operator_source,
            re.IGNORECASE,
        ):
            operator = "CONTAINS"
            provenance = "unsupported_typed_scope_operator_from_nl"
        elif _PREFIX_TOKEN_SUFFIX.search(after):
            # Bounded form such as "IDs that fall under PR_123 prefix".
            # The operator cue follows the typed token, but remains directly
            # attached to it rather than being inferred from distant text.
            post_token_negated = bool(_POST_TOKEN_NEGATION_CUE_SUFFIX.search(operator_window))
            operator = "NOT_STARTS_WITH" if post_token_negated else "STARTS_WITH"
            provenance = (
                "negated_typed_prefix_scope_from_nl"
                if post_token_negated
                else "typed_prefix_scope_from_nl"
            )
        elif _PREFIX_OPERATOR_SUFFIX.search(operator_source):
            operator = "NOT_STARTS_WITH" if negated_prefix_scope(match, operator_source) else "STARTS_WITH"
            provenance = (
                "negated_typed_prefix_scope_from_nl"
                if operator == "NOT_STARTS_WITH"
                else "typed_prefix_scope_from_nl"
            )
        elif _ENDS_WITH_OPERATOR_SUFFIX.search(operator_source):
            operator = "ENDS_WITH"
            provenance = "unsupported_typed_scope_operator_from_nl"
        elif _CONTAINS_OPERATOR_SUFFIX.search(operator_source):
            operator = "CONTAINS"
            provenance = "unsupported_typed_scope_operator_from_nl"
        elif _UNSPECIFIED_TYPED_SCOPE_SUFFIX.search(operator_source):
            # Preserve an explicit typed equality-like request without
            # treating an arbitrary entity ID as a prefix filter.
            operator = "UNSPECIFIED"
            provenance = "unsupported_typed_scope_operator_from_nl"
        else:
            continue
        prefix = match.group("prefix").upper()
        scopes.append(
            EntityScope(
                label=TYPED_PREFIX_LABELS[prefix],
                property="entity_id",
                operator=operator,
                value=f"{prefix}_{match.group('number')}",
                provenance=provenance,
                source_span=[match.start(), match.end()],
            )
        )
    return scopes


def _entity_noun_occurrences(text: str) -> list[tuple[int, int, str]]:
    found: list[tuple[int, int, str]] = []
    for label, pattern in ENTITY_NOUN_PATTERNS:
        for match in pattern.finditer(text):
            found.append((match.start(), match.end(), label))
    return sorted(found)


def _source_anchor_noun_occurrences(
    text: str, nouns: list[tuple[int, int, str]]
) -> set[tuple[int, int, str]]:
    """Find entity nouns that structurally introduce a canonical source ID.

    Only a direct ``<entity noun> <canonical-id>`` span is treated as a source
    introduction. Explicit ID/identifier wording between the noun and anchor
    remains eligible to express a requested projection.
    """
    mentions, _ = _extract_entity_mentions(text)
    canonical_anchors = [
        (str(item.get("entity_label") or ""), item.get("span", []))
        for item in mentions
        if item.get("provenance") == "canonical_id_from_nl"
    ]
    anchored: set[tuple[int, int, str]] = set()
    for anchor_label, span in canonical_anchors:
        if len(span) != 2:
            continue
        anchor_start = int(span[0])
        typed_spans = [
            (start, end, label)
            for start, end, label in nouns
            if label == anchor_label
            and end <= anchor_start
            and _source_anchor_gap_is_structural(text, end, anchor_start)
        ]
        if not typed_spans:
            continue

        # Prefer the longest canonical typed noun phrase, then suppress only
        # noun matches wholly contained by that source-introduction span.
        # This prevents e.g. Issue inside IssueComment from being re-read as
        # an output cue while preserving an explicit Issue projection later.
        source_start, source_end, _ = min(
            typed_spans,
            key=lambda item: (-(item[1] - item[0]), item[0]),
        )
        anchored.update(
            noun
            for noun in nouns
            if source_start <= noun[0] and noun[1] <= source_end
        )
    return anchored


def _source_anchor_gap_is_structural(text: str, noun_end: int, anchor_start: int) -> bool:
    """Accept bounded punctuation, but never lexical words, before an ID."""
    gap = text[noun_end:anchor_start]
    if not gap:
        return True
    return re.fullmatch(r"[\s:([{\-]+", gap) is not None


def _projection_items_from_text(text: str) -> list[ProjectionItem]:
    lower = text.lower()
    nouns = _entity_noun_occurrences(text)
    source_anchor_nouns = _source_anchor_noun_occurrences(text, nouns)
    aggregate_argument_spans = _aggregate_distinct_argument_spans(text)

    def is_aggregate_argument(match: re.Match[str]) -> bool:
        return any(start <= match.start() < end for start, end in aggregate_argument_spans)

    def item_uniqueness_cue_span(
        start: int,
        *,
        bounded_item_phrase: str = r"(?:the\s+)?",
    ) -> list[int] | None:
        """Return the lexical cue span governing this item occurrence."""
        window_start = max(0, start - 64)
        cue = re.search(
            rf"\b(?P<cue>distinct|unique)\s+{bounded_item_phrase}$",
            text[window_start:start],
            re.I,
        )
        return (
            [window_start + cue.start("cue"), window_start + cue.end("cue")]
            if cue
            else None
        )

    def parse_post_noun_id_projection_cue(
        match: re.Match[str],
    ) -> tuple[bool, list[int] | None, tuple[int, int] | None]:
        """Parse a bounded possessive/uniqueness/identifier phrase after a noun."""
        tail = text[match.end():match.end() + 64]
        cue = re.match(
            r"\s*(?:(?:'s)|')?\s*(?:(?P<distinct>distinct|unique)\s+)?"
            r"(?:entity\s+)?(?P<identifier>ids?|identifiers?)\b",
            tail,
            re.I,
        )
        if not cue:
            return False, None, None
        start = match.end() + cue.start()
        end = match.end() + cue.end()
        distinct_span = None
        if cue.group("distinct"):
            distinct_span = [
                match.end() + cue.start("distinct"),
                match.end() + cue.end("distinct"),
            ]
        return True, distinct_span, (start, end)

    nullable = bool(
        re.search(r"\b(?:optional|if\s+any|where\s+they\s+exist|where\s+it\s+exists|"
                  r"neither\s+is\s+required|empty\s+when\s+missing)\b", lower)
    )
    candidates: list[ProjectionItem] = []

    def add(
        label: str,
        start: int,
        end: int,
        *,
        property_name: str = "entity_id",
        role: str = "target_entity",
        distinct: bool = False,
        distinct_source_span: list[int] | None = None,
    ) -> None:
        existing = next(
            (item for item in candidates if item.label == label and item.property == property_name),
            None,
        )
        if existing is not None:
            existing.distinct = existing.distinct or distinct
            if distinct_source_span is not None and distinct_source_span not in existing.distinct_source_spans:
                existing.distinct_source_spans.append(distinct_source_span)
            return
        candidates.append(
            ProjectionItem(
                role=role,
                label=label,
                property=property_name,
                distinct=distinct,
                distinct_source_spans=(
                    [distinct_source_span] if distinct_source_span is not None else []
                ),
                nullable=nullable,
                provenance="bounded_role_projection_rule",
                source_span=[start, end],
            )
        )

    # An explicitly named entity followed by ID/identifier is the strongest
    # bounded projection cue; it is independent of the source anchor.
    for label, pattern in ENTITY_NOUN_PATTERNS:
        for match in pattern.finditer(text):
            if is_aggregate_argument(match):
                continue
            matched, post_noun_distinct_span, cue_span = parse_post_noun_id_projection_cue(match)
            if not matched:
                tail = text[match.end():match.end() + 52]
                id_cue = re.match(
                    r"\s+(?:whose|with)\s+(?:entity\s+)?(?:ids?|identifiers?)\b",
                    tail,
                    re.I,
                )
                if id_cue:
                    cue_span = (match.end() + id_cue.start(), match.end() + id_cue.end())
                    matched = True
            if matched and cue_span:
                add(
                    label,
                    cue_span[0],
                    cue_span[1],
                    distinct=bool(
                        post_noun_distinct_span
                        or item_uniqueness_cue_span(match.start())
                    ),
                    distinct_source_span=(
                        post_noun_distinct_span
                        or item_uniqueness_cue_span(match.start())
                    ),
                )

    # “IDs of <entity>” and equivalent possessives preserve natural column order.
    for label, pattern in ENTITY_NOUN_PATTERNS:
        for match in pattern.finditer(text):
            if is_aggregate_argument(match):
                continue
            prefix = text[max(0, match.start() - 48):match.start()]
            if re.search(r"\b(?:ids?|identifiers?)\s+of\s+(?:the\s+)?$", prefix, re.I):
                add(
                    label,
                    match.start(),
                    match.end(),
                    distinct=bool(item_uniqueness_cue_span(
                        match.start(),
                        bounded_item_phrase=(
                            r"(?:(?:the|entity)\s+)*(?:ids?|identifiers?)"
                            r"\s+of\s+(?:the\s+)?"
                        ),
                    )),
                    distinct_source_span=item_uniqueness_cue_span(
                        match.start(),
                        bounded_item_phrase=(
                            r"(?:(?:the|entity)\s+)*(?:ids?|identifiers?)"
                            r"\s+of\s+(?:the\s+)?"
                        ),
                    ),
                )

    # A question/list cue can itself name the requested entity role even when
    # the wording says “which objects?” and later refers to “their IDs”.
    output_nouns: list[tuple[int, int, str]] = []
    property_noun_pattern = re.compile(
        r"\b(?:(?:registrable|site)\s+)?domains?\b", re.I
    )

    def entity_noun_is_attributive_property_qualifier(
        match: re.Match[str],
    ) -> bool:
        """Do not infer an entity-ID column from a noun modifying domain(s)."""
        if parse_post_noun_id_projection_cue(match)[0]:
            return False
        return any(
            re.fullmatch(r"\s+", text[match.end():property_match.start()])
            for property_match in property_noun_pattern.finditer(text)
            if property_match.start() >= match.end()
        )

    for start, end, label in nouns:
        if any(span_start <= start < span_end for span_start, span_end in aggregate_argument_spans):
            continue
        if (start, end, label) in source_anchor_nouns:
            continue
        entity_match = next(
            (
                match
                for noun_label, pattern in ENTITY_NOUN_PATTERNS
                if noun_label == label
                for match in pattern.finditer(text)
                if match.start() == start and match.end() == end
            ),
            None,
        )
        if label == "ExternalResource" and entity_match and entity_noun_is_attributive_property_qualifier(entity_match):
            continue
        prefix = lower[max(0, start - 24):start]
        if (
            re.search(r"\b(?:which|what|list|show|return|give|display)\s+(?:me\s+)?(?:the\s+)?(?:distinct|unique\s+)?(?:involved|mentioned|referenced|linked)?\s*$", prefix)
            or re.search(r"\b(?:and|or)\s+(?:involved|mentioned|referenced|linked)\s*$", prefix)
            or (label == "UnknownObject" and re.search(r"\band\s*$", prefix))
        ):
            output_nouns.append((start, end, label))
            if re.search(r"\b(?:which|what)\s+(?:unknown(?:[- ]type)?\s+|untyped\s+)?objects?\s*$", prefix):
                label = "UnknownObject"
            cue_span = item_uniqueness_cue_span(start)
            add(
                label,
                start,
                end,
                distinct=cue_span is not None,
                distinct_source_span=cue_span,
            )

    # A bare “who” is a role cue for Actor, but only when it asks for a result.
    who = re.search(r"\b(?:who|whoever)\b", text, re.I)
    if who and not any(item.label == "Actor" for item in candidates):
        actor_distinct_span = item_uniqueness_cue_span(
            who.start(),
            bounded_item_phrase=(
                r"(?:(?:the|entity)\s+)*(?:ids?|identifiers?)"
                r"\s+of\s+(?:the\s+)?"
            ),
        )
        add(
            "Actor",
            who.start(),
            who.end(),
            distinct=actor_distinct_span is not None,
            distinct_source_span=actor_distinct_span,
        )

    # Bounded ID pronouns first reuse a unique, explicit ID projection that
    # precedes the pronoun. Otherwise, only an output-cued noun (never a noun
    # used solely to introduce a source anchor) may be an antecedent. Ambiguous
    # multi-role wording remains unresolved rather than choosing arbitrarily.
    for match in re.finditer(
        r"\b(?:their|these|those)\s+(?:entity\s+)?(?:ids?|identifiers?)\b",
        text,
        re.I,
    ):
        established_roles = {
            item.label
            for item in candidates
            if item.label
            and item.property == "entity_id"
            and item.source_span[0] < match.start()
        }
        if established_roles:
            # A unique explicit output role takes priority. If multiple roles
            # are already explicit, do not add a pronoun-derived projection.
            continue
        preceding_output_nouns = [
            item
            for item in output_nouns
            if item[1] <= match.start() and item not in source_anchor_nouns
        ]
        output_roles = {item[2] for item in preceding_output_nouns}
        if len(output_roles) == 1:
            start, end, label = preceding_output_nouns[-1]
            add(label, start, end)

    # Domain is a property projection, distinct from the resource-ID column.
    for match in property_noun_pattern.finditer(text):
        if is_aggregate_argument(match):
            continue
        preceding_text = lower[max(0, match.start() - 24):match.start()]
        if re.search(r"\b(?:external|outside)\s+$", preceding_text):
            continue
        domain_distinct_span = item_uniqueness_cue_span(
            match.start(),
            bounded_item_phrase=(
                r"(?:(?:external|outside)\s+)?resources?\s+"
                r"(?:(?:registrable|site)\s+)?"
            ),
        ) or item_uniqueness_cue_span(match.start())
        if not any(item.property == "url_domain_etld1" for item in candidates):
            candidates.append(
                ProjectionItem(
                    role="entity_property",
                    label="ExternalResource",
                    property="url_domain_etld1",
                    distinct=domain_distinct_span is not None,
                    nullable=nullable,
                    provenance="bounded_property_projection_rule",
                    source_span=[match.start(), match.end()],
                    distinct_source_spans=(
                        [domain_distinct_span] if domain_distinct_span is not None else []
                    ),
                )
            )

    # If a typed output noun is followed later by a generic “IDs” reference,
    # consume that role without reusing an earlier source entity.
    if not candidates:
        generic_ids = re.search(r"\b(?:ids?|identifiers?)\b", text, re.I)
        if generic_ids:
            preceding = [
                item
                for item in nouns
                if item[1] <= generic_ids.start()
                and item not in source_anchor_nouns
                and not any(
                    span_start <= item[0] < span_end
                    for span_start, span_end in aggregate_argument_spans
                )
            ]
            if preceding:
                start, end, label = preceding[-1]
                add(label, start, end)

    candidates.sort(key=lambda item: (item.source_span[0], item.source_span[1], item.label or ""))
    return candidates


def _aggregate_distinct_argument_spans(text: str) -> list[tuple[int, int]]:
    target = (
        r"(?:pull\s+requests?|prs?|issues?|repos?(?:itories)?|actors?|users?|commits?|"
        r"external\s+resources?|(?:(?:registrable|site)\s+)?domains?)"
    )
    pattern = re.compile(
        rf"\bcount\s+(?:of\s+)?distinct\s+(?P<target>{target})"
        r"(?:\s+(?:entity\s+)?(?:ids?|identifiers?))?\b",
        re.IGNORECASE,
    )
    return [(match.start("target"), match.end()) for match in pattern.finditer(text)]


def _parse_aggregate_distinct(text: str) -> list[dict[str, Any]]:
    labels = (
        ("PullRequest", r"pull\s+requests?|prs?"),
        ("Issue", r"issues?"),
        ("Repo", r"repos?(?:itories)?"),
        ("Actor", r"actors?|users?"),
        ("Commit", r"commits?"),
        ("ExternalResource", r"external\s+resources?"),
    )
    found: list[dict[str, Any]] = []
    for label, pattern in labels:
        if re.search(
            rf"\bcount\s+(?:of\s+)?distinct\s+(?:{pattern})\b",
            text,
            re.IGNORECASE,
        ):
            found.append(
                {
                    "function": "count",
                    "field": f"{label}.entity_id",
                    "distinct": True,
                    "provenance": "aggregate_argument_distinct_from_nl",
                }
            )
    if re.search(
        r"\bcount\s+(?:of\s+)?distinct\s+(?:(?:registrable|site)\s+)?domains?\b",
        text,
        re.IGNORECASE,
    ):
        found.append(
            {
                "function": "count",
                "field": "ExternalResource.url_domain_etld1",
                "distinct": True,
                "provenance": "aggregate_argument_distinct_from_nl",
            }
        )
    return found


def _ordinary_count_requested(text: str) -> bool:
    """Detect a count requirement while excluding count-distinct phrases."""
    for match in re.finditer(r"\bcount\b", text, re.IGNORECASE):
        tail = text[match.end():match.end() + 40]
        if re.match(r"\s+(?:of\s+)?distinct\b", tail, re.IGNORECASE):
            continue
        return True
    return False


def _projection_tuple_distinct_from_text(text: str, items: list[ProjectionItem]) -> bool:
    """Infer only whole-result cues; bare/local DISTINCT is not tuple DISTINCT."""
    if re.search(
        r"\b(?:distinct\s+(?:result\s+)?rows?|unique\s+(?:rows?|combinations?|pairs?|tuples?)|"
        r"deduplicate\s+(?:the\s+)?(?:result\s+)?rows?|de-duplicate\s+(?:the\s+)?(?:result\s+)?rows?)\b",
        text,
        re.IGNORECASE,
    ):
        return True

    # A leading `return distinct X and Y` is accepted only when it clearly
    # scopes over two requested projection items, with no aggregate-local
    # DISTINCT or explicit local-only modifier in the second item.
    leading_distinct = re.search(r"\b(?:return|show|list|display|give)\s+distinct\b", text, re.I)
    if not leading_distinct or len(items) < 2 or _aggregate_distinct_argument_spans(text):
        return False
    # In ``return distinct IDs of actors and resource IDs``, the bounded
    # ``distinct IDs of <entity>`` phrase scopes to its named item, not the
    # whole multi-column tuple.
    if re.match(
        r"\s+(?:(?:the|entity)\s+)*(?:ids?|identifiers?)\s+of\s+(?:the\s+)?",
        text[leading_distinct.end():],
        re.I,
    ):
        return False
    second_start = items[1].source_span[0] if items[1].source_span else len(text)
    between_items = text[leading_distinct.end():second_start]
    if re.search(r"\bordinary\b", between_items, re.I):
        return False
    return True


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


def _infer_target_labels_with_provenance(
    text: str, aligned: list[dict[str, Any]]
) -> tuple[list[str], dict[str, str]]:
    lower = text.lower()
    labels: list[str] = []
    provenance: dict[str, str] = {}

    def add(label: str, source: str) -> None:
        if label not in labels:
            labels.append(label)
        provenance.setdefault(label, source)

    if any(phrase in lower for phrase in ("external links", "external domains", "external resources", "by domain")):
        add("ExternalResource", "explicit_target_from_nl")
    if any(phrase in lower for phrase in ("referenced objects", "which objects", "objects are referenced")):
        add("UnknownObject", "explicit_target_from_nl")
    if "mentioned repos" in lower or "which repos" in lower:
        add("Repo", "explicit_target_from_nl")
    if any(phrase in lower for phrase in ("which actors", "actors commented", "involved actors", "interacted with", "mention actor", "opened")):
        add("Actor", "explicit_target_from_nl")
    if "commits" in lower and "reference" in lower:
        add("Commit", "explicit_target_from_nl")
    if "list prs" in lower or "find prs" in lower or "list pull requests" in lower or "find pull requests" in lower:
        add("PullRequest", "explicit_target_from_nl")
    # A canonical entity with a role-bearing phrase is stronger than a generic noun.
    if not labels and aligned:
        fallback = str(aligned[0].get("entity_label"))
        if fallback and fallback != "None":
            add(fallback, "fallback_from_aligned_source")
    return list(dict.fromkeys(x for x in labels if x and x != "None")), provenance


def _infer_target_labels(text: str, aligned: list[dict[str, Any]]) -> list[str]:
    return _infer_target_labels_with_provenance(text, aligned)[0]


def _infer_intent_key(text: str, relation_semantics: list[str], ir: ControlledQueryIR) -> str:
    lower = text.lower()
    semantic_set = set(relation_semantics)
    if "comprehensive" in lower and "domain" in lower and "involved actors" in lower:
        return "comprehensive_external_actor_aggregation"
    source_is_pull_request = bool(
        ir.source_entity and ir.source_entity.get("entity_label") == "PullRequest"
    )
    narrow_domain_aggregation = bool(
        ir.aggregation
        and "ExternalResource" in ir.target_labels
        and "LINKS_TO" in semantic_set
        and ir.projection.get("property") == "url_domain_etld1"
        and source_is_pull_request
        and ir.time_range
        and ir.time_range.get("start")
        and ir.time_range.get("end")
        and "Actor" not in ir.target_labels
        and "involved actors" not in lower
    )
    if narrow_domain_aggregation:
        return "narrow_domain_aggregation"
    if "mentioned repos" in lower and "external links" in lower:
        return "actor_multi_target_reference"
    has_pull_request_scope = any(scope.label == "PullRequest" for scope in ir.entity_scopes)
    has_issue_scope = any(scope.label == "Issue" for scope in ir.entity_scopes)
    if "mention actor" in lower and "also link" in lower and (ir.repo_scope or has_pull_request_scope) and "LINKS_TO" in semantic_set:
        return "repo_actor_external_lower_bound"
    if "review comment" in lower and "reference" in lower and "COMMENTED_ON_REVIEW" in semantic_set:
        return "review_reference"
    if "issue" in lower and "comment" in lower and "commit" in lower and "REFERENCES" in semantic_set:
        return "issue_comment_commit"
    if "pr" in lower and "referenced objects" in lower and (ir.repo_scope or has_pull_request_scope) and ir.time_range:
        return "repo_pr_reference_window"
    if has_issue_scope and "issue" in lower and "comment" in lower and "commit" in lower and "REFERENCES" in semantic_set:
        return "issue_comment_commit"
    if "commented on issue" in lower or ("actors" in lower and "commented on issue" in lower):
        return "issue_comment_actor"
    if "opened" in lower and "issue" in lower and "OPENED_BY" in semantic_set:
        return "issue_opened_by"
    if has_issue_scope and not semantic_set:
        return "issue_prefix_filter"
    if has_pull_request_scope and not semantic_set:
        return "repo_pull_request_filter"
    if (
        ("external links" in lower or "ExternalResource" in ir.target_labels)
        and ("pull request" in lower or re.search(r"\bpr\b", lower))
        and "LINKS_TO" in semantic_set
        and not ir.aggregation
    ):
        return "typed_reference_external_property"
    if "objects" in lower and "REFERENCES" in semantic_set:
        return "typed_reference_object"
    if "actors" in lower and "issue comment" in lower and "MENTIONS" in semantic_set:
        return "typed_reference_actor"
    if "PullRequest" in ir.target_labels and (ir.repo_scope or has_pull_request_scope) and not semantic_set:
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
    ir.entity_scopes = _extract_typed_entity_scopes(text)
    for scope in ir.entity_scopes:
        _append_provenance(ir, "entity_scopes", scope.provenance)
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
    elif (
        ("commented on issue" in lower or "comments that" in lower)
        and "issue" in lower
    ):
        relation_semantics.append("COMMENTED_ON_ISSUE")
        if "actor" in lower:
            relation_semantics.append("OPENED_BY")
    if any(cue in lower for cue in SERVICE_LEXICON["LINKS_TO"]):
        relation_semantics.append("LINKS_TO")
    # "external links mentioned by PR" describes the source context, not a
    # second MENTIONS relation to an output target. Require an actor/repo
    # mention role before materializing that semantic constraint.
    mentions_target_cue = any(
        cue in lower
        for cue in ("mentioned repos", "which repos", "mention actor", "which actors", "involved actors", "actors")
    )
    if any(cue in lower for cue in SERVICE_LEXICON["MENTIONS"]) and mentions_target_cue:
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
    ir.unnormalized_limit_values = [
        int(match.group(1))
        for pattern in UNNORMALIZED_LIMIT_PATTERNS
        for match in pattern.finditer(text)
    ]
    ir.limit = ir.explicit_limit
    _append_provenance(ir, "limit", "explicit_limit_from_nl" if limit_match else "template_contract_default")

    ordinary_count_requested = _ordinary_count_requested(text)
    implicit_grouping_count = any(word in lower for word in ["group by", "by domain"])
    if ordinary_count_requested:
        ir.aggregation = [{"function": "count", "field": "*", "provenance": "bounded_semantic_rule"}]
        _append_provenance(ir, "aggregation", "bounded_semantic_rule")
    elif implicit_grouping_count:
        ir.aggregation = [{"function": "count", "field": "*", "provenance": "implicit_grouping_aggregation"}]
        _append_provenance(ir, "aggregation", "implicit_grouping_aggregation")
    aggregate_distinct = _parse_aggregate_distinct(text)
    if aggregate_distinct:
        ir.aggregation.extend(
            item for item in aggregate_distinct
            if item not in ir.aggregation
        )
        _append_provenance(ir, "aggregation", "aggregate_argument_distinct_from_nl")
    if "latest interaction time" in lower:
        ir.aggregation.append(
            {"function": "max", "field": "source_event_time", "provenance": "aggregation_latest_projection"}
        )
        _append_provenance(ir, "aggregation", "aggregation_latest_projection")
    if "domain" in lower:
        ir.projection["property"] = "url_domain_etld1"
        _append_provenance(ir, "projection", "bounded_semantic_rule")
    explicit_latest_sort = "latest" in lower and "latest interaction time" not in lower
    if explicit_latest_sort or any(word in lower for word in ["sorted by time", "sort by", "over time"]):
        ir.sort = [{"field": "source_event_time", "order": "desc", "provenance": "explicit_sort_from_nl"}]
        _append_provenance(ir, "sort", "bounded_semantic_rule")
    elif "ascending" in lower:
        ir.sort = [{"field": "source_event_time", "order": "asc", "provenance": "explicit_sort_from_nl"}]
        _append_provenance(ir, "sort", "bounded_semantic_rule")

    ir.projection_items = _projection_items_from_text(text)
    ir.projection_distinct = _projection_tuple_distinct_from_text(text, ir.projection_items)
    if ir.projection_distinct:
        # Only remove item-local attribution when it points to the exact same
        # lexical token that was reinterpreted as tuple-level DISTINCT.
        leading = re.search(
            r"\b(?:return|show|list|display|give)\s+(?P<distinct>distinct)\b",
            text,
            re.I,
        )
        if leading and ir.projection_items:
            first = ir.projection_items[0]
            leading_cue_span = [leading.start("distinct"), leading.end("distinct")]
            if first.distinct and first.distinct_source_spans == [leading_cue_span]:
                first.distinct = False
                first.distinct_source_spans = []
    if any(item.property == "url_domain_etld1" for item in ir.projection_items):
        ir.projection["property"] = "url_domain_etld1"
        _append_provenance(ir, "projection", "bounded_role_projection_rule")
    projected_labels = list(dict.fromkeys(item.label for item in ir.projection_items if item.label))
    if projected_labels:
        ir.target_labels = projected_labels
        ir.target_label_provenance = {label: "explicit_target_from_nl" for label in projected_labels}
    else:
        ir.target_labels, ir.target_label_provenance = _infer_target_labels_with_provenance(text, aligned)
    ir.unsupported_explicit_constraints = _detect_unsupported_explicit_constraints(text, ir)
    if ir.unsupported_explicit_constraints:
        _append_provenance(ir, "unsupported_explicit_constraints", "unsupported_explicit_constraint_from_nl")
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
    if ir.unsupported_explicit_constraints:
        ir.bounded_status = "ABSTAIN_UNSUPPORTED_EXPLICIT_CONSTRAINT"
        ir.abstention_reason = "unsupported explicit NL constraint"
    elif "COUPLES_WITH" in relation_semantics or "RESOLVES" in relation_semantics:
        ir.bounded_status = "ABSTAIN_PLACEHOLDER"
        ir.abstention_reason = "placeholder relation is outside the executable native contract"
    elif len(actor_ids) > 1:
        ir.bounded_status = "ABSTAIN_MULTIPLE_ACTOR_IDS"
        ir.abstention_reason = "current actor-targeted contract supports one canonical actor slot"
    elif not aligned and not ir.entity_scopes:
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


def _return_clause(skeleton: str) -> str:
    match = re.search(r"\bRETURN\b(.*?)(?:\bORDER\s+BY\b|\bLIMIT\b|$)", skeleton, flags=re.IGNORECASE | re.DOTALL)
    return match.group(1) if match else ""


def _return_has_tuple_distinct(skeleton: str) -> bool:
    # _return_clause starts immediately after RETURN. A leading DISTINCT is
    # tuple-level; DISTINCT nested inside an aggregate expression is local.
    return bool(re.match(r"\s*DISTINCT\b", _return_clause(skeleton), re.IGNORECASE))


def _split_return_expressions(body: str) -> list[str]:
    """Split a RETURN body on commas outside parentheses and quoted strings."""
    expressions: list[str] = []
    start = 0
    depth = 0
    quote: str | None = None
    escaped = False
    for index, char in enumerate(body):
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in {"'", '"'}:
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        elif char == "," and depth == 0:
            expressions.append(body[start:index].strip())
            start = index + 1
    tail = body[start:].strip()
    if tail:
        expressions.append(tail)
    return expressions


def _single_column_return_distinct_entails_item(
    skeleton: str, requested: dict[str, Any]
) -> bool:
    """Allow item DISTINCT only for a one-expression top-level RETURN."""
    if not requested.get("distinct") or not _return_has_tuple_distinct(skeleton):
        return False
    expressions = _split_return_expressions(
        re.sub(r"^\s*DISTINCT\b", "", _return_clause(skeleton), flags=re.IGNORECASE)
    )
    if len(expressions) != 1:
        return False
    expression = re.sub(
        r"\s+AS\s+[A-Za-z_][A-Za-z0-9_]*\s*$", "", expressions[0], flags=re.IGNORECASE
    ).strip()
    match = re.fullmatch(
        r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<property>[A-Za-z_][A-Za-z0-9_]*)",
        expression,
    )
    if not match:
        return False
    aliases = _skeleton_label_aliases(skeleton)
    return (
        aliases.get(match.group("alias")) == str(requested.get("label") or "")
        and match.group("property") == str(requested.get("property") or "entity_id")
    )


def _service_semantics_in_skeleton(skeleton: str) -> set[str]:
    semantics = set(re.findall(r"service_rel_type\s*=\s*'([^']+)'", skeleton, flags=re.IGNORECASE))
    for group in re.findall(r"service_rel_type\s+IN\s*\[([^\]]+)\]", skeleton, flags=re.IGNORECASE):
        semantics.update(re.findall(r"'([^']+)'", group))
    return {value.upper() for value in semantics}


def _skeleton_label_aliases(skeleton: str) -> dict[str, str]:
    return {
        alias: label
        for alias, label in re.findall(r"\(\s*([A-Za-z_]\w*)\s*:\s*([A-Za-z_]\w*)", skeleton)
    }


def _projection_contract_order_matches_skeleton(
    template: IndependentTemplate, return_clause: str
) -> bool:
    expressions: list[str] = []
    expression_start = 0
    depth = 0
    for index, char in enumerate(return_clause):
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        elif char == "," and depth == 0:
            expressions.append(return_clause[expression_start:index].strip())
            expression_start = index + 1
    if return_clause[expression_start:].strip():
        expressions.append(return_clause[expression_start:].strip())

    aliases_by_label: dict[str, list[str]] = {}
    for alias, label in _skeleton_label_aliases(template.skeleton).items():
        aliases_by_label.setdefault(label, []).append(alias)
    positions: list[int] = []
    matched_expression_indices: set[int] = set()
    for item in template.projection_contract:
        label = str(item.get("label") or "")
        property_name = str(item.get("property") or "entity_id")
        function = str(item.get("function") or "").lower()
        matching_indices: list[int] = []
        for index, expression in enumerate(expressions):
            if index in matched_expression_indices:
                continue
            if function:
                if not re.search(rf"\b{re.escape(function)}\s*\(", expression, re.I):
                    continue
                if property_name == "*":
                    if not re.search(rf"\b{re.escape(function)}\s*\(\s*(?:DISTINCT\s+)?\*\s*\)", expression, re.I):
                        continue
                elif not re.search(rf"\b{re.escape(property_name)}\b", expression, re.I):
                    continue
                if item.get("distinct") and not re.search(r"\bDISTINCT\b", expression, re.I):
                    continue
                matching_indices.append(index)
                continue
            property_matches = bool(re.search(rf"\b[A-Za-z_]\w*\.{re.escape(property_name)}\b", expression))
            if not property_matches:
                continue
            if property_name == "url_domain_etld1":
                matching_indices.append(index)
                continue
            if any(
                re.search(rf"\b{re.escape(alias)}\.{re.escape(property_name)}\b", expression)
                for alias in aliases_by_label.get(label, [])
            ):
                if item.get("distinct") and not (
                    re.search(r"\bDISTINCT\b", expression, re.I)
                    or re.match(r"^\s*DISTINCT\b", return_clause, re.I)
                ):
                    continue
                matching_indices.append(index)
        if not matching_indices:
            return False
        matched_expression_indices.add(matching_indices[0])
        positions.append(matching_indices[0])
    return positions == sorted(positions) and len(matched_expression_indices) == len(expressions)


def _time_bound_consumed(skeleton: str, token: str) -> bool:
    if token not in skeleton:
        return False
    return bool(
        re.search(
            rf"(?:source_event_time|event_time)[^\n;]*\${re.escape(token)}|\${re.escape(token)}[^\n;]*(?:source_event_time|event_time)",
            skeleton,
            flags=re.IGNORECASE,
        )
    )


def _compatible_scope_slots(scope: EntityScope, template: IndependentTemplate) -> list[str]:
    """Return singular template slots whose typed predicate consumes scope."""
    if scope.operator != "STARTS_WITH":
        return []
    matching_slots = {
        str(item.get("slot"))
        for item in template.scope_slots
        if item.get("label") == scope.label
        and item.get("property") == scope.property
        and item.get("operator") == scope.operator
        and item.get("slot")
    }
    if not matching_slots:
        return []
    aliases = [
        alias
        for alias, label in _skeleton_label_aliases(template.skeleton).items()
        if label == scope.label
    ]
    operator_pattern = r"\s+".join(re.escape(part) for part in scope.operator.split("_"))
    template_slots = _template_slot_names(template)
    return sorted(
        slot
        for slot in matching_slots
        if slot in template_slots
        and any(
            re.search(
                rf"\b{re.escape(alias)}\.{re.escape(scope.property)}\s+{operator_pattern}\s+\${re.escape(slot)}\b",
                template.skeleton,
                flags=re.IGNORECASE,
            )
            for alias in aliases
        )
    )


def _repo_scope_typed_prefix_conflicts(ir: ControlledQueryIR) -> list[dict[str, Any]]:
    """Return explicit positive typed prefixes that disagree with repo context."""
    if not ir.repo_scope:
        return []
    repo_entity_id = str(ir.repo_scope.get("repo_entity_id") or "")
    conflicts: list[dict[str, Any]] = []
    for scope in ir.entity_scopes:
        if scope.operator != "STARTS_WITH":
            continue
        expected = build_repo_scope_prefixes(repo_entity_id, [scope.label]).get(
            "base_prefixes", {}
        ).get(scope.label)
        if expected is None or str(scope.value) == str(expected):
            continue
        conflicts.append(
            {
                "reason_code": RepoScopeTypedPrefixConflictError.reason_code,
                "repo_entity_id": repo_entity_id,
                "scope_label": scope.label,
                "expected_repo_prefix": str(expected),
                "explicit_scope_value": str(scope.value),
                "scope_operator": scope.operator,
                "source_span": list(scope.source_span),
            }
        )
    return conflicts


def _scope_slot_analysis(
    ir: ControlledQueryIR, template: IndependentTemplate
) -> tuple[dict[str, list[EntityScope]], dict[str, EntityScope], list[dict[str, Any]]]:
    """Group explicit scopes by slot and identify non-representable conflicts."""
    groups: dict[str, list[EntityScope]] = {}
    repo_prefix_conflicts = _repo_scope_typed_prefix_conflicts(ir)
    conflicts_by_span = {
        tuple(item["source_span"]): item for item in repo_prefix_conflicts
    }
    unassigned: list[dict[str, Any]] = [dict(item) for item in repo_prefix_conflicts]
    for scope in ir.entity_scopes:
        if tuple(scope.source_span) in conflicts_by_span:
            continue
        slots = _compatible_scope_slots(scope, template)
        if len(slots) == 1:
            groups.setdefault(slots[0], []).append(scope)
        else:
            unassigned.append(
                {
                    **asdict(scope),
                    "reason_code": "UNSUPPORTED_NEGATED_TYPED_SCOPE_OPERATOR"
                    if scope.operator == "NOT_STARTS_WITH"
                    else "UNSUPPORTED_TYPED_SCOPE_OPERATOR"
                    if scope.provenance == "unsupported_typed_scope_operator_from_nl"
                    else "NO_COMPATIBLE_TYPED_SCOPE_SLOT"
                    if not slots
                    else "AMBIGUOUS_TYPED_SCOPE_SLOT",
                }
            )

    assignments: dict[str, EntityScope] = {}
    conflicts: list[dict[str, Any]] = []
    for slot, scopes in groups.items():
        distinct: dict[tuple[str, str, str, str], EntityScope] = {}
        for scope in scopes:
            semantic_value = (scope.label, scope.property, scope.operator, scope.value)
            distinct.setdefault(semantic_value, scope)
        if len(distinct) > 1:
            conflicts.append(
                {
                    "slot": slot,
                    "reason_code": ScopeSlotConflictError.reason_code,
                    "distinct_values": [list(value) for value in distinct],
                    "scopes": [asdict(scope) for scope in scopes],
                }
            )
        else:
            # Repeated equivalent constraints are one semantic assignment.
            assignments[slot] = next(iter(distinct.values()))
    return groups, assignments, [*unassigned, *conflicts]


def audit_ir_constraint_coverage(ir: ControlledQueryIR, template: IndependentTemplate) -> dict[str, Any]:
    """Audit every explicit bounded IR constraint against one template.

    The audit is intentionally contract-local: it inspects the selected
    skeleton and selection metadata only, never evaluation annotations or
    reference Cypher. Derived helper slots and template defaults therefore do
    not become artificial user constraints.
    """

    entity = audit_entity_constraint_coverage(ir, template)
    skeleton = template.skeleton
    return_clause = _return_clause(skeleton)
    service_semantics = _service_semantics_in_skeleton(skeleton)

    requested_relations = [str(item.get("semantic")) for item in ir.relation_semantics if item.get("semantic")]
    consumed_relations = [value for value in requested_relations if value.upper() in service_semantics]
    unconsumed_relations = [
        {"semantic": value, "reason": "template skeleton has no compatible service relation semantic"}
        for value in requested_relations
        if value not in consumed_relations
    ]

    explicit_targets = [
        label
        for label in ir.target_labels
        if ir.target_label_provenance.get(label) == "explicit_target_from_nl"
    ]
    aliases = _skeleton_label_aliases(skeleton)
    output_aliases = set(re.findall(r"\b([A-Za-z_]\w*)\.", return_clause))
    selection_targets = {str(value) for value in template.selection.get("target_labels", [])}
    consumed_targets: list[str] = []
    unconsumed_targets: list[dict[str, Any]] = []
    for label in explicit_targets:
        label_aliases = {alias for alias, value in aliases.items() if value == label}
        if label in selection_targets or label_aliases.intersection(output_aliases):
            consumed_targets.append(label)
        else:
            unconsumed_targets.append(
                {"label": label, "reason": "explicit target label is absent from the template output contract"}
            )

    requested_time = ir.time_range or {}
    consumed_start = not requested_time.get("start") or _time_bound_consumed(skeleton, "time_start")
    consumed_end = not requested_time.get("end") or _time_bound_consumed(skeleton, "time_end")
    time_unconsumed: list[dict[str, Any]] = []
    if requested_time.get("start") and not consumed_start:
        time_unconsumed.append({"bound": "start", "value": requested_time["start"], "reason": "start bound is not rendered by a time predicate"})
    if requested_time.get("end") and not consumed_end:
        time_unconsumed.append({"bound": "end", "value": requested_time["end"], "reason": "end bound is not rendered by a time predicate"})

    requested_aggregations = [
        {
            "function": str(item.get("function") or "").lower(),
            "field": item.get("field"),
            "distinct": bool(item.get("distinct")),
            "provenance": item.get("provenance"),
        }
        for item in ir.aggregation
        if item.get("function")
    ]
    aggregate_matches = re.findall(
        r"\b(count|collect|max|min|sum|avg)\s*\(([^)]*)\)", skeleton, flags=re.IGNORECASE
    )
    aggregate_functions = {function.lower() for function, _ in aggregate_matches}
    aggregate_aliases = _skeleton_label_aliases(skeleton)

    def aggregate_field_matches(field: str, expression: str) -> bool:
        if re.search(rf"\b{re.escape(field)}\b", expression):
            return True
        if "." not in field:
            return False
        label, property_name = field.split(".", 1)
        return any(
            re.search(rf"\b{re.escape(alias)}\.{re.escape(property_name)}\b", expression)
            for alias, candidate_label in aggregate_aliases.items()
            if candidate_label == label
        )

    def aggregate_compatible(item: dict[str, Any]) -> bool:
        function = item["function"]
        field = str(item.get("field") or "")
        if function not in aggregate_functions:
            return False
        if item.get("provenance") == "implicit_grouping_aggregation":
            return True
        def candidate_matches(expression: str, *, require_distinct: bool) -> bool:
            has_distinct = bool(re.search(r"\bDISTINCT\b", expression, re.I))
            if has_distinct != require_distinct:
                return False
            if field in {"", "*"}:
                return bool(re.fullmatch(r"\s*(?:DISTINCT\s+)?\*\s*", expression, re.I))
            return aggregate_field_matches(field, expression)

        field_matches = any(
            candidate_function.lower() == function
            and candidate_matches(expression, require_distinct=bool(item.get("distinct")))
            for candidate_function, expression in aggregate_matches
        )
        if not field_matches:
            return False
        if item.get("distinct"):
            return any(
                candidate_function.lower() == function
                and re.search(r"\bDISTINCT\b", expression, re.I)
                and (field in {"", "*"} or aggregate_field_matches(field, expression))
                for candidate_function, expression in aggregate_matches
            )
        return True

    consumed_aggregations = [item for item in requested_aggregations if aggregate_compatible(item)]
    unconsumed_aggregations = [
        {**item, "reason": "requested aggregate function is absent from the template skeleton"}
        for item in requested_aggregations
        if item not in consumed_aggregations
    ]

    requested_projection = []
    if ir.projection.get("property") and ir.provenance.get("projection"):
        requested_projection.append(str(ir.projection["property"]))
    consumed_projection = [property_name for property_name in requested_projection if re.search(rf"\b{re.escape(property_name)}\b", return_clause)]
    unconsumed_projection = [
        {"property": property_name, "reason": "explicit projection property is absent from the template output"}
        for property_name in requested_projection
        if property_name not in consumed_projection
    ]

    requested_scopes = [asdict(item) for item in ir.entity_scopes]
    consumed_scopes: list[dict[str, Any]] = []
    unconsumed_scopes: list[dict[str, Any]] = []
    scope_groups, scope_assignments, scope_slot_conflicts = _scope_slot_analysis(ir, template)
    conflict_by_slot = {str(item["slot"]): item for item in scope_slot_conflicts if item.get("slot")}
    for scope_item, scope in zip(requested_scopes, ir.entity_scopes):
        repo_prefix_conflict = next(
            (
                item
                for item in scope_slot_conflicts
                if item.get("reason_code")
                == RepoScopeTypedPrefixConflictError.reason_code
                and item.get("source_span") == scope_item.get("source_span")
            ),
            None,
        )
        if repo_prefix_conflict:
            unconsumed_scopes.append(
                {
                    **scope_item,
                    **repo_prefix_conflict,
                    "status": "ABSTAIN_WITH_TYPED_UNCONSUMED_REASON",
                    "reason": "explicit typed prefix conflicts with the repository-derived prefix",
                }
            )
            continue
        slots = _compatible_scope_slots(scope, template)
        if len(slots) != 1:
            reason_code = next(
                (
                    str(item["reason_code"])
                    for item in scope_slot_conflicts
                    if item.get("source_span") == scope_item.get("source_span")
                ),
                (
                    "UNSUPPORTED_NEGATED_TYPED_SCOPE_OPERATOR"
                    if scope.operator == "NOT_STARTS_WITH"
                    else "UNSUPPORTED_TYPED_SCOPE_OPERATOR"
                    if scope.provenance == "unsupported_typed_scope_operator_from_nl"
                    else "NO_COMPATIBLE_TYPED_SCOPE_SLOT"
                    if not slots
                    else "AMBIGUOUS_TYPED_SCOPE_SLOT"
                ),
            )
            unconsumed_scopes.append(
                {
                    **scope_item,
                    "status": "ABSTAIN_WITH_TYPED_UNCONSUMED_REASON",
                    "reason_code": reason_code,
                    "reason": (
                        "negative typed scope operator is not supported by the template contract"
                        if scope.operator == "NOT_STARTS_WITH"
                        else "explicit typed scope operator is not supported by the template contract"
                        if scope.provenance == "unsupported_typed_scope_operator_from_nl"
                        else "no unique compatible typed scope slot"
                    ),
                }
            )
            continue
        slot = slots[0]
        if slot in conflict_by_slot:
            unconsumed_scopes.append(
                {
                    **scope_item,
                    "status": "ABSTAIN_WITH_TYPED_UNCONSUMED_REASON",
                    "slot": slot,
                    "reason_code": ScopeSlotConflictError.reason_code,
                    "reason": "multiple distinct explicit values target one singular scope slot",
                }
            )
        elif slot in scope_assignments:
            consumed_scopes.append(
                {**scope_item, "status": "CONSUMED_BY_SELECTED_CONTRACT", "slot": slot}
            )

    requested_projection_items = [asdict(item) for item in ir.projection_items]
    contract_projection = template.projection_contract
    consumed_projection_items: list[dict[str, Any]] = []
    unconsumed_projection_items: list[dict[str, Any]] = []
    projection_contract_available = bool(contract_projection)
    unmatched_contract_projection = [dict(item) for item in contract_projection]
    remaining_contract_indices = list(range(len(contract_projection)))
    consumed_contract_indices: list[int] = []
    if projection_contract_available:
        for requested in requested_projection_items:
            match_index = next(
                (
                    index
                    for index, contract in enumerate(unmatched_contract_projection)
                    if str(contract.get("label") or "") == str(requested.get("label") or "")
                    and str(contract.get("property") or "entity_id") == str(requested.get("property") or "entity_id")
                    and str(contract.get("role") or requested.get("role")) == str(requested.get("role"))
                    and (
                        not requested.get("distinct")
                        or bool(contract.get("distinct"))
                        or _single_column_return_distinct_entails_item(template.skeleton, requested)
                    )
                    and (not requested.get("nullable") or bool(contract.get("nullable")))
                ),
                None,
            )
            if match_index is None:
                unconsumed_projection_items.append(
                    {**requested, "status": "ABSTAIN_WITH_TYPED_UNCONSUMED_REASON", "reason": "no compatible ordered projection contract item"}
                )
            else:
                contract_item = unmatched_contract_projection.pop(match_index)
                consumed_contract_indices.append(remaining_contract_indices.pop(match_index))
                consumed_projection_items.append(
                    {**requested, "status": "CONSUMED_BY_SELECTED_CONTRACT", "contract_item": contract_item}
                )
        if unmatched_contract_projection:
            # A contract may contain additional columns only when explicitly
            # marked as entailed by its family-level semantics.
            unjustified = [item for item in unmatched_contract_projection if not item.get("entailed")]
            if unjustified:
                unconsumed_projection_items.extend(
                    {
                        "label": item.get("label"),
                        "property": item.get("property", "entity_id"),
                        "status": "ABSTAIN_WITH_TYPED_UNCONSUMED_REASON",
                        "reason": "template would add an unrequested projection item",
                    }
                    for item in unjustified
                )
    elif requested_projection_items:
        # v4 compatibility: the ordered RETURN expressions themselves are the
        # legacy contract. D1.3a's v5 pack makes that contract explicit and
        # additionally checks for unrequested columns.
        alias_labels = _skeleton_label_aliases(template.skeleton)
        return_aliases = set(re.findall(r"\b([A-Za-z_]\w*)\.", return_clause))
        selection_targets = {str(value) for value in template.selection.get("target_labels", [])}
        for requested in requested_projection_items:
            label = str(requested.get("label") or "")
            property_name = str(requested.get("property") or "entity_id")
            property_matches = bool(re.search(rf"\b[A-Za-z_]\w*\.{re.escape(property_name)}\b", return_clause))
            label_matches = any(alias_labels.get(alias) == label for alias in return_aliases)
            if property_name == "url_domain_etld1":
                label_matches = label_matches or label in selection_targets
            distinct_matches = (
                not requested.get("distinct")
                or _single_column_return_distinct_entails_item(template.skeleton, requested)
            )
            if property_matches and label_matches and distinct_matches:
                consumed_projection_items.append(
                    {**requested, "status": "CONSUMED_BY_SELECTED_CONTRACT", "contract_item": "legacy_return_expression"}
                )
            else:
                unconsumed_projection_items.append(
                    {**requested, "status": "ABSTAIN_WITH_TYPED_UNCONSUMED_REASON", "reason": "legacy RETURN expression does not expose requested role/property"}
                )
    projection_order_matches = (
        consumed_contract_indices == sorted(consumed_contract_indices)
        and (not projection_contract_available or _projection_contract_order_matches_skeleton(template, return_clause))
    )
    projection_options = template.projection_options or {}
    return_tuple_distinct = _return_has_tuple_distinct(skeleton)
    contract_tuple_distinct = bool(
        projection_options.get("distinct")
        and str(projection_options.get("distinct_scope") or "").lower() == "tuple"
    )
    contract_entailed_tuple_distinct = contract_tuple_distinct and bool(
        projection_options.get("entailed")
    )
    single_column_item_distinct = (
        len(requested_projection_items) == 1
        and _single_column_return_distinct_entails_item(skeleton, requested_projection_items[0])
    )
    if ir.projection_distinct:
        projection_distinct_compatible = return_tuple_distinct or contract_tuple_distinct
        projection_distinct_reason = None if projection_distinct_compatible else (
            "requested tuple DISTINCT is absent from RETURN and projection contract"
        )
    elif single_column_item_distinct:
        # A one-expression RETURN DISTINCT proves uniqueness of that requested
        # item, but does not establish tuple DISTINCT semantics for a wider
        # projection contract.
        projection_distinct_compatible = True
        projection_distinct_reason = "single-column RETURN DISTINCT entails requested item uniqueness"
    elif return_tuple_distinct and not projection_contract_available:
        # Pre-v5 packs used the whole skeleton as their implicit contract.
        # Preserve that frozen compatibility path; once an explicit ordered
        # projection contract exists, a default tuple DISTINCT must be
        # separately declared as entailed in projection_options.
        projection_distinct_compatible = True
        projection_distinct_reason = "legacy skeleton contract (no ordered projection contract)"
    elif return_tuple_distinct:
        projection_distinct_compatible = contract_entailed_tuple_distinct
        projection_distinct_reason = None if projection_distinct_compatible else (
            "template adds tuple DISTINCT without an entailed projection contract"
        )
    else:
        projection_distinct_compatible = True
        projection_distinct_reason = None

    explicit_sorts = [
        item for item in ir.sort if item.get("provenance") == "explicit_sort_from_nl"
    ]
    order_clause_match = re.search(r"\bORDER\s+BY\b(.*?)(?:\bLIMIT\b|$)", skeleton, flags=re.IGNORECASE | re.DOTALL)
    order_clause = order_clause_match.group(1) if order_clause_match else ""
    consumed_sorts: list[dict[str, Any]] = []
    unconsumed_sorts: list[dict[str, Any]] = []
    for item in explicit_sorts:
        field = str(item.get("field") or "")
        order = str(item.get("order") or "").upper()
        field_present = bool(field) and bool(re.search(rf"\b{re.escape(field)}\b", order_clause))
        order_present = not order or bool(re.search(rf"\b{order}\b", order_clause, flags=re.IGNORECASE))
        if field_present and order_present:
            consumed_sorts.append(item)
        else:
            unconsumed_sorts.append({**item, "reason": "explicit sort is absent or incompatible with template ORDER BY"})

    explicit_limit = ir.explicit_limit
    limit_consumed = explicit_limit is None or bool(re.search(r"\bLIMIT\s+\d+\b", skeleton, flags=re.IGNORECASE))
    limit_unconsumed = [] if limit_consumed else [{"limit": explicit_limit, "reason": "template has no LIMIT contract"}]
    unsupported_cardinality_values = {
        int(value)
        for item in ir.unsupported_explicit_constraints
        if item.get("kind") == "unsupported_cardinality_surface"
        for value in re.findall(r"\d+", str(item.get("surface_text") or ""))
    }
    unnormalized_limit_unconsumed = [
        {"limit": value, "reason": "unsupported explicit cardinality phrase does not match the template default"}
        for value in ir.unnormalized_limit_values
        if ir.explicit_limit != value
        and (template.default_limit != value or value in unsupported_cardinality_values)
    ]
    unsupported_explicit_constraints = {
        "requested": list(ir.unsupported_explicit_constraints),
        "consumed": [],
        "unconsumed": list(ir.unsupported_explicit_constraints),
    }

    accepted = bool(
        entity["accepted"]
        and not unsupported_explicit_constraints["unconsumed"]
        and not unconsumed_relations
        and not unconsumed_targets
        and not time_unconsumed
        and not unconsumed_aggregations
        and not unconsumed_projection
        and not unconsumed_scopes
        and not unconsumed_projection_items
        and projection_order_matches
        and projection_distinct_compatible
        and not unconsumed_sorts
        and not limit_unconsumed
        and not unnormalized_limit_unconsumed
    )
    return {
        "accepted": accepted,
        "unsupported_explicit_constraints": unsupported_explicit_constraints,
        "entity": entity,
        "relation_semantics": {
            "requested": requested_relations,
            "consumed": consumed_relations,
            "unconsumed": unconsumed_relations,
        },
        "target_labels": {
            "requested": explicit_targets,
            "consumed": consumed_targets,
            "unconsumed": unconsumed_targets,
            "provenance": {label: ir.target_label_provenance.get(label) for label in explicit_targets},
        },
        "time": {
            "requested_start": requested_time.get("start"),
            "requested_end": requested_time.get("end"),
            "consumed_start": consumed_start,
            "consumed_end": consumed_end,
            "unconsumed": time_unconsumed,
        },
        "aggregation": {
            "requested": requested_aggregations,
            "consumed": consumed_aggregations,
            "unconsumed": unconsumed_aggregations,
        },
        "projection": {
            "requested": requested_projection,
            "consumed": consumed_projection,
            "unconsumed": unconsumed_projection,
            "requested_items": requested_projection_items,
            "consumed_items": consumed_projection_items,
            "unconsumed_items": unconsumed_projection_items,
            "ordered_contract_available": projection_contract_available,
            "order_preserved": projection_order_matches,
            "tuple_distinct": {
                "requested": ir.projection_distinct,
                "return_clause_distinct": return_tuple_distinct,
                "contract_guarantees_tuple_distinct": contract_tuple_distinct,
                "contract_allows_implicit_tuple_distinct": contract_entailed_tuple_distinct,
                "accepted": projection_distinct_compatible,
                "reason": projection_distinct_reason,
            },
        },
        "entity_scopes": {
            "requested": requested_scopes,
            "consumed": consumed_scopes,
            "unconsumed": unconsumed_scopes,
            "slot_conflicts": scope_slot_conflicts,
            "slot_group_count": len(scope_groups),
        },
        "sort": {
            "requested": explicit_sorts,
            "consumed": consumed_sorts,
            "unconsumed": unconsumed_sorts,
            "derived_latest_projection": [
                item for item in ir.sort if item.get("provenance") == "aggregation_latest_projection"
            ],
        },
        "limit": {
            "explicit": explicit_limit,
            "consumed": limit_consumed,
            "unconsumed": limit_unconsumed,
            "unnormalized_explicit_values": ir.unnormalized_limit_values,
            "unnormalized_unconsumed": unnormalized_limit_unconsumed,
            "contract_default_entailed_values": [
                value for value in ir.unnormalized_limit_values if template.default_limit == value
                and ir.explicit_limit in {None, value}
                and value not in unsupported_cardinality_values
            ],
        },
        "reasons": (
            (["unsupported explicit NL constraint"] if unsupported_explicit_constraints["unconsumed"] else [])
            + (["entity constraint coverage failed"] if not entity["accepted"] else [])
            + (["unconsumed relation semantics"] if unconsumed_relations else [])
            + (["unconsumed target labels"] if unconsumed_targets else [])
            + (["unconsumed time bound"] if time_unconsumed else [])
            + (["unconsumed aggregation"] if unconsumed_aggregations else [])
            + (["unconsumed projection"] if unconsumed_projection else [])
            + (["unconsumed typed entity scope"] if unconsumed_scopes else [])
            + (["unconsumed ordered projection item"] if unconsumed_projection_items else [])
            + (["incompatible tuple DISTINCT semantics"] if not projection_distinct_compatible else [])
            + (["unconsumed explicit sort"] if unconsumed_sorts else [])
            + (["unconsumed explicit limit"] if limit_unconsumed else [])
            + (["unsupported explicit cardinality conflicts with template default"] if unnormalized_limit_unconsumed else [])
        ),
    }


def _template_score(
    template: IndependentTemplate,
    ir: ControlledQueryIR,
    coverage: dict[str, Any] | None = None,
) -> tuple[int, list[str]]:
    coverage = coverage or audit_ir_constraint_coverage(ir, template)
    if not coverage["accepted"]:
        entity = coverage["entity"]
        reasons: list[str] = []
        if entity["unconsumed"]:
            reasons.append("unconsumed direct entity constraint")
        if entity["conflicting"]:
            reasons.append("conflicting direct entity constraint")
        reasons.extend(coverage.get("reasons", []))
        return -100, list(dict.fromkeys(reasons or ["IR constraint coverage failed"]))
    if template.selection:
        selection = template.selection
        expected_intent = str(selection.get("intent_key") or "")
        if expected_intent and expected_intent != ir.intent_key:
            return -100, [f"intent mismatch: expected {expected_intent}, observed {ir.intent_key}"]
        has_contract_scope = any(
            scope.label in {"PullRequest", "Issue"}
            and any(
                item.get("label") == scope.label
                and item.get("property") == scope.property
                and item.get("operator") == scope.operator
                for item in template.scope_slots
            )
            for scope in ir.entity_scopes
        )
        if selection.get("requires_repo_scope") and not ir.repo_scope and not has_contract_scope:
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
            "candidate_ir_constraint_coverage": {
                template.template_id: audit_ir_constraint_coverage(ir, template)
                for template in templates
            },
        }
    scored: list[tuple[int, IndependentTemplate, list[str], dict[str, Any]]] = []
    coverage_by_template: dict[str, dict[str, Any]] = {}
    entity_coverage_by_template: dict[str, dict[str, Any]] = {}
    for template in templates:
        coverage = audit_ir_constraint_coverage(ir, template)
        coverage_by_template[template.template_id] = coverage
        entity_coverage_by_template[template.template_id] = coverage["entity"]
        score, reasons = _template_score(template, ir, coverage)
        if score >= 0:
            scored.append((score, template, reasons, coverage))
    scored.sort(key=lambda item: (-item[0], item[1].family, item[1].template_id))
    if not scored:
        has_conflict = any(
            item["entity"]["conflicting"] for item in coverage_by_template.values()
        )
        has_unconsumed = any(
            item.get("unsupported_explicit_constraints", {}).get("unconsumed")
            or item["entity"]["unconsumed"]
            or item["relation_semantics"]["unconsumed"]
            or item["target_labels"]["unconsumed"]
            or item["time"]["unconsumed"]
            or item["aggregation"]["unconsumed"]
            or item["projection"]["unconsumed"]
            or item["projection"].get("unconsumed_items")
            or item.get("entity_scopes", {}).get("unconsumed")
            or item["sort"]["unconsumed"]
            or item["limit"]["unconsumed"]
            or item["limit"].get("unnormalized_unconsumed")
            for item in coverage_by_template.values()
        )
        reason = (
            "unsupported explicit NL constraint"
            if any(item.get("unsupported_explicit_constraints", {}).get("unconsumed") for item in coverage_by_template.values())
            else "conflicting direct entity scope"
            if has_conflict
            else "unconsumed IR constraint"
            if has_unconsumed
            else "no compatible template contract"
        )
        return None, {
            "status": "abstain",
            "reason": reason,
            "candidate_entity_constraint_coverage": entity_coverage_by_template,
            "candidate_ir_constraint_coverage": coverage_by_template,
        }
    best_score = scored[0][0]
    tied = [x for x in scored if x[0] == best_score]
    if len(tied) > 1 and best_score <= 2:
        return None, {
            "status": "abstain",
            "reason": "ambiguous low-confidence template contract",
            "candidates": [x[1].template_id for x in tied],
            "candidate_entity_constraint_coverage": entity_coverage_by_template,
            "candidate_ir_constraint_coverage": coverage_by_template,
        }
    chosen = tied[0]
    return chosen[1], {
        "status": "selected",
        "score": chosen[0],
        "reasons": chosen[2],
        "candidates": [{"template_id": x[1].template_id, "score": x[0]} for x in scored],
        "entity_constraint_coverage": chosen[3]["entity"],
        "ir_constraint_coverage": chosen[3],
        "candidate_entity_constraint_coverage": entity_coverage_by_template,
        "candidate_ir_constraint_coverage": coverage_by_template,
    }


def _slot_values(ir: ControlledQueryIR, template: IndependentTemplate) -> dict[str, Any]:
    repo_prefix_conflicts = _repo_scope_typed_prefix_conflicts(ir)
    if repo_prefix_conflicts:
        conflict = repo_prefix_conflicts[0]
        raise RepoScopeTypedPrefixConflictError(
            repo_entity_id=str(conflict["repo_entity_id"]),
            scope_label=str(conflict["scope_label"]),
            expected_repo_prefix=str(conflict["expected_repo_prefix"]),
            explicit_scope_value=str(conflict["explicit_scope_value"]),
            scope_operator=str(conflict["scope_operator"]),
            source_span=list(conflict["source_span"]),
        )
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
    _, scope_assignments, scope_issues = _scope_slot_analysis(ir, template)
    conflicts = [
        item
        for item in scope_issues
        if item.get("reason_code") == ScopeSlotConflictError.reason_code
    ]
    if conflicts:
        conflict = conflicts[0]
        semantic_values = [tuple(str(part) for part in item) for item in conflict["distinct_values"]]
        raise ScopeSlotConflictError(str(conflict["slot"]), semantic_values)
    for slot, scope in scope_assignments.items():
        values[slot] = scope.value
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
