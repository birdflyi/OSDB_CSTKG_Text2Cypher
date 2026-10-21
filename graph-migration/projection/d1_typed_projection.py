from __future__ import annotations

import ast
import csv
import hashlib
import ipaddress
import json
import re
from collections import Counter, defaultdict
from functools import lru_cache
from importlib.metadata import version
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlsplit

from models import NormalizedEdge, NormalizedGraph, NormalizedNode
from normalizers.eventaction_service import map_eventaction_service_rel
from normalizers.time_utils import normalize_event_time

VERSION = "d1_typed_v1_2"
QUARANTINE_LABEL = "D1UnresolvedIdentity"
DOMAIN_VERSION = "tldextract==3.2.0;ICANN_ONLY;BUNDLED_SNAPSHOT;NO_CACHE_NO_NETWORK"
PSL_SHA256 = "e86b831f26b65b20a18e78f848db04fe380ac13c2680af12305cda2395b3c71f"
_TYPE_MAP = {
    "actor": "Actor", "user": "Actor", "developer": "Actor",
    "repo": "Repo", "repository": "Repo", "issue": "Issue",
    "issuecomment": "IssueComment", "issue_comment": "IssueComment", "comment": "IssueComment",
    "pullrequest": "PullRequest", "pull_request": "PullRequest", "pr": "PullRequest",
    "pullrequestreview": "PullRequestReview", "pull_request_review": "PullRequestReview",
    "pullrequestreviewcomment": "PullRequestReviewComment",
    "pull_request_review_comment": "PullRequestReviewComment",
    "commit": "Commit", "commitcomment": "CommitComment", "commit_comment": "CommitComment",
    "branch": "Branch", "tag": "Tag", "release": "Release", "push": "Push", "gollum": "Gollum",
    "externalresource": "ExternalResource", "external_resource": "ExternalResource",
    "unknownobject": "UnknownObject", "unknown_object": "UnknownObject",
}
# Formats mirror entity_abbr_map.yaml and real_csv_preprocess._build_exid_by_type.
# A prefix is a consistency check, never a type inference or repair instruction.
_ID_PATTERNS = {
    "Actor": r"A_[0-9]+", "Repo": r"R_[0-9]+", "Issue": r"I_[0-9]+#[0-9]+",
    "PullRequest": r"PR_[0-9]+#[0-9]+", "IssueComment": r"IC_[0-9]+#[0-9]+#[0-9]+",
    "PullRequestReview": r"PRR_[0-9]+#[0-9]+#prr-[0-9]+",
    "PullRequestReviewComment": r"PRRC_[0-9]+#[0-9]+#r[0-9]+",
    "Commit": r"C_[0-9]+@[0-9a-fA-F]{40}",
    "CommitComment": r"CC_[0-9]+@[0-9a-fA-F]{40}#r[0-9]+",
    "Branch": r"B_[0-9]+:[^\s<>\"']+", "Tag": r"T_[0-9]+-[^\s<>\"']+",
    "Release": r"RE_[0-9]+-[0-9]+", "Push": r"P_[0-9]+\.[0-9]+",
}
_EXTERNAL_PATTERNS = {"GitHub_Service_External_Links", "GitHub_Other_Service"}
_REFERENCE_PATTERNS = {
    "Issue_PR", "SHA", "CommitComment", "Gollum", "Release",
    "GitHub_Files_FileChanges", "GitHub_GenSer_Other_Links", "Branch_Tag_GHDir",
}
_URL_RE = re.compile(r"https?://[^\s\]\)\"'>,;]+", re.IGNORECASE)
_NULL_IDS = {"", "nan", "none", "null", "na", "n/a", "unknown"}


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _first(row: dict[str, Any], *keys: str) -> str:
    return next((_text(row.get(key)) for key in keys if _text(row.get(key)).lower() not in _NULL_IDS), "")


def _typed_label(raw_type: Any) -> tuple[str, str]:
    key = _text(raw_type).lower()
    if key in _TYPE_MAP:
        return _TYPE_MAP[key], "typed_mapping"
    if key in {"object", "artifact"}:
        return "UnknownObject", "generic_object_type"
    return "UnknownObject", "unrecognized_type" if key else "missing_type"


def _unreliable_reason(value: str) -> str:
    if value.lower() in _NULL_IDS:
        return "MISSING_ID"
    if re.search(r"(?:^|[_:#@.\-])(?:none|nan|null|unknown)(?:$|[_:#@.\-])", value, re.I):
        return "PLACEHOLDER_COMPONENT"
    return ""


def _parse_object_properties(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    for parser in (json.loads, ast.literal_eval):
        try:
            parsed = parser(_text(value))
            if isinstance(parsed, dict):
                return parsed
        except (ValueError, SyntaxError, TypeError):
            pass
    return {}


@lru_cache(maxsize=1)
def _domain_parser() -> Any:
    try:
        import tldextract

        snapshot = Path(tldextract.__file__).parent / ".tld_set_snapshot"
        if version("tldextract") != "3.2.0" or hashlib.sha256(snapshot.read_bytes()).hexdigest() != PSL_SHA256:
            return None
        return tldextract.TLDExtract(
            cache_dir=None, suffix_list_urls=(), fallback_to_snapshot=True,
            include_psl_private_domains=False,
        )
    except (ImportError, OSError):
        return None


@lru_cache(maxsize=8192)
def _url_info(match_text: str) -> dict[str, Any]:
    urls = list(dict.fromkeys(_URL_RE.findall(_text(match_text))))
    result = {
        "url_raw": urls[0] if len(urls) == 1 else "", "url_host": "",
        "url_domain_etld1": "", "url_status": "NO_URL" if not urls else "AMBIGUOUS_URLS",
        "domain_status": "NOT_COMPUTED", "domain_rule_version": DOMAIN_VERSION,
        "psl_sha256": PSL_SHA256,
    }
    if len(urls) != 1:
        return result
    try:
        parsed = urlsplit(urls[0])
        host = (parsed.hostname or "").rstrip(".").encode("idna").decode("ascii").lower()
        _ = parsed.port  # Reject malformed ports, credentials and invalid host syntax.
        if not host or parsed.username is not None or parsed.password is not None:
            raise ValueError("missing host or credentials")
        try:
            ipaddress.ip_address(host)
            result.update(url_host=host, url_status="VALID_URL", domain_status="IP_ADDRESS_NO_ETLD1")
            return result
        except ValueError:
            pass
        if any(not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label) for label in host.split(".")):
            raise ValueError("invalid hostname")
        result.update(url_host=host, url_status="VALID_URL")
    except (ValueError, UnicodeError):
        result.update(url_status="INVALID_URL", domain_status="INVALID_URL")
        return result
    parser = _domain_parser()
    if parser is None:
        result["domain_status"] = "PINNED_PSL_UNAVAILABLE"
        return result
    parts = parser(host)
    if not parts.suffix or not parts.domain:
        result["domain_status"] = "UNKNOWN_SUFFIX_OR_PUBLIC_SUFFIX_ONLY"
    else:
        result.update(url_domain_etld1=parts.registered_domain, domain_status="PINNED_PSL_ICANN")
    return result


def _github_host(host: str) -> bool:
    return any(host == domain or host.endswith("." + domain) for domain in ("github.com", "githubusercontent.com")) or host == "github-redirect.dependabot.com"


def _reference_service_rel_from_pattern(pattern: str, match_text: str) -> str:
    # Known source pattern semantics take precedence over incidental URL text.
    if pattern in _EXTERNAL_PATTERNS:
        return "LINKS_TO"
    if pattern in {"Repo", "Actor"}:
        return "MENTIONS"
    if pattern in _REFERENCE_PATTERNS:
        return "REFERENCES"
    url = _url_info(match_text)
    if not pattern and url["url_status"] == "VALID_URL" and not _github_host(url["url_host"]):
        return "LINKS_TO"
    return ""


class D1TypedProjection:
    """Preserve every source row; only qualified identities enter execution CSVs."""

    mode = "d1_typed"

    def __init__(self) -> None:
        self.unresolved: list[dict[str, Any]] = []
        self.quarantined_records: list[dict[str, Any]] = []
        self.counters: Counter[str] = Counter()
        self.endpoints: dict[tuple[int, str], dict[str, Any]] = {}

    def project(self, rows: Iterable[dict[str, Any]]) -> NormalizedGraph:
        self.__init__()
        rows = list(rows)
        claims: dict[str, set[str]] = defaultdict(set)
        for row in rows:
            for prefix, side in (("src", "source"), ("tar", "target")):
                identity = _first(row, f"{prefix}_entity_id", f"{side}_entity_id", f"{prefix}_id")
                label, _ = _typed_label(_first(row, f"{prefix}_entity_type", f"{side}_entity_type", f"{prefix}_type"))
                if not _unreliable_reason(identity) and label != "UnknownObject":
                    claims[identity].add(label)
        conflicts = {key for key, labels in claims.items() if len(labels) > 1}
        graph = NormalizedGraph()
        for index, row in enumerate(rows):
            self.counters["input_rows"] += 1
            source = self._endpoint(row, "source", index, conflicts)
            target = self._endpoint(row, "target", index, conflicts)
            self.endpoints[index, "source"] = source
            self.endpoints[index, "target"] = target
            raw_type = _first(row, "relation_type", "edge_type", "relation")
            self.counters[f"raw_relation:{raw_type or '<missing>'}"] += 1
            relation = {"Reference": "REFERENCE", "EventAction": "EVENT_ACTION"}.get(raw_type)
            if relation is None:
                self.quarantined_records.append({"record_index": index, "reason": "UNSUPPORTED_RAW_RELATION", "raw_record": row})
                self.counters["unmaterialized_rows"] += 1
                continue
            for endpoint in (source, target):
                graph.add_node(NormalizedNode(endpoint["uid"], endpoint["label"], endpoint["id"], endpoint["properties"]))
            props = self._edge_properties(row, relation, source, target, index)
            graph.add_edge(NormalizedEdge(source["uid"], target["uid"], relation, props))
            self.counters[f"relation:{relation}"] += 1
            if not props["query_eligible"]:
                self.quarantined_records.append({
                    "record_index": index, "reason": "INELIGIBLE_ENDPOINT_OR_SERVICE", "raw_record": row,
                    "source_status": source["properties"]["identity_status"],
                    "target_status": target["properties"]["identity_status"],
                })
        graph.skipped_records = self.counters["unmaterialized_rows"]
        self.counters["multi_type_entity_ids"] = len(conflicts)
        self.counters.setdefault("unmaterialized_rows", 0)
        return graph

    def _endpoint(self, row: dict[str, Any], side: str, index: int, conflicts: set[str]) -> dict[str, Any]:
        prefix = "src" if side == "source" else "tar"
        identity = _first(row, f"{prefix}_entity_id", f"{side}_entity_id", f"{prefix}_id")
        raw_type = _first(row, f"{prefix}_entity_type", f"{side}_entity_type", f"{prefix}_type")
        label, type_status = _typed_label(raw_type)
        pattern = _text(row.get("tar_entity_match_pattern_type")) if side == "target" else ""
        match_text = _text(row.get("tar_entity_match_text")) if side == "target" else ""
        url = _url_info(match_text)
        reason = _unreliable_reason(identity)
        status = "EXPLICIT_SOURCE_ID_FORMAT_COMPATIBLE"
        # A URL-derived identity requires Reference + resource semantics, not arbitrary context.
        external = (
            side == "target" and _first(row, "relation_type", "edge_type", "relation") == "Reference"
            and url["url_status"] == "VALID_URL" and label in {"UnknownObject", "ExternalResource"}
            and (pattern in _EXTERNAL_PATTERNS or (not pattern and not _github_host(url["url_host"])))
            and type_status != "unrecognized_type"
        )
        entity_id = identity
        if reason and external:
            entity_id, label, status, reason = url["url_raw"], "ExternalResource", "URL_DERIVED_RESOURCE", ""
        elif not reason:
            if identity in conflicts:
                reason = "MULTIPLE_TYPE_CLAIMS"
            elif label in _ID_PATTERNS and not re.fullmatch(_ID_PATTERNS[label], identity):
                reason = "TYPE_ID_CONFLICT"
            elif label == "ExternalResource" and (not external or identity != url["url_raw"]):
                reason = "UNVERIFIED_EXTERNAL_ID"
            elif label == "UnknownObject" and type_status in {"typed_mapping", "generic_object_type"}:
                # The schema permits this label but supplies no canonical-ID
                # admission rule. A nonempty token, a known-type ID prefix, or
                # row-local claims of verification are not independent identity
                # evidence. Keep the claim for audit; do not invent a format,
                # infer a type, or create a query anchor from it.
                reason = "UNKNOWNOBJECT_IDENTITY_NOT_VERIFIED"
            elif type_status in {"missing_type", "unrecognized_type"} or label == "Gollum":
                reason = "TYPE_OR_FORMAT_NOT_VALIDATED"
        eligible = not reason
        props: dict[str, Any] = {
            "raw_entity_id": identity, "raw_entity_type": raw_type, "declared_label": label,
            "projection_mode": self.mode, "projection_version": VERSION,
            "type_mapping_status": type_status, "query_eligible": eligible,
            "identity_status": reason or status, "identity_validation": "SOURCE_AND_FORMAT_ONLY_NO_REMOTE_LOOKUP",
        }
        for suffix in ("id", "type"):
            aggregate = _first(row, f"{prefix}_entity_{suffix}_agg", f"{side}_entity_{suffix}_agg")
            if aggregate:
                props[f"aggregate_entity_{suffix}"] = aggregate
        if not eligible:
            # Separate label and namespace: frozen templates cannot accidentally traverse these.
            entity_id, output_label = f"unresolved:{side}:{index}", QUARANTINE_LABEL
            self.unresolved.append({
                "record_index": index, "side": side, "raw_relation_type": row.get("relation_type"),
                "raw_entity_id": identity, "raw_entity_type": raw_type, "declared_label": label,
                "reason": reason, "query_eligible": False,
                "alternative_evidence": _parse_object_properties(row.get("tar_entity_objnt_prop_dict")) if side == "target" else {},
            })
        else:
            output_label = label
        props["entity_id"] = entity_id
        if side == "target" and url["url_raw"]:
            # Domain is relationship-owned; node keeps only URL identity evidence.
            props.update(url_raw=url["url_raw"], url_host=url["url_host"])
        return {"id": entity_id, "label": output_label, "uid": f"{output_label}:{entity_id}", "properties": props}

    def _edge_properties(self, row: dict[str, Any], relation: str, source: dict[str, Any], target: dict[str, Any], index: int) -> dict[str, Any]:
        event_time = _first(row, "event_time", "timestamp", "time")
        verb, service = "", ""
        if relation == "EVENT_ACTION":
            verb, service = map_eventaction_service_rel(
                _first(row, "relation_label_repr", "relation_label"), _first(row, "event_trigger", "trigger"), s_core=None,
            )
            if service in {"REFERS_TO", "RESOLVES", "COUPLES_WITH"}:
                service = ""
        else:
            service = _reference_service_rel_from_pattern(_text(row.get("tar_entity_match_pattern_type")), _text(row.get("tar_entity_match_text")))
        raw_json = json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        raw_sha = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()
        props: dict[str, Any] = {
            "native_rel_type": relation, "service_rel_type": service, "eventaction_verb_label": verb,
            "raw_relation_type": row.get("relation_type"), "raw_relation_label_repr": row.get("relation_label_repr"),
            "raw_event_type": row.get("event_type"), "raw_event_trigger": row.get("event_trigger"),
            "source_event_id": _first(row, "source_event_id", "event_id"), "record_index": index,
            "raw_record_json": raw_json, "source_record_sha256": raw_sha,
            "edge_uid": f"{VERSION}:{index}:{raw_sha}",
            "event_time": event_time or None, "source_event_time": self._event_time(event_time),
            "projection_version": VERSION,
            "source_identity_status": source["properties"]["identity_status"],
            "target_identity_status": target["properties"]["identity_status"],
            "query_eligible": bool(source["properties"]["query_eligible"] and target["properties"]["query_eligible"] and service),
        }
        if relation == "REFERENCE":
            props.update(_url_info(_text(row.get("tar_entity_match_text"))))
            # An unknown domain must remain absent (NULL in Cypher), never a false suffix or empty bucket.
            if not props["url_domain_etld1"]:
                del props["url_domain_etld1"]
            props.update(tar_entity_match_text=row.get("tar_entity_match_text"), tar_entity_match_pattern_type=row.get("tar_entity_match_pattern_type"))
        return {k: v for k, v in props.items() if v is not None}

    @staticmethod
    def _event_time(value: str) -> str | None:
        try:
            return normalize_event_time(value)
        except (ValueError, OverflowError, OSError):
            return None

    def report(self) -> dict[str, Any]:
        return {
            "projection_mode": self.mode, "projection_version": VERSION,
            "unresolved_count": len(self.unresolved),
            "unresolved_by_side_relation_type_reason": dict(Counter(
                "|".join(str(case.get(k) or "") for k in ("side", "raw_relation_type", "raw_entity_type", "reason"))
                for case in self.unresolved
            )),
            "quarantined_record_count": len(self.quarantined_records),
            "counters": dict(sorted(self.counters.items())),
            "domain_rule_version": DOMAIN_VERSION, "psl_sha256": PSL_SHA256,
        }


def run_d1_typed_projection(input_path: str | Path, output_dir: str | Path) -> dict[str, Any]:
    from exporters.csv_exporter import CsvExporter
    from projection.d1_contract_adapter import csv_to_import_cypher, static_coverage, unresolved_triage

    input_path, output_dir = Path(input_path).resolve(), Path(output_dir).resolve()
    if output_dir.exists():
        raise FileExistsError(f"Refusing to overwrite existing staging: {output_dir}")
    with input_path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    projector = D1TypedProjection()
    graph = projector.project(rows)
    output_dir.mkdir(parents=True, exist_ok=False)
    # Preservation CSV contains every supported row, including isolated identities.
    CsvExporter().export(graph, output_dir / "csv")
    execution = NormalizedGraph()
    for node in graph.nodes.values():
        if node.properties["query_eligible"]:
            execution.add_node(node)
    for edge in graph.edges:
        if edge.properties["query_eligible"]:
            execution.add_edge(edge)
    CsvExporter().export(execution, output_dir / "execution_csv")
    adapter = csv_to_import_cypher(output_dir / "execution_csv", output_dir / "execution_import.cypher")
    coverage = static_coverage(execution)
    triage = unresolved_triage(projector, input_path)
    summary = {
        "summary": graph.to_dict()["summary"], "execution_summary": execution.to_dict()["summary"],
        **projector.report(), "input": str(input_path),
        "input_sha256": hashlib.sha256(input_path.read_bytes()).hexdigest(),
        "node_labels": dict(Counter(n.label for n in graph.nodes.values())),
        "execution_node_labels": dict(Counter(n.label for n in execution.nodes.values())),
        "native_relations": dict(Counter(e.rel_type for e in graph.edges)),
        "execution_native_relations": dict(Counter(e.rel_type for e in execution.edges)),
        "service_relations": dict(Counter(e.properties["service_rel_type"] for e in graph.edges)),
        "domain_status": dict(Counter(e.properties.get("domain_status") for e in graph.edges if e.rel_type == "REFERENCE")),
        "time_present_by_native_relation": dict(Counter(e.rel_type for e in graph.edges if e.properties.get("source_event_time"))),
        "adapter": adapter, "runtime_status": "RUNTIME_NOT_VERIFIED",
    }
    artifacts = {"projection_summary.json": summary, "b1_static_coverage.json": coverage, "unresolved_triage.json": triage}
    for name, data in artifacts.items():
        with (output_dir / name).open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
    for name, cases in (("unresolved_cases.jsonl", projector.unresolved), ("quarantined_records.jsonl", projector.quarantined_records)):
        with (output_dir / name).open("w", encoding="utf-8", newline="\n") as handle:
            for case in cases:
                handle.write(json.dumps(case, ensure_ascii=False) + "\n")
    return summary
