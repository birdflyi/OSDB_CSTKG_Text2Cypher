from __future__ import annotations

import importlib
import importlib.util
import re
from functools import lru_cache
from pathlib import Path
from typing import Any


_VERB_FROM_REL_RE = re.compile(r"EventAction::label=([A-Za-z0-9_]+)")


def _to_upper_snake(name: str) -> str:
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    s = re.sub(r"[^A-Za-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s.upper()


def canonical_service_rel_name(verb: str) -> str:
    v = str(verb or "").strip()
    if not v:
        return ""
    rel = _to_upper_snake(v)
    # Keep explicit _BY suffix for By verbs.
    if v.endswith("By") and not rel.endswith("_BY"):
        rel = rel + "_BY"
    return rel


@lru_cache(maxsize=1)
def load_event_trigger_triples_dict() -> dict[str, Any]:
    # Prefer project-local ER_config.py to keep two-track mapping deterministic
    # with repository-pinned rules.
    try:
        module_path_candidates = [
            Path(__file__).resolve().parents[2] / "data_scripts" / "ER_config.py",  # code/data_scripts/ER_config.py
            Path(__file__).resolve().parents[3] / "data_scripts" / "ER_config.py",  # project_root/data_scripts/ER_config.py
        ]
        for module_path in module_path_candidates:
            if not module_path.exists():
                continue
            spec = importlib.util.spec_from_file_location(
                "osdb_local_er_config", str(module_path)
            )
            if spec is None or spec.loader is None:
                continue
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            triples = getattr(mod, "event_trigger_ERE_triples_dict", None)
            if isinstance(triples, dict):
                return triples
    except Exception:
        pass

    # Fallback: installed GH_CoRE package module.
    try:
        mod = importlib.import_module("GH_CoRE.model.ER_config")
        triples = getattr(mod, "event_trigger_ERE_triples_dict", None)
        if isinstance(triples, dict):
            return triples
    except Exception:
        pass
    return {}


@lru_cache(maxsize=1)
def build_service_rel_mapping_from_er_config() -> dict[str, str]:
    triples = load_event_trigger_triples_dict()
    verbs: set[str] = set()
    for rels in triples.values():
        if not isinstance(rels, list):
            continue
        for triple in rels:
            if not (isinstance(triple, (list, tuple)) and len(triple) == 3):
                continue
            rel = str(triple[1])
            m = _VERB_FROM_REL_RE.search(rel)
            if m:
                verbs.add(m.group(1))
    return {v: canonical_service_rel_name(v) for v in sorted(verbs)}


def extract_eventaction_verb_from_label_repr(raw_relation_label_repr: str) -> str:
    text = str(raw_relation_label_repr or "").strip()
    if not text or "_" not in text:
        return ""
    parts = [p for p in text.split("_") if p]
    if len(parts) < 3:
        return ""
    # Pattern: <Src>_<Verb>_<Dst>
    return "_".join(parts[1:-1]) if len(parts) > 2 else ""


def extract_eventaction_verb_from_event_trigger(event_trigger: str) -> str:
    trigger = str(event_trigger or "").strip()
    if not trigger:
        return ""
    triples = load_event_trigger_triples_dict()
    rels = triples.get(trigger)
    if not isinstance(rels, list):
        return ""
    for triple in rels:
        if not (isinstance(triple, (list, tuple)) and len(triple) == 3):
            continue
        rel = str(triple[1])
        m = _VERB_FROM_REL_RE.search(rel)
        if m:
            return m.group(1)
    return ""


def map_eventaction_service_rel(
    raw_relation_label_repr: str,
    raw_event_trigger: str,
    s_core: set[str] | None = None,
) -> tuple[str, str]:
    """
    Returns (verb_label, service_rel_type).
    service_rel_type is empty if verb is unknown or outside s_core.
    """
    service_map = build_service_rel_mapping_from_er_config()
    verb = extract_eventaction_verb_from_label_repr(raw_relation_label_repr)
    if not verb:
        verb = extract_eventaction_verb_from_event_trigger(raw_event_trigger)
    if not verb:
        return "", ""

    mapped = service_map.get(verb, "")
    if not mapped:
        return verb, ""
    if s_core is not None and mapped not in s_core:
        return verb, ""
    return verb, mapped


def build_service_verbs_payload() -> dict[str, Any]:
    service_map = build_service_rel_mapping_from_er_config()
    s_core = sorted(service_map.values())
    payload: dict[str, Any] = {
        "verbs_universe": sorted(service_map.keys()),
        "service_rel_map": service_map,
        "S_core": s_core,
    }
    if len(s_core) > 25:
        payload["S_core_25"] = s_core[:25]
    else:
        payload["S_core_25"] = list(s_core)
    return payload
