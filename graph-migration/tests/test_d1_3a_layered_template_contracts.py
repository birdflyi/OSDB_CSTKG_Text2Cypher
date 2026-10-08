from __future__ import annotations

from pathlib import Path
import sys

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "graph-migration"))
sys.path.insert(0, str(ROOT / "experiment-harness" / "d1_3a"))

from runners.independent_controlled_pipeline import load_independent_templates  # noqa: E402
from template_provenance import (  # noqa: E402
    resolved_template_bundle_sha256,
    template_dependency_closure,
)


INHERITED_ID = "indv4_actor_multi_target_reference"
SCOPE_SLOTS = [
    {"label": "Repo", "property": "entity_id", "operator": "starts_with", "slot": "repo_scope_prefix"}
]
PROJECTION_CONTRACT = [
    {"role": "target_entity", "label": "Repo", "property": "entity_id"},
    {"role": "target_entity", "label": "ExternalResource", "property": "entity_id"},
]
PROJECTION_OPTIONS = {"distinct": True, "distinct_scope": "tuple", "entailed": True}


def _base_template(**overrides: object) -> dict[str, object]:
    item: dict[str, object] = {
        "template_id": INHERITED_ID,
        "family": "synthetic",
        "intent": "synthetic inherited contract",
        "cypher_skeleton": "MATCH (r:Repo) RETURN DISTINCT r.entity_id",
        "required_slots": [],
        "scope_slots": SCOPE_SLOTS,
        "projection_contract": PROJECTION_CONTRACT,
        "projection_options": PROJECTION_OPTIONS,
        "selection": {"base": True, "keep": "parent"},
    }
    item.update(overrides)
    return item


def _write_pack(path: Path, payload: dict[str, object]) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _inherited(templates):
    return next(template for template in templates if template.template_id == INHERITED_ID)


def test_child_without_contract_entry_inherits_parent_template_fields(tmp_path: Path) -> None:
    base = tmp_path / "v4.yaml"
    child = tmp_path / "v5.yaml"
    _write_pack(base, {"templates": [_base_template()]})
    _write_pack(child, {"extends": "v4.yaml", "templates": []})

    inherited = _inherited(load_independent_templates(child))
    assert inherited.scope_slots == SCOPE_SLOTS
    assert inherited.projection_contract == PROJECTION_CONTRACT
    assert inherited.projection_options == PROJECTION_OPTIONS
    assert inherited.selection == {"base": True, "keep": "parent"}


def test_child_selection_override_preserves_other_contract_fields(tmp_path: Path) -> None:
    base = tmp_path / "base.yaml"
    child = tmp_path / "child.yaml"
    _write_pack(base, {"templates": [_base_template()]})
    _write_pack(
        child,
        {
            "extends": "base.yaml",
            "templates": [],
            "template_contracts": {INHERITED_ID: {"selection": {"keep": "child"}}},
        },
    )

    inherited = _inherited(load_independent_templates(child))
    assert inherited.selection == {"base": True, "keep": "child"}
    assert inherited.scope_slots == SCOPE_SLOTS
    assert inherited.projection_contract == PROJECTION_CONTRACT
    assert inherited.projection_options == PROJECTION_OPTIONS


def test_projection_options_merge_and_explicit_projection_list_replaces_parent(tmp_path: Path) -> None:
    base = tmp_path / "base.yaml"
    child = tmp_path / "child.yaml"
    replacement = [{"role": "target_entity", "label": "Actor", "property": "entity_id"}]
    _write_pack(base, {"templates": [_base_template()]})
    _write_pack(
        child,
        {
            "extends": "base.yaml",
            "templates": [],
            "template_contracts": {
                INHERITED_ID: {
                    "projection_options": {"entailed": False},
                    "projection_contract": replacement,
                }
            },
        },
    )

    inherited = _inherited(load_independent_templates(child))
    assert inherited.projection_options == {
        "distinct": True,
        "distinct_scope": "tuple",
        "entailed": False,
    }
    assert inherited.projection_contract == replacement
    assert inherited.scope_slots == SCOPE_SLOTS


def test_explicit_empty_list_is_an_intentional_contract_clear(tmp_path: Path) -> None:
    base = tmp_path / "base.yaml"
    child = tmp_path / "child.yaml"
    _write_pack(base, {"templates": [_base_template()]})
    _write_pack(
        child,
        {
            "extends": "base.yaml",
            "templates": [],
            "template_contracts": {INHERITED_ID: {"projection_contract": []}},
        },
    )

    inherited = _inherited(load_independent_templates(child))
    assert inherited.projection_contract == []
    assert inherited.scope_slots == SCOPE_SLOTS


def test_three_layer_chain_preserves_v5_contract_and_hashes_all_dependencies(tmp_path: Path) -> None:
    v4 = tmp_path / "independent_template_pack_v4.yaml"
    v5 = tmp_path / "independent_template_pack_v5.yaml"
    v6 = tmp_path / "independent_template_pack_v6.yaml"
    v6_added = {
        "template_id": "synthetic_v6_added",
        "family": "synthetic",
        "intent": "new child-only template",
        "cypher_skeleton": "MATCH (a:Actor) RETURN a.entity_id",
        "required_slots": [],
    }
    _write_pack(v4, {"templates": [_base_template(scope_slots=[], projection_contract=[], projection_options={})]})
    _write_pack(
        v5,
        {
            "extends": v4.name,
            "templates": [],
            "template_contracts": {
                INHERITED_ID: {
                    "scope_slots": SCOPE_SLOTS,
                    "projection_contract": PROJECTION_CONTRACT,
                    "projection_options": PROJECTION_OPTIONS,
                }
            },
        },
    )
    _write_pack(v6, {"extends": v5.name, "templates": [v6_added]})

    templates = load_independent_templates(v6)
    inherited = _inherited(templates)
    ids = [template.template_id for template in templates]
    assert ids == [INHERITED_ID, "synthetic_v6_added"]
    assert len(ids) == len(set(ids))
    assert inherited.scope_slots == SCOPE_SLOTS
    assert inherited.projection_contract == PROJECTION_CONTRACT
    assert inherited.projection_options == PROJECTION_OPTIONS

    records_before = template_dependency_closure(v6, tmp_path)
    hash_before = resolved_template_bundle_sha256(records_before)
    assert [record["path"] for record in records_before] == [
        v4.name,
        v5.name,
        v6.name,
    ]
    assert records_before == template_dependency_closure(v6, tmp_path)

    v5_payload = yaml.safe_load(v5.read_text(encoding="utf-8"))
    v5_payload["template_contracts"][INHERITED_ID]["projection_options"]["entailed"] = False
    _write_pack(v5, v5_payload)
    records_after = template_dependency_closure(v6, tmp_path)
    assert records_after[0] == records_before[0]
    assert records_after[1]["path"] == v5.name
    assert records_after[1]["sha256"] != records_before[1]["sha256"]
    assert records_after[2] == records_before[2]
    assert resolved_template_bundle_sha256(records_after) != hash_before


def test_layered_pack_rejects_duplicate_template_ids(tmp_path: Path) -> None:
    base = tmp_path / "base.yaml"
    child = tmp_path / "child.yaml"
    _write_pack(base, {"templates": [_base_template()]})
    _write_pack(
        child,
        {"extends": base.name, "templates": [_base_template(intent="duplicate") ]},
    )

    with pytest.raises(ValueError, match="duplicate template_id"):
        load_independent_templates(child)
