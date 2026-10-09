from __future__ import annotations

from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "graph-migration"))

from runners.independent_controlled_pipeline import (  # noqa: E402
    UNSUPPORTED_EXPLICIT_CONSTRAINT_REASON,
    generate_independent,
    load_independent_schema,
    load_independent_templates,
    parse_nl_to_ir,
)


TEMPLATES = load_independent_templates(
    ROOT / "data_real" / "pilot_queries" / "independent_template_pack_v5.yaml"
)
SCHEMA = load_independent_schema(
    ROOT / "data_real" / "pilot_queries" / "schema_metadata.yaml"
)


@pytest.mark.parametrize(
    "query",
    [
        "Show issues whose IDs start with I_900001 but not I_900002",
        "Show issues whose IDs start with I_900001, but not I_900002",
        "List pull requests whose IDs start with PR_900001 but not PR_900002",
        "Show issues whose IDs start with I_900001 except I_900002",
        "Show issues whose IDs start with I_900001 excluding I_900002",
        "Show issues whose IDs start with I_900001 but not PR_900002",
    ],
)
def test_trailing_typed_exclusion_fails_closed(query: str) -> None:
    ir = parse_nl_to_ir("fix18", query)
    result = generate_independent("fix18", query, TEMPLATES, SCHEMA)

    assert ir.bounded_status == "ABSTAIN_UNSUPPORTED_EXPLICIT_CONSTRAINT"
    assert result.template_id is None
    assert result.rendered_cypher is None
    assert ir.unsupported_explicit_constraints
    item = ir.unsupported_explicit_constraints[0]
    assert item["kind"] == "unsupported_exclusion_surface"
    assert item["reason_code"] == UNSUPPORTED_EXPLICIT_CONSTRAINT_REASON
    assert item["provenance"] == "unsupported_explicit_constraint_from_nl"
    assert item["surface_text"]
    assert result.validation["selection"]["reason"] == "unsupported explicit NL constraint"


@pytest.mark.parametrize(
    "query",
    [
        "Show issues but not unrelated prose",
        "The process ran without duplicates during migration.",
        "Show issues except those whose IDs start with I_900001",
    ],
)
def test_trailing_exclusion_firewall_is_not_global_lexical_blacklist(query: str) -> None:
    ir = parse_nl_to_ir("fix18", query)
    assert not any(
        item["kind"] == "unsupported_exclusion_surface"
        for item in ir.unsupported_explicit_constraints
    )
