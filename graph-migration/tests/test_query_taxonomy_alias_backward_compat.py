from pathlib import Path
import sys

from loaders.real_query_loader import (  # noqa: E402
    load_real_pilot_queries,
)


def test_pr_belongs_alias_maps_to_repo_scope_label(tmp_path: Path) -> None:
    taxonomy = """query_types:
  - label: l1_pr_in_repo_scope
    aliases: [pr_repo, pr-belongs, l1_pr_belongs_to_repo]
"""
    queries = """{"id":"q1","nl_query":"Which repo does PR belong to?","query_type":"pr-belongs","gold_cypher":"MATCH (n) RETURN n"}
{"id":"q2","nl_query":"Which repo does PR belong to?","query_type":"l1_pr_belongs_to_repo","gold_cypher":"MATCH (n) RETURN n"}
"""
    tax_path = tmp_path / "query_taxonomy.yaml"
    qry_path = tmp_path / "queries.jsonl"
    tax_path.write_text(taxonomy, encoding="utf-8")
    qry_path.write_text(queries, encoding="utf-8")

    result = load_real_pilot_queries(qry_path, tax_path)
    assert len(result.all_records) == 2
    assert result.all_records[0].query_type == "l1_pr_in_repo_scope"
    assert result.all_records[1].query_type == "l1_pr_in_repo_scope"
