from build_semantic_fixtures import build
from run_semantic_fixture_checks import mutations


def test_fixture_is_deterministic_and_boundary_records_are_explicit():
    specs = [{'reference_call': {'arguments': {'pr_id': 'PR_1#2', 'start_date': '2023-01-01', 'end_date': '2024-01-01'}}}]
    assert build(specs, 'boundary') == build(specs, 'boundary')
    nodes, base_edges = build(specs, 'base')
    _, boundary_edges = build(specs, 'boundary')
    _, repeated_edges = build(specs, 'multiplicity')
    assert len(base_edges) < len(boundary_edges) < len(repeated_edges)
    text = str(boundary_edges)
    assert '2022-12-31T23:59:59Z' in text and '2024-01-01T00:00:00Z' in text


def test_actual_query_mutants_change_only_declared_fragments():
    query = 'MATCH (p)-[r:REFERENCE]->(x) WHERE true AND r.source_event_time >= $start_date AND r.source_event_time < $end_date RETURN count(r)'
    changed = mutations(query)
    assert set(changed) == {'drop_lower', 'drop_upper', 'drop_window', 'records_to_distinct_targets'}
    assert '$start_date' not in changed['drop_lower'] and '$end_date' in changed['drop_lower']
