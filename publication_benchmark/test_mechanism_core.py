import pytest

from mechanism_core import Oracle, compile_call
from test_baselines import fixture_graph


def test_compiler_keeps_values_as_parameters_and_requires_all_conditions():
    call = {'operation': 'pr_link_count', 'arguments': {'pr_id': 'PR_1#1',
        'start_date': '2023-01-01', 'end_date': '2024-01-01', 'grain': 'records'}}
    result = compile_call(call)
    assert 'PR_1#1' not in result['query']
    assert result['parameters']['pr_id'] == 'PR_1#1'
    assert result['parameters']['end_date'] == '2024-01-01T00:00:00Z'
    with pytest.raises(ValueError):
        compile_call({'operation': call['operation'], 'arguments': {'pr_id': 'PR_1#1'}})
    with pytest.raises(ValueError):
        compile_call({'operation': call['operation'], 'arguments': {**call['arguments'], 'exclude': 'x'}})
    with pytest.raises(ValueError):
        compile_call({'operation': call['operation'], 'arguments': {**call['arguments'], 'start_date': '2023-02-30'}})


def test_record_grain_projection_and_distinct_are_not_interchangeable():
    nodes, edges = fixture_graph()
    oracle = Oracle(nodes, edges)
    a = {'pr_id': 'PR_1#1', 'fields': ['domain', 'external_resource_id'], 'distinct': False}
    assert oracle.expected({'operation': 'pr_links', 'arguments': a}) == [['example.org', 'E_1'], ['example.org', 'E_1']]
    assert oracle.expected({'operation': 'pr_links', 'arguments': {**a, 'distinct': True}}) == [['example.org', 'E_1']]
    assert oracle.expected({'operation': 'pr_links', 'arguments': {**a, 'fields': ['domain']}}) == [['example.org'], ['example.org']]
    a = {'pr_id': 'PR_1#1', 'start_date': '2023-01-01', 'end_date': '2024-01-01', 'grain': 'records'}
    assert oracle.expected({'operation': 'pr_link_count', 'arguments': a}) == [[2]]
    assert oracle.expected({'operation': 'pr_link_count', 'arguments': {**a, 'grain': 'distinct_targets'}}) == [[1]]


def test_path_grain_and_full_identifier_used_as_prefix_keep_literal_semantics():
    nodes, edges = fixture_graph()
    nodes['extra'] = ('Issue', 'I_1#10')
    ref = next(e for e in edges if e['target'][0] == 'Commit')
    oracle = Oracle(nodes, edges + [{**ref, 'uid': 'extra-reference'}])
    assert oracle.expected({'operation': 'issue_count', 'arguments': {'issue_prefix': 'I_1#1'}}) == [[2]]
    a = {'issue_prefix': 'I_1', 'grain': 'paths'}
    assert oracle.expected({'operation': 'issue_commit_path_count', 'arguments': a}) == [[2]]
    assert oracle.expected({'operation': 'issue_commit_path_count', 'arguments': {**a, 'grain': 'distinct_comments'}}) == [[1]]
    assert oracle.expected({'operation': 'issue_commit_path_count', 'arguments': {**a, 'grain': 'distinct_commits'}}) == [[1]]
