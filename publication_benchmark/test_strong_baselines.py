import json
from unittest.mock import patch

import pytest

import strong_baselines as s


def test_minimal_protocol_never_fills_fields():
    value = {'status': 'ANSWER', 'operation': 'issue_count', 'arguments': {'issue_prefix': 'I_1#'}}
    assert s.parse(json.dumps(value), 'S') == value
    with pytest.raises(ValueError):
        s.parse(json.dumps({'operation': 'issue_count', 'arguments': {}}), 'S')
    with pytest.raises(ValueError):
        s.parse(json.dumps({**value, 'id': 'extra'}), 'S')


def test_property_warning_is_repair_feedback_not_correctness_feedback():
    prediction = {'status': 'ANSWER', 'cypher': 'MATCH (a:Actor) RETURN a.actor_id', 'parameters': {}}
    warning = {'code': 'Neo.ClientNotification.Statement.UnknownPropertyKeyWarning'}
    with patch.object(s, 'transaction', return_value=({'notifications': [warning]}, 0, 1)):
        result = s.validate(prediction, 'G')
    assert result['repairable'] and result['status'] == 'SCHEMA_WARNING'


def test_semantically_wrong_but_structurally_valid_query_not_repaired():
    prediction = {'status': 'ANSWER', 'cypher': 'MATCH (i:Issue) RETURN count(i)', 'parameters': {}}
    with patch.object(s, 'transaction', return_value=({'results': [{'columns': ['count(i)'], 'data': [{'row': [3]}]}]}, 0, 1)):
        result = s.evaluate(prediction, 'G')
    assert result['execution']['rows'] == [[3]]
    assert not result['check']['repairable']


def test_examples_share_both_representations_and_no_test_input():
    sample = s.public_input({'question': 'TASK_ONLY', 'columns': ['count'], 'expected_rows': [[999]], 'reference_call': 'HIDDEN'})
    assert 'expected_rows' not in json.dumps(sample) and 'HIDDEN' not in json.dumps(sample)
    assert len(sample['paired_schematic_examples']) == 6
    for e in sample['paired_schematic_examples']:
        assert s.read_guard(e['direct']['cypher']) is None


def test_read_guard_blocks_mutation_but_not_keyword_inside_literal():
    assert s.read_guard('MATCH (n) DELETE n')
    assert s.read_guard("MATCH (n) WHERE n.entity_id='SET' RETURN n.entity_id") is None
