import pytest

from run_mechanism_pilot import parse_single_prediction


def test_singleton_presentation_acceptance_preserves_wrong_values():
    text = '{"id":"X","status":"ANSWER","count":17}'
    result = parse_single_prediction(text, ['X'], {'id', 'status', 'count'})
    assert result == [{'id': 'X', 'status': 'ANSWER', 'count': 17}]
    assert result == parse_single_prediction('```json\n[' + text + ']\n```', ['X'], {'id', 'status', 'count'})
    with pytest.raises(ValueError):
        parse_single_prediction(text, ['OTHER'], {'id', 'status', 'count'})
    with pytest.raises(ValueError):
        parse_single_prediction(text, ['X'], {'id', 'status', 'count', 'missing'})
    with pytest.raises(ValueError):
        parse_single_prediction('```json\n' + text + '\n```\n```json\n[]\n```', ['X'], {'id', 'status', 'count'})
