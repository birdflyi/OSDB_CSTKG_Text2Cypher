"""Malformed model structures must remain format errors, not runtime failures."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.plans import parse


@pytest.mark.parametrize('field,value', [
    ('nodes', ['n']), ('edges', [None]), ('where', ['invalid']),
    ('where', [dict(field='n.id', op=[], value=1)]),
    ('returns', [None]), ('returns', [dict(field='n.id', alias=[])]),
    ('returns', [dict(field='n.id', alias='id', agg=['count'])]),
    ('order_by', dict(column='id', direction='ASC')),
    ('order_by', [None]), ('order_by', None),
])
def test_bad_nested_structure_is_format_error(field, value):
    call = dict(nodes=[dict(id='n', label='Entity')],
                returns=[dict(field='n.id', alias='id')])
    call[field] = value
    reply = json.dumps(dict(status='ANSWER', calls=[call], combine=dict(op='single')))
    assert parse(reply).status == 'format_error'


@pytest.mark.parametrize('calls,combine', [([None], dict(op='single')),
                                         ([{}], dict(op=[]))])
def test_invalid_call_or_combine_does_not_escape(calls, combine):
    assert parse(json.dumps(dict(status='ANSWER', calls=calls, combine=combine))).status == 'format_error'
