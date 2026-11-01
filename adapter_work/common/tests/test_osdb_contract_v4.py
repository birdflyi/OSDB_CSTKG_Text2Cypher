"""Check prospective OSDB syntax against the real parser and compiler."""
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / 'adapter_work'), str(ROOT / 'operations/analysis')]
from common.plans import parse
from common.prompts import compose_ours
from observed_neo4j_services_dev_v2 import compile_selection

PROFILE = ROOT / 'adapter_work/prompts/ours_osdb_contract_v4.json'


def test_ordered_distinct_list_compiles_without_repair():
    schema = {'entities': [{'label': 'ExampleEntity', 'identity_property': 'key',
                           'properties': {'key': 'str'}}], 'relations': []}
    messages = compose_ours('OSDB-STKG', dict(graph='synthetic', question='List keys', schema=schema), PROFILE)
    assert 'no untyped relationship scan' in messages[0]['content']
    call = dict(nodes=[dict(id='n', label='ExampleEntity')], edges=[], where=[],
                returns=[dict(field='n.key', alias='value')], distinct=True,
                order_by=[dict(column='value', direction='ASC')])
    plan = dict(status='ANSWER', calls=[call], combine=dict(op='single'))
    assert parse(json.dumps(plan)).status == 'answer'
    query = compile_selection(call, schema)['query']
    assert 'DISTINCT' in query and 'ORDER BY' in query
    call['order_by'] = [dict(field='value', direction='asc')]
    assert parse(json.dumps(plan)).status == 'format_error'


def test_profile_does_not_change_other_benchmarks():
    old = json.loads((PROFILE.parent / 'ours.json').read_bytes())
    new = json.loads(PROFILE.read_bytes())
    for benchmark in old.keys() - {'OSDB-STKG'}:
        assert new[benchmark] == old[benchmark]
