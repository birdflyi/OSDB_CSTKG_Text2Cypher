import json
from pathlib import Path
import pytest
from adapter_work.osdb_output import compare, normalized
from adapter_work.osdb_intake import build
from adapter_work.cohort_preparation import receipt
from adapter_work.cohort_runtime import validate_inventory
from adapter_work.score_position import score_position


def answer(rows, columns=('id',)):
    return dict(status='EXECUTED', complete=True, columns=list(columns), rows=rows)


LIST = dict(kind='list', columns=['id'], types=['string'], order='ordered', duplicates='forbid')


def test_types_order_duplicates_empty():
    gold = answer([['1'], ['2']])
    assert compare(gold, gold, LIST)
    assert not compare(gold, answer([['2'], ['1']]), LIST)
    assert not compare(gold, answer([[1], ['2']]), LIST)
    assert not compare(gold, answer([['1'], ['1']]), LIST)
    assert compare(answer([]), answer([]), LIST)
    assert not compare(answer([]), dict(status='ERROR', complete=False), LIST)


def test_bag_and_count():
    bag = dict(kind='rows', columns=['id'], types=['string'], order='bag', duplicates='retain')
    assert compare(answer([['a'], ['b'], ['a']]), answer([['b'], ['a'], ['a']]), bag)
    assert not compare(answer([['a'], ['a']]), answer([['a']]), bag)
    count = dict(kind='count', columns=['n'], types=['integer'], order='ordered', duplicates='forbid')
    assert compare(answer([[0]], ['n']), answer([[0]], ['n']), count)
    assert not compare(answer([[0]], ['n']), answer([], ['n']), count)
    with pytest.raises(ValueError):
        normalized(answer([[True]], ['n']), count)


def test_build_six_cells_and_score(tmp_path):
    def write(name, value):
        p = tmp_path/name
        p.write_text(json.dumps(value), encoding='utf8')
        return receipt(p)
    schema = write('schema.json', dict(node_props={}, relationships=[]))
    source = write('source.json', dict(raw='fixture'))
    valid = write('validation.json', dict(draft_id='fixture', status='PASS', csv_answer=['a'],
                  db_answer=['a'], layer='event', snapshot_sha256='a'*64))
    task = dict(task_id='fixture', graph='fixture', question='Return IDs in order.', schema=schema,
                source=source, reference_query='MATCH (n) RETURN n.id AS id ORDER BY id',
                answer=answer([['a']]), output_contract=LIST, validation=valid,
                layer='event', snapshot_sha256='a'*64)
    src = tmp_path/'tasks.jsonl'
    src.write_text(json.dumps(task)+'\n', encoding='utf8')
    result = build(src, tmp_path/'built')
    inv = json.loads(Path(result['inventory']['path']).read_bytes())
    validate_inventory(inv)
    assert result['counts']['cells'] == 6
    assert result['counts']['initial_requests'] == 2
    policies = json.loads(Path(result['scoring_contract']['path']).read_bytes())
    p = policies['positions'][0]['policy']
    score = score_position(p, task['answer'], dict(status='VALID', answer=task['answer']), method='direct')
    assert score['value'] is True
    assert score['metric_id'] == 'project_osdb_explicit_output_v2'
    # Public messages have no reference query or answer field.
    for file in (tmp_path/'built'/'inventory'/'public').glob('*.json'):
        assert 'reference_query' not in json.loads(file.read_bytes())


def test_reject_unbound_validation(tmp_path):
    valid = tmp_path/'validation.json'
    valid.write_text(json.dumps(dict(draft_id='x', status='PASS', csv_answer=[], db_answer=[])))
    schema = tmp_path/'schema.json'; schema.write_text('{}')
    task = dict(task_id='x', graph='g', question='q', schema=receipt(schema), source=receipt(schema),
                validation=receipt(valid), answer=answer([]), output_contract=LIST)
    src = tmp_path/'tasks.jsonl'; src.write_text(json.dumps(task))
    with pytest.raises(ValueError, match='snapshot'):
        build(src, tmp_path/'out')
