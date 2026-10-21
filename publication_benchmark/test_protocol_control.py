"""Meaningful checks for control isolation, accounting and refusal grading."""
import json
import pytest

import mechanism_core as m
import protocol_control as p
import run_protocol_control as r


def test_progress_can_update_without_rewriting_raw_evidence(tmp_path):
    progress=tmp_path/'progress.json'
    r.derived(progress,{'status':'RUNNING','done':1})
    r.derived(progress,{'status':'RUNNING','done':2})
    assert json.loads(progress.read_text())['done']==2
    raw=tmp_path/'request.json'
    r.r.store(raw,{'question':'fixed'})
    with pytest.raises(ValueError,match='Immutable'):
        r.r.store(raw,{'question':'changed'})


def test_same_compiler_and_contract_are_used():
    assert p.s.CATALOG is m.CATALOG
    assert p.s.compile_call is m.compile_call
    assert len(m.CATALOG)==6
    assert p.PROTOCOL_PROMPT.startswith(p.s.PROMPTS['S'])


def test_refusal_does_not_count_as_answering_graph_answerable_task():
    spec={'expected_status':'ANSWER','expected_catalog_status':'UNSUPPORTED','service_expressible':False,
          'columns':['count'],'expected_rows':[[5]]}
    reply={'prediction':{'status':'UNSUPPORTED','operation':None,'arguments':None}}
    obs={'execution':{'status':'NOT_EXECUTED'}}
    grade=r.grade(spec,reply,obs)
    assert grade['safe_catalog_refusal']
    assert grade['catalog_decision_correct']
    assert not grade['correct_task_completion_or_boundary']
    assert not grade['wrong_returned_answer']


def test_budget_slots_are_not_reused_and_cap_is_enforced(tmp_path):
    assert r.reserve(tmp_path,tmp_path/'one',maximum=2)==1
    assert r.reserve(tmp_path,tmp_path/'two',maximum=2)==2
    with pytest.raises(RuntimeError,match='BUDGET_EXHAUSTED'):
        r.reserve(tmp_path,tmp_path/'three',maximum=2)
    assert json.loads((tmp_path/'0001.json').read_text())['request_path'].endswith('one')


def test_protocol_clarification_still_uses_strict_parser():
    assert p.s.parse('{"status":"UNSUPPORTED","operation":null,"arguments":null}','S')['status']=='UNSUPPORTED'
    with pytest.raises(ValueError):
        p.s.parse('{"status":"UNSUPPORTED","operation":null,"arguments":null,"reason":"x"}','S')
