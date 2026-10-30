import asyncio
from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from adapter_work import preparation, request_pipeline as pipeline, cyquark_training as training
from common import direct, ri
from common.dispatch import request_item
from engine.config import canonical, Endpoint
from engine.errors import IntegrityError
from engine.fake import FakeTransport
from engine.tests.test_scheduler import make_scheduler


def test_scope_rejects_orphan_controls_before_writing(tmp_path):
    with pytest.raises(ValueError, match='requires ours'):
        preparation.build(tmp_path/'materials',tmp_path/'output',methods=['correct-detail'])
    assert not (tmp_path/'output').exists()


def test_cyquark_needs_an_exact_graph_binding():
    bindings={'cyquark':dict(model='wrong-global-checkpoint',ready=True)}
    with pytest.raises(ValueError,match='graph-specific'):
        pipeline.method_binding(bindings,'cyquark','CypherBench','movie')
    bindings['cyquark']['graphs']={'CypherBench/movie':dict(model='movie',ready=False)}
    assert pipeline.method_binding(bindings,'cyquark','CypherBench','movie')['model']=='movie'


def test_shared_positions_never_become_database_results():
    positions=[dict(cell_id='direct',method='direct',benchmark='b',position_id='p',logical_key='source'),
               dict(cell_id='feedback',position_id='p',source=dict(method='direct',benchmark='b',position_id='p'))]
    paths=[dict(logical_key='source',method='direct',benchmark='b',position_id='p',final=dict(status='ready'))]
    row=pipeline.position_results(positions,paths)[1]
    assert row['status']=='dependency_bound' and row['database_started'] is False
    assert pipeline.position_results(positions,[])[1]['status']=='dependency_pending'
    # Ordinary per-case errors omit benchmark/position metadata in the engine.
    paths=[dict(logical_key='source',final=dict(status='case_error'))]
    assert pipeline.position_results(positions,paths)[1]['status']=='dependency_failed'


def test_request_matrix_uses_pool_without_database(tmp_path):
    async def scenario():
        original,_,transport,ledger=await make_scheduler(tmp_path,cap=3,transport=FakeTransport(0.03))
        original.http_workers.shutdown();original.db_workers.shutdown()
        jobs,positions=[],[]
        for n in range(11):
            spec=dict(benchmark='b',method='direct',task_id=str(n),graph='g',position_id=str(n),
                      generation_stage='initial',messages=[dict(role='user',content=str(n))])
            item=request_item(spec,model='m',sampling={})
            jobs.append(pipeline.Job('cell-a' if n<2 else 'cell-b',str(n),direct,spec,item,{}))
            positions.append(dict(method='direct',benchmark='b',position_id=str(n),logical_key=item.key))
        result=await pipeline.execute(original.spec,jobs,positions,original.pool,ledger,transport,simulated=True)
        assert result['stop_reason'] is None and len(result['positions'])==11
        assert all(r['status']=='answered' for r in result['positions'])
        assert transport.calls==11 and 1<transport.peak<=3
        assert result['real_network_calls']==0 and not result['database_started']
        assert not ledger.summary()['pending']
    asyncio.run(scenario())


def test_ri_two_stages_use_one_request_campaign(tmp_path):
    source=Path(r'D:\ch7\reports\t1_preparation_20261029\adapter_positions_v22\cells\cypher-ri__CypherBench.positions.json')
    spec=ri.prepare(source,'CypherBench','M07-B01',limit=1,total_tokens=40)[0]
    def response(call,wire,endpoint):
        body=json.loads(wire)
        text='<answer>RETURN 1</answer>'
        if body.get('stop') == ['</json>']:
            text='<json>'+json.dumps(dict(name=spec.schema.graph,entities=[],relations=[]))+'</json>'
        return dict(status='ok',content=text,http_status=200,usage={'completion_tokens':10},finish_reason='stop')
    async def scenario():
        original,_,transport,ledger=await make_scheduler(tmp_path,cap=2,transport=FakeTransport(0,response))
        original.http_workers.shutdown();original.db_workers.shutdown()
        await original.pool.update([Endpoint('a','http://127.0.0.1/not-called','m','unused',2,
                                    wire_params=('model','prompt','max_tokens','stop'))])
        item=request_item(spec,model='m',sampling={})
        result=await pipeline.execute(original.spec,[pipeline.Job('ri','p',ri,spec,item,{})],
                     [dict(position_id='p',logical_key=item.key)],original.pool,ledger,transport,simulated=True)
        assert result['positions'][0]['status']=='ready' and transport.calls==2
        assert len(ledger.records)==2
    asyncio.run(scenario())


def test_training_parsers_do_not_silently_accept_bad_filter_outputs():
    assert training.paraphrases('1) A?\n2) B?\n3) C?\n[END]')==['A?','B?','C?']
    assert training.filter_result('<sense>yes</sense>')
    assert not training.filter_result('<sense>no</sense>')
    with pytest.raises(ValueError): training.filter_result('<sense>yes</sense><sense>no</sense>')
    with pytest.raises(ValueError): training.paraphrases('1) A\n2) A\n3) A\n[END]')


def training_row():
    return dict(source_id='generated-1',benchmark='b',graph='g',split='train',question='How many?',
                cypher='RETURN 1',messages=[dict(role='user',content='How many?')],logical_filter_passed=True,
                provenance={k:'hash-or-revision' for k in ('generator_revision','schema_sha256','snapshot_sha256',
                  'paraphrase_response_sha256','filter_response_sha256','prompt_profile_sha256')})


def test_training_excludes_exact_heldout_questions_and_rejects_wrong_graph(tmp_path):
    path=tmp_path/'rows.jsonl';row=training_row()
    path.write_text(json.dumps(row)+'\n',encoding='utf8')
    rows,rejected=training.checked_rows(path,benchmark='b',graph='g',heldout_questions=['HOW  MANY?'])
    assert not rows and rejected[0]['reason']=='exact_heldout_question_overlap'
    with pytest.raises(ValueError,match='Wrong graph'):
        training.checked_rows(path,benchmark='b',graph='other')


def test_training_masks_exact_template_boundary_and_never_truncates():
    tok=SimpleNamespace(apply_chat_template=lambda messages,add_generation_prompt,**kw:
                        [1,2,3] if add_generation_prompt else [1,2,3,4,5])
    assert training.encode_example(tok,training_row(),5)['labels']==[-100,-100,-100,4,5]
    assert training.encode_example(tok,training_row(),4) is None
    tok.apply_chat_template=lambda messages,add_generation_prompt,**kw: [1,2] if add_generation_prompt else [1,9,3]
    with pytest.raises(ValueError,match='boundary'):
        training.encode_example(tok,training_row(),10)
