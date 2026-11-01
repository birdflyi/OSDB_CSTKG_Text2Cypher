from contextlib import contextmanager
import json
from pathlib import Path
import sys
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
import feedback_consumer as c

def rows(n=2):
    return [dict(cell_id='c',position_id=str(i),method='direct-feedback') for i in range(n)]

def result(row,folder,score=None):
    return dict(row,status='EXECUTED',final=c.save(folder/'answer.json',dict(status='EXECUTED',complete=True,rows=[[1]])),score=score or dict(status='scored',value=True))

def test_disconnect_during_score_reopens_and_retries_only_current_case(tmp_path,monkeypatch):
    opened=[];calls=[]
    @contextmanager
    def factory(*args):
        opened.append(1);yield object()
    def run(rs,sources,backend,folder,on_result):
        calls.append(rs[0]['position_id'])
        if len(calls)==1:
            on_result(result(rs[0],folder,dict(status='scoring_error',error_type='UnknownExecution')))
            raise c.UnknownExecution('reset')
        on_result(result(rs[0],folder))
    monkeypatch.setattr(c,'run_group',run)
    values=c.consume_graph('b','g',rows(),{}, {},tmp_path/'out',factory,sleep=lambda _:None)
    assert calls==['0','0','1'] and len(opened)==2
    assert len(values)==2 and all(v['score']['status']=='scored' for v in values)

def test_completed_score_survives_cleanup_disconnect(tmp_path,monkeypatch):
    calls=[];opened=[]
    @contextmanager
    def factory(*args):opened.append(1);yield object()
    def run(rs,sources,backend,folder,on_result):
        calls.append(rs[0]['position_id']);on_result(result(rs[0],folder))
        if len(calls)==1:raise c.UnknownExecution('cleanup reset')
    monkeypatch.setattr(c,'run_group',run)
    values=c.consume_graph('b','g',rows(),{}, {},tmp_path/'out',factory,sleep=lambda _:None)
    assert calls==['0','1'] and len(opened)==2
    assert values[0]['cleanup_transport_failed'] is True

def test_startup_retry_is_bounded_and_preserves_denominator(tmp_path):
    opened=[]
    @contextmanager
    def factory(*args):
        opened.append(1);raise c.UnknownExecution('not ready');yield
    values=c.consume_graph('b','g',rows(),{}, {},tmp_path/'out',factory,sleep=lambda _:None)
    assert len(opened)==3 and len(values)==2
    assert all(v['status']=='graph_paused' for v in values)

def test_integrity_failure_is_not_retried(tmp_path,monkeypatch):
    @contextmanager
    def factory(*args):yield object()
    def run(*args,**kw):raise c.IntegrityError('changed seal')
    monkeypatch.setattr(c,'run_group',run)
    with pytest.raises(c.IntegrityError):
        c.consume_graph('b','g',rows(),{}, {},tmp_path/'out',factory,sleep=lambda _:None)

def test_persistent_disconnect_exhausts_case_then_continues(tmp_path,monkeypatch):
    @contextmanager
    def factory(*args):yield object()
    calls=[]
    def run(rs,sources,backend,folder,on_result):
        calls.append(rs[0]['position_id'])
        if rs[0]['position_id']=='0':raise c.UnknownExecution('reset')
        on_result(result(rs[0],folder))
    monkeypatch.setattr(c,'run_group',run)
    values=c.consume_graph('b','g',rows(),{}, {},tmp_path/'out',factory,sleep=lambda _:None)
    assert calls==['0','0','1']
    assert values[0]['recovery_exhausted'] and values[1]['status']=='EXECUTED'
