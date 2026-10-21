import json
from types import SimpleNamespace
import pytest
from paced_transport import PacedPost


def test_failure_is_not_retried_and_next_start_remains_paced(tmp_path):
    now=[0.0];starts=[]
    def sleep(seconds):now[0]+=seconds
    def post(url,**kwargs):starts.append(now[0]);now[0]+=5;return SimpleNamespace(status_code=500)
    paced=PacedPost(post,'private-endpoint',tmp_path,clock=lambda:now[0],sleep=sleep)
    for _ in range(3):paced('private-endpoint',json={'task':'frozen'},headers={'Authorization':'secret'})
    assert starts==[60,120,180]
    assert len(list(tmp_path.glob('*.json')))==3
    assert 'secret' not in ''.join(p.read_text() for p in tmp_path.glob('*.json'))
    first=json.loads((tmp_path/'0001.json').read_text())
    assert first['pacing_wait_seconds']==60 and first['network_seconds']==5


def test_other_endpoints_are_not_paced(tmp_path):
    calls=[]
    paced=PacedPost(lambda url,**kwargs:calls.append(url),'model',tmp_path,sleep=lambda s:pytest.fail('Unexpected sleep'))
    paced('local-db')
    assert calls==['local-db'] and not list(tmp_path.glob('*.json'))


def test_transport_exception_is_recorded_and_propagated_once(tmp_path):
    now=[0.0];count=[0]
    def post(url,**kwargs):count[0]+=1;raise ConnectionError('private-message')
    paced=PacedPost(post,'model',tmp_path,clock=lambda:now[0],sleep=lambda s:now.__setitem__(0,now[0]+s))
    with pytest.raises(ConnectionError):paced('model',json={})
    assert count[0]==1
    assert 'private-message' not in (tmp_path/'0001.json').read_text()
