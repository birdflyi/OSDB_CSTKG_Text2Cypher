import json
from pathlib import Path
import sys
from types import SimpleNamespace
import pytest
import requests
from run_retry_cohort import RetryCoordinator, physical_dispatch, cohort_summary


def test_retry_timing_and_raw_selection_preserve_first_failure(tmp_path):
    clock=[0.0]; starts=[]
    def physical(stage,arm,data):
        starts.append(clock[0]);clock[0]+=4
        return {'raw':{'http_status':500 if len(starts)==1 else 200},'failure':'INFRASTRUCTURE' if len(starts)==1 else None}
    coordinator=RetryCoordinator(tmp_path,physical,clock=lambda:clock[0],sleep=lambda seconds:clock.__setitem__(0,clock[0]+seconds))
    first,last,receipt=coordinator.dispatch('G_case_first','G',{})
    assert starts==[0,14] and first['failure']=='INFRASTRUCTURE' and last['failure'] is None
    assert receipt['physical_stages']==['G_case_first_attempt1','G_case_first_attempt2']
    assert coordinator.total_wait==10 and not coordinator.stopped


def test_global_three_failure_stop_forbids_another_logical_request(tmp_path):
    clock=[0.0]; calls=[]
    def physical(stage,arm,data):calls.append(stage);return {'raw':{'http_status':500},'failure':'INFRASTRUCTURE'}
    coordinator=RetryCoordinator(tmp_path,physical,clock=lambda:clock[0],sleep=lambda seconds:clock.__setitem__(0,clock[0]+seconds))
    coordinator.dispatch('G_case_first','G',{})
    assert len(calls)==3 and coordinator.stopped
    with pytest.raises(RuntimeError):coordinator.dispatch('P_case_first','P',{})
    assert len(calls)==3


def test_previous_physical_streak_is_not_reset_by_logical_boundary(tmp_path):
    calls=[]
    def physical(stage,arm,data):calls.append(stage);return {'raw':{'http_status':500},'failure':'INFRASTRUCTURE'}
    coordinator=RetryCoordinator(tmp_path,physical,sleep=lambda seconds:pytest.fail('No further retry allowed'))
    coordinator.consecutive=2
    coordinator.dispatch('P_case_first','P',{})
    assert len(calls)==1 and coordinator.stopped


@pytest.mark.parametrize('record',[{'http_status':200,'body':{'wrong':'answer'}},{'http_status':401}])
def test_completed_response_is_not_rerolled(tmp_path,record):
    calls=[]
    coordinator=RetryCoordinator(tmp_path,lambda *args:(calls.append(args) or {'raw':record}),sleep=lambda seconds:pytest.fail('No retry'))
    coordinator.dispatch('G_case_first','G',{})
    assert len(calls)==1


@pytest.mark.parametrize('arm',['G','P','I'])
def test_actual_attempt_charged_once_cached_decoder_makes_no_hidden_call(tmp_path,monkeypatch,arm):
    engine=Path(__file__).resolve().parents[2]/'OSDB_CSTKG_Text2Cypher_publication_e9'/'publication_benchmark'
    sys.path.insert(0,str(engine))
    import run_information_comparison as comparison
    cfg={'endpoint':'http://example.invalid','api_key':'private-not-for-output','model':'frozen-model','temperature':0.2,'top_p':0.95,'reasoning_effort':'low'}
    calls=[]
    pred={'status':'UNSUPPORTED',**({'cypher':None,'parameters':None} if arm=='G' else {'operation':None,'arguments':None})}
    def post(url,**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(status_code=200,headers={},request=requests.Request('POST',url,json=kwargs['json']).prepare(),json=lambda:{'choices':[{'finish_reason':'stop','message':{'content':json.dumps(pred)}}],'usage':{'total_tokens':10,'prompt_tokens':8,'completion_tokens':2}})
    monkeypatch.setattr(requests,'post',post)
    reply=physical_dispatch(tmp_path/'out',arm+'_case_first_attempt1',arm,{'question':'frozen'},cfg,tmp_path/'budget',comparison)
    assert reply['prediction']==pred and len(calls)==1
    assert len(list((tmp_path/'budget').glob('*.json')))==1
    saved=tmp_path/'out'/'calls'/(arm+'_case_first_attempt1')
    assert json.loads((saved/'request_wire.bin').read_bytes())==json.loads((saved/'request.json').read_text())
    assert not any('private-not-for-output' in p.read_text(errors='ignore') for p in tmp_path.rglob('*') if p.is_file())


def test_cost_accounting_charges_physical_attempts_not_three_views(tmp_path):
    engine=Path(__file__).resolve().parents[2]/'OSDB_CSTKG_Text2Cypher_publication_e9'/'publication_benchmark'
    sys.path.insert(0,str(engine))
    import analyze_continuation as accounting
    folder=tmp_path/'calls'/'G_case_first_attempt1';folder.mkdir(parents=True)
    (folder/'terminal.json').write_text(json.dumps({'http_status':200,'seconds':5,'body':{'usage':{'total_tokens':10,'prompt_tokens':8,'completion_tokens':2}}}))
    grade={'correct':True,'wrong_returned_answer':False,'safe_catalog_refusal':False}
    records=[{'arm':'G','view':view,'expected_status':'ANSWER','failure':None,'grade':grade} for view in ('FIRST_PHYSICAL','AFTER_TRANSPORT','AFTER_STRUCTURAL')]
    coordinator=SimpleNamespace(stopped=False,total_wait=0)
    summary=cohort_summary(tmp_path,records,1,coordinator,accounting)
    assert summary['actual_calls']==1 and summary['physical_usage']['known_total_tokens']==10
    assert summary['arm_physical_costs']['G']['known_total_tokens']==10
