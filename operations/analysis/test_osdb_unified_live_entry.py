"""Only new live-entry wiring; synthetic responses, no model/DB/network."""
import ast,base64,json,subprocess,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import test_osdb_independent_path_cost_dev as costs_fixture
import test_osdb_campaign_dispatch_dev as campaign_fixture
from osdb_node_transport_live import NodeTransport,UnknownRemoteAttempt,node_worker_source
from osdb_model_limit_guard_dev import CentralModelGate,LimitedModelClient
from osdb_scoped_attempts_dev import immutable,read
from osdb_live_path_native_cost import PathCosts,timed_scheduled_path
from observed_neo4j_services_dev_v2 import UnsupportedPlan
from contextlib import contextmanager
from osdb_live_dispatch import run_campaign


class NewEntryTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.initial={str(i)+'__direct':[{'role':'system','content':'s'},{'role':'user','content':str(i)}] for i in range(48)}
        self.guard=patch('socket.socket',side_effect=AssertionError('No actual network'));self.socket=self.guard.start()
    def tearDown(self):
        self.assertEqual(self.socket.call_count,0);self.guard.stop();self.temp.cleanup()
    def test_worker_is_exact_fixed_one_shot_class_and_compiles(self):
        generated=node_worker_source();compile(generated,'node_model_once.py','exec')
        old=Path(__file__).with_name('osdb_budgeted_model_client_dev.py').read_text(encoding='utf-8-sig')
        extract=lambda s:next(n for n in ast.parse(s).body if isinstance(n,ast.ClassDef) and n.name=='OneShotHTTP')
        self.assertEqual(ast.dump(extract(old)),ast.dump(extract(generated)))
    def test_ascii_receipt_restores_exact_utf8_raw_response(self):
        raw='{"message":"中文"}'.encode('utf-8')
        def fake(command,**kwargs):
            envelope=json.loads(kwargs['input']);self.assertEqual(envelope['request_id'],'0__direct__initial')
            return subprocess.CompletedProcess(command,0,stdout=json.dumps({'request_id':envelope['request_id'],
                'http_status':200,'body':{'message':'中文'},'raw_response_base64':base64.b64encode(raw).decode()}).encode(),stderr=b'')
        transport=NodeTransport(['synthetic'],'/owned',self.initial,self.root/'io',run=fake)
        got=transport({'messages':self.initial['0__direct']})
        self.assertEqual(got['raw_response_bytes'],raw);self.assertEqual(got['body']['message'],'中文')
    def test_ssh_timeout_is_unknown_not_a_retryable_terminal(self):
        calls=[]
        def fake(*args,**kwargs):calls.append(1);raise subprocess.TimeoutExpired('synthetic',275)
        transport=NodeTransport(['synthetic'],'/owned',self.initial,self.root/'io',run=fake)
        with self.assertRaises(UnknownRemoteAttempt):transport({'messages':self.initial['0__direct']})
        self.assertEqual(calls,[1]);self.assertTrue(read(self.root/'io/0__direct__initial/ssh.json')['outcome_unknown'])
    def test_remote_unknown_preserves_scoped_pending_and_central_lock(self):
        f=campaign_fixture.CampaignTests();f.setUp()
        try:
            policy=f.root/'policy.json';immutable(policy,{'max_total_model_requests_inflight':64,'allow_128':False,
                'waiting_for_user_node_restart':False,'model_dispatch_during_restart':False})
            def unknown(body):raise UnknownRemoteAttempt('synthetic unknown')
            with CentralModelGate(policy,1) as gate:
                with f.lease() as lease:
                    client=LimitedModelClient(lease,f.private,f.initial,f.admission,transport=unknown,model_gate=gate)
                    with self.assertRaises(UnknownRemoteAttempt):client.generate(next(iter(f.initial)),'initial')
            self.assertEqual(f.legacy.inspect()['pending'],[1])
            self.assertTrue((f.root/'.t1-global-model-dispatch.lock').exists())
        finally:f.tearDown()
    def native_path(self,applicable):
        clock=costs_fixture.Clock();client=costs_fixture.Client(self.root,clock);costs=PathCosts(client.key,clock);lock=costs_fixture.Lock(clock)
        def physical(q,p):clock.advance(11);return {'status':'EXECUTED','complete':True,'columns':['n'],'rows':[[1]],'result_bytes':5}
        def execute(q,p):return costs.read(physical,q,p,lock,lambda:0)
        def answer(arm,record,schema,read):clock.advance(2);value=read('synthetic',{});clock.advance(3);return value
        def detail(arm,text,schema,read,strategy):clock.advance(5);value=read('synthetic',{});clock.advance(17);return value
        target='osdb_live_path_native_cost.'
        compile_value={'query':'synthetic native','parameters':{}}
        with patch(target+'outcome',answer),patch(target+'execute_response',detail),patch(target+'decode',return_value={}),\
             patch(target+'compile_native_set_count',return_value=compile_value,side_effect=None if applicable else UnsupportedPlan('outside native scope')):
            result=timed_scheduled_path(client,client.key,'ours',{},execute,costs)
        self.assertEqual(result['costs']['groups']['method']['root_wall_seconds'],27)
        self.assertEqual(result['costs']['groups']['full_detail_control']['root_wall_seconds'],40)
        self.assertEqual(client.calls,['initial'])
        return result
    def test_native_replay_cost_is_separate_and_has_no_extra_model_call(self):
        result=self.native_path(True)
        self.assertEqual(result['costs']['groups']['native_control']['root_wall_seconds'],18)
        self.assertTrue(result['native_control']['matches_ours'])
    def test_not_applicable_native_control_does_not_change_method_outcome(self):
        result=self.native_path(False)
        self.assertEqual(result['final']['status'],'EXECUTED')
        self.assertEqual(result['native_control']['status'],'CONTROL_NOT_APPLICABLE')
        self.assertEqual(result['costs']['groups']['native_control']['read_callbacks'],0)
    def test_live_guarded_dispatch_drains_two_paths_then_closes_one_graph(self):
        f=campaign_fixture.CampaignTests();f.setUp()
        try:
            policy=f.root/'policy.json';immutable(policy,{'max_total_model_requests_inflight':64,'allow_128':False,
                'waiting_for_user_node_restart':False,'model_dispatch_during_restart':False})
            with CentralModelGate(policy,2) as gate:
                with f.lease() as lease:
                    def transport(body):
                        value=f.transport(body)
                        if f.model_calls>=2:lease.stopped=True
                        return value
                    client=LimitedModelClient(lease,f.private,f.initial,f.admission,transport=transport,model_gate=gate)
                    @contextmanager
                    def graph(name):
                        with f.graph(name) as handle:
                            original=handle['execute']
                            def execute(q,p):
                                result=original(q,p)
                                return result
                            handle['execute']=execute;yield handle
                    result=run_campaign(client,f.manifest,f.root/'new_dispatch',graph,max_inflight=2,
                        graph_snapshot_hashes=f.hashes,evidence_kind='live_development')
            self.assertEqual(result['paths_completed'],2);self.assertEqual(result['undispatched_paths'],46)
            self.assertEqual(f.model_calls,2);self.assertEqual(f.db_calls,3)
            self.assertEqual(f.graph_events,[('open','redis'),('close','redis')]);self.assertEqual(f.legacy.inspect()['pending'],[])
            ours=read(f.root/'campaign/paths/redis_fixture0__ours.json')
            self.assertEqual(ours['native_control']['status'],'CONTROL_NOT_APPLICABLE')
        finally:f.tearDown()


if __name__=='__main__':unittest.main()
