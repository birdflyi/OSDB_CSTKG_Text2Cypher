"""Length/null normalization and unissued-only continuation; no real requests."""
from contextlib import contextmanager
from pathlib import Path
import time,unittest
from unittest.mock import patch
import test_osdb_campaign_dispatch_dev as fixtures
from osdb_budgeted_model_client_live_v2 import normalized_reply,outcome
from osdb_model_limit_guard_live_v2 import CentralModelGate,LimitedModelClient
from osdb_live_dispatch_v2 import run_campaign
from osdb_scoped_attempts_dev import immutable


class ContinuationTests(unittest.TestCase):
    def reply(self,reason):
        return {'http_status':200,'body':{'model':'ling-3.0-flash-n72-75','choices':[
            {'message':{'content':None,'reasoning_content':'synthetic reasoning'},'finish_reason':reason}],
            'usage':{'prompt_tokens':10,'completion_tokens':8192,'total_tokens':8202}}}
    def test_length_null_is_truncation_not_infrastructure_or_correct_answer(self):
        reply=normalized_reply(self.reply('length'),'ling-3.0-flash-n72-75')
        self.assertIsNone(reply['transport_error']);self.assertEqual(reply['finish_reason'],'length')
        self.assertEqual(reply['response_characters'],0);self.assertEqual(reply['usage']['completion_tokens'],8192)
        def forbidden(*args):self.fail('Truncated response must not execute a query')
        self.assertEqual(outcome('ours',reply,{},forbidden)['status'],'MODEL_TRUNCATED')
    def test_stop_null_remains_invalid_infrastructure_envelope(self):
        self.assertIsNotNone(normalized_reply(self.reply('stop'),'ling-3.0-flash-n72-75')['transport_error'])
    def test_prior_twelve_are_not_generated_or_graph_reopened(self):
        f=fixtures.CampaignTests();f.setUp()
        try:
            policy=f.root/'policy.json';immutable(policy,{'max_total_model_requests_inflight':64,'allow_128':False,
                'waiting_for_user_node_restart':False,'model_dispatch_during_restart':False})
            prior={}
            for row in f.rows[:12]:
                key=row['case_id']+'__'+row['arm'];p=f.root/'prior'/(key+'.json')
                immutable(p,{'final':{'status':'EXECUTED'},'costs':{'method_path_wall_seconds':1},'method_ended_epoch':time.time()-3600})
                prior[key]=p
            with CentralModelGate(policy,2) as gate:
                with f.lease() as lease:
                    def transport(body):
                        self.assertNotIn('redis_',body['messages'][1]['content'])
                        result=f.transport(body)
                        if f.model_calls>=2:lease.stopped=True
                        return result
                    client=LimitedModelClient(lease,f.private,f.initial,f.admission,transport=transport,model_gate=gate)
                    with patch('socket.socket',side_effect=AssertionError('No actual network')):
                        result=run_campaign(client,f.manifest,f.root/'new_dispatch',f.graph,max_inflight=2,
                            graph_snapshot_hashes=f.hashes,evidence_kind='synthetic',prior_paths=prior)
            self.assertEqual(f.model_calls,2);self.assertEqual(result['paths_completed'],14)
            self.assertEqual(result['prior_completed_paths'],12);self.assertEqual(result['current_block_initial_dispatched'],2)
            self.assertEqual(f.graph_events,[('open','duckdb'),('close','duckdb')]);self.assertEqual(f.legacy.inspect()['pending'],[])
        finally:f.tearDown()


if __name__=='__main__':unittest.main()
