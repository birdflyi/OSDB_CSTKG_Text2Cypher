import io
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0,r'D:\ch7\code\OSDB_CSTKG_Text2Cypher_publication_t1\experiment-harness\public_benchmark_v1')
from attempt_budget import AttemptBudget
from parallel_budget import ParallelLease
from common import immutable,rates
from capacity_worker import Pacer,response_metrics,baseline_ready,steady_metrics,dispatch_window_expired,recommend,campaign
from coordinator import optional_monitor_deploy
from monitor import render
from rich.console import Console


class LeaseTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        approval=self.root/'approval.json'
        immutable(approval,{'approved_cumulative_cap':64,'historical_attempts_at_approval':0})
        self.legacy=AttemptBudget(self.root/'ledger',approval)
    def tearDown(self):self.tmp.cleanup()

    def terminal(self,lease,i):
        ack=lease.reserved[str(i)]
        return {'budget_slot':ack['slot'],'http_status':200,'body':{'usage':{}},'seconds':.01}

    def test_parallel_unique_slots_and_idempotent_reserve(self):
        with ParallelLease(self.legacy,self.root/'lease',32) as lease:
            with ThreadPoolExecutor(max_workers=16) as pool:
                rows=list(pool.map(lambda i:lease.reserve(str(i),{'messages':[str(i)]}),range(32)))
            self.assertEqual(sorted(r['slot'] for r in rows),list(range(1,33)))
            self.assertEqual(lease.reserve('0',{'messages':['0']}),rows[0])
            self.assertEqual(lease.status()['campaign_reserved'],32)
            for i in range(32):lease.terminal(str(i),self.terminal(lease,i))
        self.assertEqual(self.legacy.inspect()['pending'],[])

    def test_conflicts_rejected_without_new_charge(self):
        with ParallelLease(self.legacy,self.root/'lease',2) as lease:
            lease.reserve('0',{'messages':['x']})
            with self.assertRaises(RuntimeError):lease.reserve('0',{'messages':['y']})
            t=self.terminal(lease,0);lease.terminal('0',t)
            self.assertTrue(lease.terminal('0',t)['duplicate'])
            with self.assertRaises(RuntimeError):lease.terminal('0',{**t,'seconds':2.})
        self.assertEqual(self.legacy.inspect()['reserved'],1)

    def test_pending_is_preserved_and_blocks_new_lease(self):
        with ParallelLease(self.legacy,self.root/'lease',1) as lease:
            lease.reserve('0',{'messages':['unknown']})
        self.assertEqual(self.legacy.inspect()['pending'],[1])
        with self.assertRaises(RuntimeError):
            with ParallelLease(self.legacy,self.root/'lease2',1):pass
        self.assertFalse((self.legacy.ledger/'.t1-reservation.lock').exists())

    def test_finite_cap_and_preexisting_lock_not_removed(self):
        with ParallelLease(self.legacy,self.root/'lease',1) as lease:
            lease.reserve('0',{'messages':['x']})
            with self.assertRaises(RuntimeError):lease.reserve('1',{'messages':['y']})
            lease.terminal('0',self.terminal(lease,0))
        lock=self.legacy.ledger/'.t1-reservation.lock';lock.write_text('existing-owner')
        with self.assertRaises(Exception):
            with ParallelLease(self.legacy,self.root/'lease2',1):pass
        self.assertEqual(lock.read_text(),'existing-owner')

    def test_terminal_without_reservation_or_wrong_slot(self):
        with ParallelLease(self.legacy,self.root/'lease',1) as lease:
            with self.assertRaises(RuntimeError):lease.terminal('missing',{'budget_slot':1})
            lease.reserve('0',{'messages':['x']})
            with self.assertRaises(RuntimeError):lease.terminal('0',{'budget_slot':99,'http_status':200})
            lease.terminal('0',self.terminal(lease,0))


class MetricsTests(unittest.TestCase):
    def test_campaign_stops_after_three_infrastructure_failures_without_retry(self):
        class LocalControl:
            def __init__(self,*args):self.n=0
            def call(self,route,body):
                if route=='/reserve':self.n+=1;return {'slot':self.n}
                return {'budget':{'pending':0}}
        def fail(*args):
            return {'http_status':503,'body':{},'seconds':.01,'ended_epoch':time.time()}
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);private=root/'private.json';manifest=root/'manifest.json'
            immutable(private,{'model':'fake'})
            import socket
            immutable(manifest,{'execution_hostname':socket.gethostname(),'jobs':[],
                'protocol':{'output_tokens_per_second_target':3500,'max_calls':32,
                    'stage_new_dispatch_time_limit_seconds':10},
                'stages':[{'name':'c16','concurrency':16,'count':32,'trials':[
                    {'trial_id':str(i),'arm':'direct','body':{},'question':'fixed'} for i in range(32)]}]})
            with patch('capacity_worker.Controller',LocalControl),patch('capacity_worker.model_request',fail):
                campaign(private,manifest,root/'output')
            result=json.loads((root/'output/summary.json').read_text(encoding='utf-8'))
            self.assertEqual(result['dispatched'],3)
            self.assertIn('3',result['stop_reason'])
            self.assertEqual(result['stages'][0]['undispatched'],29)

    def test_monitor_deploy_error_does_not_block_experiment(self):
        class Broken:
            def exec_command(self,*args,**kwargs):raise OSError('offline')
        result=optional_monitor_deploy(Broken(),'owned-monitor-command')
        self.assertTrue(result['experiment_continues'])
        self.assertFalse(result['monitor_deployed'])

    def test_insufficient_steady_sample_cannot_win_using_full_batch_rate(self):
        stage={'concurrency':32,'completed':192,'complete_responses':192,'HTTP200_responses':192,
            'steady_sample_sufficient':True,'steady_complete_cases_per_minute':40,'stop_reason':None}
        short={'concurrency':128,'completed':499,'complete_responses':490,'HTTP200_responses':495,
            'steady_sample_sufficient':False,'complete_cases_per_minute':100,'stop_reason':None}
        self.assertEqual(recommend([stage,short]),32)
        stage64={**stage,'concurrency':64,'completed':384,'complete_responses':384,'HTTP200_responses':384,
            'steady_complete_cases_per_minute':41}
        self.assertEqual(recommend([stage,stage64]),32)

    def test_bounded_pacing_and_normal_tail_drain(self):
        self.assertFalse(dispatch_window_expired(239,0))
        self.assertTrue(dispatch_window_expired(241,0))
        # Completion after the dispatch deadline is not an infrastructure error.
        row=response_metrics({'seconds':120,'ended_epoch':360,'http_status':200,
            'body':{'choices':[{'finish_reason':'stop','message':{'content':'ok'}}]}})
        self.assertTrue(row['complete'])

    def test_large_request_can_accumulate_more_than_one_second_credit(self):
        p=Pacer(3500);p.estimates['ours']=7000;p.last-=3
        self.assertEqual(p.delay('ours'),0)
        self.assertEqual(p.take('ours'),7000)

    def test_steady_window_excludes_startup_and_drain_without_deleting_failures(self):
        rows=[{'ended_epoch':i,'complete':True,'completion_tokens':100} for i in range(16)]
        rows[-1]['ended_epoch']=200;rows[-1]['complete']=False
        stats=steady_metrics(rows,2)
        self.assertEqual(stats['steady_complete_cases_per_minute'],60)
        self.assertEqual(stats['steady_window_completions'],11)
        self.assertEqual(len(rows),16);self.assertFalse(rows[-1]['complete'])

    def test_baseline_time_bound_does_not_block_healthy_following_stage(self):
        self.assertTrue(baseline_ready(15,15,24))
        self.assertFalse(baseline_ready(15,14,24))
        self.assertFalse(baseline_ready(3,3,24))

    def test_rolling_windows_and_unknown_usage(self):
        now=1000
        r=rates([{'ended_epoch':999,'completion_tokens':100,'complete':True},
                 {'ended_epoch':950,'completion_tokens':None,'complete':False},
                 {'ended_epoch':800,'completion_tokens':200,'complete':True},
                 {'ended_epoch':600,'completion_tokens':900,'complete':True}],now)
        self.assertEqual(r['60']['completed'],2);self.assertEqual(r['300']['completed'],3)
        self.assertEqual(r['60']['usage_unknown'],1)
        self.assertEqual(r['300']['per_minute'],.6)

    def test_pacer_settlement_keeps_unknown_out_of_actual_tokens(self):
        p=Pacer(3500);p.last-=1
        self.assertEqual(p.delay('direct'),0)
        estimate=p.take('direct');p.settle('direct',estimate,1000)
        self.assertAlmostEqual(p.estimates['direct'],673.)
        before=p.estimates['ours'];p.settle('ours',1018,None)
        self.assertEqual(before,p.estimates['ours'])

    def test_http200_truncation_not_complete_or_missing_zero(self):
        t={'seconds':1.,'ended_epoch':2.,'http_status':200,
           'body':{'choices':[{'finish_reason':'length','message':{'content':'partial'}}]}}
        r=response_metrics(t);self.assertFalse(r['complete']);self.assertIsNone(r['completion_tokens'])

    def test_chinese_views_render_without_telemetry_or_model_calls(self):
        state={'updated_epoch':time.time()-40,'state':'运行中','total':10,'completed':3,
               'active':[],'jobs':[],'stage_name':'c16','rates':{},'statistics':{}}
        for view in ('pipeline','job'):
            sink=io.StringIO();Console(file=sink,width=120).print(render(state,view))
            text=sink.getvalue();self.assertIn('未知',text);self.assertIn('数据已过期',text)
            self.assertIn('运行中',text)
            if view=='job':self.assertIn('['+'#'*13+'-'*31+']',text)


if __name__=='__main__':unittest.main()
