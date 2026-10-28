"""Source binding and native consumer fault injection; no live model or DB."""

import argparse
from copy import deepcopy
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

from neo4j.exceptions import ClientError, ServiceUnavailable

from accept_neo4j_native_plan_20261029 import AUTHOR, selection
from accept_neo4j_read_capture_20261029 import Driver, Result
from audited_model_reply_dev_20261029 import PinnedFile, RecordedReply, ReplyConflict, ReplyIdentity
from neo4j_native_answer_contract_dev_20261029 import encoded
from neo4j_native_plan_dev_20261029 import EvidenceConflict
from neo4j_native_selection_dev_20261029 import NativeSchema
from neo4j_reply_consumer_dev_20261029 import consume_recorded_reply


def single_plan():
    return dict(status='ANSWER',calls=[selection()],combine=dict(op='single'))


class ConsumerChecks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.number = 0
        self.identity = ReplyIdentity('mindthequery','task-1','direct','people','fixture-model','initial')

    def pin(self, name, value, raw=None):
        self.number += 1
        path = self.root/(str(self.number)+'-'+name+'.json')
        raw = encoded(value) if raw is None else raw
        path.write_bytes(raw)
        return PinnedFile(str(path),hashlib.sha256(raw).hexdigest())

    def source(self, *, identity=None, content='RETURN 1 AS x', status='ok', charged=True,
               change_request=None, change_terminal=None, change_slot=None, reason='stop'):
        identity = identity or self.identity
        request = dict(model=identity.model,messages=[dict(role='user',content='public fixture question')],
                       temperature=0.2,max_tokens=8192,chat_template_kwargs=dict(thinking=True))
        if change_request:
            change_request(request)
        request_pin = self.pin('request',request)
        body = dict(model=identity.model,choices=[dict(finish_reason=reason,
                    message=dict(role='assistant',content=content,reasoning_content='not the answer'))],
                    usage=dict(prompt_tokens=20,completion_tokens=30,total_tokens=50))
        terminal = dict(asdict(identity),status=status,charged=charged,logical_key=identity.key,
            request_id='run__'+identity.key+'__a1',budget_slot=1,attempt_no=1,
            http_status=200,body=body,finish_reason=reason,content=content,usage=body['usage'])
        if change_terminal:
            change_terminal(terminal)
        slot = dict(slot=1,attempt_no=1,request_id=terminal.get('request_id'),
                    logical_key=identity.key,request_sha256=request_pin.sha256,scope='fixture-scope')
        if change_slot:
            change_slot(slot)
        return RecordedReply(identity,request=request_pin,terminal=self.pin('terminal',terminal),
                             slot=self.pin('slot',slot) if charged is True else None)

    def consume(self, source, results=(), **options):
        self.number += 1
        driver = Driver(results)
        folder = self.root/('out-'+str(self.number))
        settings = dict(snapshot_sha256='a'*64,database='fixture',transaction_timeout=75,
                        max_rows=100,max_answer_bytes=1000000)
        outcome = consume_recorded_reply(source,NativeSchema(AUTHOR,format='MTQ'),
            driver=driver,directory=folder,capture_settings=settings,**options)
        return outcome, driver, folder

    def test_direct_actual_answer_is_saved_with_original_cost_identity(self):
        source = self.source(content='  RETURN 1 AS x;  ')
        outcome, driver, folder = self.consume(source,[Result(),Result([[1]])])
        self.assertEqual(outcome['method']['answer']['rows'],[[dict(t='int',v='1')]])
        self.assertEqual(driver.tx.calls,[('EXPLAIN RETURN 1 AS x',{}),('RETURN 1 AS x',{})])
        self.assertEqual(outcome['source']['source_charge_key'],source.terminal['request_id'])
        self.assertEqual(outcome['source']['source_usage']['total_tokens'],50)
        self.assertEqual(json.loads((folder/'method_final.json').read_bytes())['result'],outcome['method'])
        self.assertEqual(outcome['new_model_calls'],0)
        self.assertIsNone(outcome['monetary_cost'])
        self.assertFalse(outcome['source']['source_ledger_chain_reaudited'])

    def test_request_digest_is_bound_to_the_slot(self):
        with self.assertRaises(ReplyConflict):
            self.source(change_slot=lambda s:s.update(request_sha256='b'*64))

    def test_slot_request_id_attempt_and_number_cannot_drift(self):
        for field,value in (('slot',2),('request_id','another'),('attempt_no',2),('logical_key','wrong')):
            with self.subTest(field=field),self.assertRaises(ReplyConflict):
                self.source(change_slot=lambda s:s.update({field:value}))

    def test_wrong_request_model_is_rejected(self):
        with self.assertRaises(ReplyConflict):
            self.source(change_request=lambda r:r.update(model='another-model'))

    def test_all_terminal_identity_fields_are_checked(self):
        for field in asdict(self.identity):
            with self.subTest(field=field),self.assertRaises(ReplyConflict):
                self.source(change_terminal=lambda r:r.update({field:'wrong'}))

    def test_missing_graph_cannot_be_inferred_from_a_logical_key(self):
        with self.assertRaises(ReplyConflict):
            self.source(change_terminal=lambda r:r.pop('graph'))

    def test_changed_file_bytes_are_rejected(self):
        pin = self.pin('source',dict(a=1))
        Path(pin.path).write_bytes(b'{}')
        with self.assertRaises(ReplyConflict):pin.load()

    def test_missing_terminal_is_not_synthesized_or_resent(self):
        with self.assertRaises(FileNotFoundError):
            PinnedFile(str(self.root/'absent'),'a'*64).load()

    def test_duplicate_nonfinite_and_nonobject_sources_are_rejected(self):
        for raw in (b'{"a":1,"a":2}',b'{"n":NaN}',b'{"n":1e999}',b'[]'):
            with self.subTest(raw=raw),self.assertRaises(ReplyConflict):
                self.pin('invalid',None,raw).load()

    def test_evidence_bound_is_not_a_truncated_parse(self):
        with self.assertRaises(ReplyConflict):self.pin('large',dict(a='x'*100)).load(max_bytes=8)

    def test_synthetic_snapshot_cannot_mutate_through_public_views(self):
        source = self.source()
        source.terminal['content']='CREATE (n)'
        source.request['model']='another'
        self.assertEqual(source.terminal['content'],'RETURN 1 AS x')
        self.assertEqual(source.request['model'],self.identity.model)
        with self.assertRaises(AttributeError):source.status='READY'

    def test_unknown_charge_and_missing_request_id_remain_unknown(self):
        def sparse(t):
            keep = dict(status='unknown',charged=None,logical_key=self.identity.key,
                        generation_outcome_unknown=True)
            t.clear();t.update(keep)
        outcome,driver,folder=self.consume(self.source(status='unknown',charged=None,change_terminal=sparse))
        self.assertEqual(outcome['method']['answer']['status'],'UNKNOWN')
        self.assertIsNone(outcome['source']['source_charged'])
        self.assertIsNone(outcome['source']['source_charge_key'])
        self.assertTrue(outcome['source']['requires_outcome_audit'])
        self.assertEqual(driver.session_calls,[])
        self.assertFalse((folder/'capture').exists())

    def test_charged_unknown_keeps_its_slot_without_execution(self):
        outcome,driver,_=self.consume(self.source(status='unknown'))
        self.assertEqual(outcome['method']['answer']['status'],'UNKNOWN')
        self.assertEqual(outcome['source']['source_budget_slot'],1)
        self.assertEqual(driver.session_calls,[])

    def test_undispatched_states_open_no_graph_or_charge(self):
        for status in ('not_dispatched','local_resource_error','local_configuration_error','admission_timeout'):
            with self.subTest(status=status):
                outcome,driver,_=self.consume(self.source(status=status,charged=False))
                self.assertEqual(outcome['method']['answer']['status'],'UNDISPATCHED')
                self.assertEqual(driver.session_calls,[])
                self.assertIsNone(outcome['source']['source_charge_key'])

    def test_contradictory_charge_evidence_fails(self):
        for status,charged in (('ok',False),('unknown',False),('not_dispatched',True),('ok',1)):
            with self.subTest(status=status,charged=charged),self.assertRaises(ReplyConflict):
                self.source(status=status,charged=charged)

    def test_charged_source_without_slot_is_rejected(self):
        source=self.source()
        pins=source.source_link()['files']
        with self.assertRaises(ReplyConflict):
            RecordedReply(self.identity,request=PinnedFile(**pins['request']),terminal=PinnedFile(**pins['terminal']))

    def test_refusal_length_and_empty_do_not_become_correct_empty_answers(self):
        for status,reason,content,expected in (('refused','content_filter',None,'REFUSED'),
                ('length','length','RETURN 1','MODEL_TRUNCATED'),('empty','stop',' ','EMPTY')):
            with self.subTest(status=status):
                outcome,driver,_=self.consume(self.source(status=status,reason=reason,content=content),allow_feedback=True)
                self.assertEqual(outcome['method']['answer']['status'],expected)
                self.assertFalse(outcome['method']['answer']['complete'])
                self.assertIsNone(outcome['feedback_draft'])
                self.assertEqual(driver.session_calls,[])

    def test_foreign_response_model_is_rejected(self):
        with self.assertRaises(ReplyConflict):
            self.source(change_terminal=lambda t:t['body'].update(model='another'))

    def test_usage_and_projection_must_match_raw_completion(self):
        for field,value in (('usage',None),('content','RETURN 2'),('finish_reason','length')):
            with self.subTest(field=field),self.assertRaises(ReplyConflict):
                self.source(change_terminal=lambda t:t.update({field:value}))

    def test_multiple_or_nonterminal_choices_are_not_executable(self):
        with self.assertRaises(ReplyConflict):
            self.source(change_terminal=lambda t:t['body']['choices'].append(deepcopy(t['body']['choices'][0])))
        with self.assertRaises(ReplyConflict):self.source(reason='tool_calls')

    def test_reasoning_content_is_never_a_fallback_answer(self):
        outcome,driver,_=self.consume(self.source(status='empty',content=''))
        self.assertEqual(outcome['method']['answer']['status'],'EMPTY')
        self.assertEqual(driver.tx.calls,[])

    def test_infrastructure_status_does_not_generate_operation_feedback(self):
        outcome,driver,_=self.consume(self.source(status='infra'),allow_feedback=True)
        self.assertEqual(outcome['method']['answer']['status'],'INFRASTRUCTURE_ERROR')
        self.assertIsNone(outcome['feedback_draft'])
        self.assertEqual(driver.session_calls,[])

    def test_ours_uses_strict_native_ir_and_saves_before_controls(self):
        source=self.source(identity=replace(self.identity,method='ours'),content=encoded(single_plan()).decode())
        results=[]
        for _ in range(3):results += [Result(),Result([],columns=('id','display'))]
        outcome,driver,folder=self.consume(source,results,run_controls=True)
        self.assertEqual(outcome['method']['answer']['status'],'EXECUTED')
        self.assertEqual(outcome['controls']['results']['native_set_count']['answer']['status'],'NOT_APPLICABLE')
        self.assertEqual(len(driver.tx.calls),6)
        events=[json.loads(p.read_bytes())['event'] for p in sorted((folder/'capture').glob('*.json'))]
        kinds=[e['kind'] for e in events]
        self.assertLess(kinds.index('source_reply_bound'),kinds.index('read_intent'))
        self.assertLess(kinds.index('method_final_saved'),kinds.index('control_execution_intent'))
        self.assertTrue(events[kinds.index('control_execution_intent')]['shared_planner'])

    def test_ours_explicit_unsupported_is_not_an_executed_plan(self):
        source=self.source(identity=replace(self.identity,method='ours'),
                           content='{"status":"UNSUPPORTED","reason":"fixture boundary"}')
        outcome,driver,_=self.consume(source,allow_feedback=True)
        self.assertEqual(outcome['method']['answer']['status'],'UNSUPPORTED')
        self.assertIsNone(outcome['feedback_draft'])
        self.assertEqual(driver.session_calls,[])

    def test_public_schema_tracks_are_not_silently_crossed(self):
        source=self.source(identity=replace(self.identity,benchmark='zograscope',method='ours'),
                           content=encoded(single_plan()).decode())
        with self.assertRaises(ValueError):self.consume(source)

    def test_malformed_ir_has_unsent_feedback_preserving_original_request_settings(self):
        source=self.source(identity=replace(self.identity,method='ours'),content='{"status":"ANSWER","status":"UNSUPPORTED"}')
        outcome,driver,_=self.consume(source,allow_feedback=True)
        draft=outcome['feedback_draft']
        self.assertEqual(outcome['method']['answer']['status'],'PROTOCOL_ERROR')
        self.assertEqual(draft['request']['max_tokens'],8192)
        self.assertEqual(draft['request']['chat_template_kwargs'],dict(thinking=True))
        self.assertEqual(draft['request']['messages'][:-2],source.request['messages'])
        self.assertEqual(draft['request']['messages'][-2]['content'],source.terminal['content'])
        self.assertFalse(draft['dispatched'])
        self.assertEqual(driver.session_calls,[])

    def test_feedback_phase_cannot_make_a_second_operation_feedback(self):
        source=self.source(identity=replace(self.identity,method='ours',phase='feedback'),content='{}')
        outcome,_,_=self.consume(source,allow_feedback=True)
        self.assertIsNone(outcome['feedback_draft'])

    def test_direct_write_guard_failure_preserves_failed_case(self):
        outcome,driver,folder=self.consume(self.source(content='CREATE (n) RETURN n'),allow_feedback=True)
        self.assertEqual(outcome['method']['answer']['status'],'GUARD_REJECTED')
        self.assertEqual(driver.tx.calls,[])
        self.assertTrue((folder/'outcome.json').exists())
        self.assertIsNotNone(outcome['feedback_draft'])

    def test_client_query_error_is_incomplete_and_not_automatically_retried(self):
        outcome,driver,_=self.consume(self.source(),[ClientError('synthetic query failure')],allow_feedback=True)
        self.assertEqual(outcome['method']['answer']['status'],'EXECUTION_ERROR')
        self.assertEqual(len(driver.tx.calls),1)
        self.assertIsNotNone(outcome['feedback_draft'])

    def test_database_connection_failure_is_not_a_model_repair_request(self):
        outcome,driver,_=self.consume(self.source(),[ServiceUnavailable('synthetic link failure')],allow_feedback=True)
        self.assertEqual(outcome['method']['answer']['status'],'INFRASTRUCTURE_ERROR')
        self.assertEqual(len(driver.tx.calls),1)
        self.assertIsNone(outcome['feedback_draft'])

    def test_source_persistence_failure_precedes_any_database_activity(self):
        source=self.source()
        driver=Driver([])
        with patch('neo4j_reply_consumer_dev_20261029.save_new',side_effect=OSError('synthetic full disk')):
            with self.assertRaises(OSError):
                consume_recorded_reply(source,None,driver=driver,directory=self.root/'failed',capture_settings={})
        self.assertEqual(driver.session_calls,[])

    def test_capture_evidence_failure_propagates_instead_of_becoming_query_error(self):
        with patch('neo4j_native_plan_dev_20261029.PlanEvidence.__call__',side_effect=EvidenceConflict('synthetic write failure')):
            with self.assertRaises(EvidenceConflict):self.consume(self.source())

    def test_control_failure_retains_the_successful_method_answer(self):
        source=self.source(identity=replace(self.identity,method='ours'),content=encoded(single_plan()).decode())
        outcome,_,folder=self.consume(source,[Result(),Result([],columns=('id','display')),
                                              ClientError('synthetic control failure')],run_controls=True)
        self.assertEqual(outcome['method']['answer']['status'],'EXECUTED')
        self.assertEqual(outcome['controls']['status'],'INCOMPLETE')
        self.assertEqual(json.loads((folder/'method_final.json').read_bytes())['result'],outcome['method'])

    def test_existing_outcome_directory_is_never_overwritten_or_resumed(self):
        source=self.source(status='unknown')
        folder=self.root/'occupied';folder.mkdir();(folder/'sentinel').write_bytes(b'keep')
        with self.assertRaises(FileExistsError):
            consume_recorded_reply(source,None,driver=Driver([]),directory=folder,capture_settings={})
        self.assertEqual((folder/'sentinel').read_bytes(),b'keep')

    def test_unknown_implementation_failure_is_not_hidden_as_a_bad_case(self):
        with self.assertRaises(RuntimeError):
            self.consume(self.source(),[RuntimeError('synthetic implementation bug')])


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',required=True);args=parser.parse_args()
    suite=unittest.defaultTestLoader.loadTestsFromTestCase(ConsumerChecks)
    planned=suite.countTestCases()
    with patch.object(socket,'create_connection',side_effect=AssertionError('No network')), \
         patch.object(socket.socket,'connect',side_effect=AssertionError('No network')):
        result=unittest.TextTestRunner(verbosity=2).run(suite)
    output=Path(args.output);output.mkdir(parents=True,exist_ok=False)
    summary=dict(kind='PINNED_SYNTHETIC_REPLY_CONSUMER_FAKE_DB_NOT_EXPERIMENT',planned=planned,
        completed=result.testsRun,checks_passed=result.testsRun-len(result.failures)-len(result.errors),
        failed=len(result.failures)+len(result.errors),new_model_calls=0,DB_connections=0,DB_queries=0,
        ledger_chain_reaudited=False,adapter_registered=False,formal_admission=False)
    (output/'summary.json').write_bytes(encoded(summary)+b'\n')
    if not result.wasSuccessful():raise SystemExit(1)


if __name__=='__main__':main()
