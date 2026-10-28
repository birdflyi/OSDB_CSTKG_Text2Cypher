"""Synthetic native plan/evidence integration; real SDK values, fake transport."""

import argparse
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

from neo4j.graph import Graph,Node

from accept_neo4j_read_capture_20261029 import Driver,Result
from neo4j_native_answer_contract_dev_20261029 import IdentityScopeConflict,UnsupportedNativeValue,encoded
from neo4j_native_plan_dev_20261029 import (
    EvidenceConflict,NativePlanChain,PlanEvidence,PreparedPlan,native_set_count_query,
)
from neo4j_native_selection_dev_20261029 import NativeSchema,UnsupportedSelection
from neo4j_read_capture_dev_20261029 import NativeReadCapture,CaptureStateError
from public_read_guard import read_query


AUTHOR=dict(node_props={'Person':[dict(property=k,datatype=v) for k,v in
    {'display':'STRING','age':'INTEGER','alive':'BOOLEAN','clock':'DATE_TIME'}.items()]},
    rel_props={'LINK':[]},relationships=[dict(start='Person',type='LINK',end='Person')])


def selection():
    return dict(nodes=[dict(id='n',label='Person')],edges=[],where=[],
                returns=[dict(field='n',alias='id'),dict(field='n.display',alias='display')])


def plan(operation='union',output='count'):
    combine=dict(op=operation,key='id',output=output)
    combine.update(alias='n') if output=='count' else combine.update(columns=['display'])
    return dict(status='ANSWER',calls=[selection(),selection()],combine=combine)


def node(identity,display='same'):
    return Node(Graph(),identity,1,['Person'],{'display':display})


def rows(*identities,display='same'):
    return [[node(identity,display),display] for identity in identities]


def read_results(values,columns=('id','display')):
    return [Result(),Result(values,columns=columns)]


class PlanChecks(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.counter=0

    def prepare(self,value=None):
        return PreparedPlan(value or plan(),NativeSchema(AUTHOR,format='MTQ'))

    def capture(self,results,evidence_class=PlanEvidence,**options):
        self.counter+=1
        evidence=evidence_class(self.root/str(self.counter),max_record_bytes=1000000)
        driver=Driver(results)
        settings=dict(snapshot_sha256='a'*64,database='fixture',transaction_timeout=75,
                      max_rows=100,max_answer_bytes=100000,emit=evidence)
        settings.update(options)
        return NativeReadCapture(driver,**settings),driver,evidence

    def events(self,evidence):
        return [json.loads(p.read_bytes())['event'] for p in sorted(evidence.directory.glob('*.json'))]

    def test_entity_union_intersection_difference_use_identity_not_display(self):
        for operation,expected in (('union',3),('intersection',1),('difference',1)):
            cap,driver,evidence=self.capture(read_results(rows('a','a','b'))+read_results(rows('b','c')))
            with cap:
                result=NativePlanChain(self.prepare(plan(operation)),cap,evidence).run_method()
            self.assertEqual(result['answer']['rows'],[[dict(t='int',v=str(expected))]])
            self.assertEqual(result['completed_read_operations'],2)
            self.assertEqual(len(driver.tx.calls),4)

    def test_empty_sets_and_duplicate_rows_are_not_unknown_or_counted_twice(self):
        for left,right,operation,expected in (([],[],'union',0),([],rows('a'),'difference',0),
              (rows('a','a'),[],'intersection',0),(rows('a','a'),[],'union',1)):
            cap,driver,evidence=self.capture(read_results(left)+read_results(right))
            with cap:result=NativePlanChain(self.prepare(plan(operation)),cap,evidence).run_method()
            self.assertEqual(result['answer']['rows'][0][0]['v'],str(expected))
            self.assertTrue(result['answer']['complete'])

    def test_final_property_projection_keeps_one_row_per_distinct_entity(self):
        cap,driver,evidence=self.capture(read_results(rows('a'))+read_results(rows('b')))
        with cap:result=NativePlanChain(self.prepare(plan(output='rows')),cap,evidence).run_method()
        self.assertEqual(result['answer']['rows'],[[dict(t='str',v='same')],[dict(t='str',v='same')]])
        self.assertFalse(result['answer']['contains_graph_entities'])

    def test_native_entity_projection_is_preserved_when_requested(self):
        value=plan(output='rows');value['combine']['columns']=['id']
        cap,driver,evidence=self.capture(read_results(rows('a'))+read_results(rows('b')))
        with cap:result=NativePlanChain(self.prepare(value),cap,evidence).run_method()
        self.assertTrue(result['answer']['contains_graph_entities'])
        self.assertEqual([row[0]['element_id'] for row in result['answer']['rows']],['a','b'])

    def test_relationship_keys_use_native_identity_and_type(self):
        value=plan()
        for call in value['calls']:
            call['nodes'].append(dict(id='m',label='Person'))
            call['edges']=[dict(id='r',source='n',target='m',label='LINK')]
            call['returns']=[dict(field='r',alias='id'),dict(field='r',alias='display')]
        def relation(identity):
            graph=Graph();a=Node(graph,'a',1,['Person'],{});b=Node(graph,'b',2,['Person'],{})
            rel=graph.relationship_type('LINK')(graph,identity,3,{})
            # Fixture-only hydration setup, matching the pinned SDK.
            rel._start_node,rel._end_node=a,b
            return [rel,rel]
        cap,driver,evidence=self.capture(read_results([relation('x')])+read_results([relation('x'),relation('y')]))
        with cap:result=NativePlanChain(self.prepare(value),cap,evidence).run_method()
        self.assertEqual(result['answer']['rows'],[[dict(t='int',v='2')]])

    def test_composed_byte_overflow_is_not_truncated_to_one_entity(self):
        value=plan(output='rows');value['combine']['columns']=['id','display']
        cap,driver,evidence=self.capture(read_results(rows('a',display='x'*400))+
                                        read_results(rows('b',display='x'*400)),max_answer_bytes=1500)
        with self.assertRaises(UnsupportedNativeValue):
            with cap:NativePlanChain(self.prepare(value),cap,evidence).run_method()
        self.assertEqual(len(driver.tx.calls),4)
        self.assertNotIn('method_final_saved',[e['kind'] for e in self.events(evidence)])

    def test_null_payload_is_retained_but_null_entity_key_fails(self):
        cap,driver,evidence=self.capture(read_results([[node('a'),None]])+read_results([]))
        with cap:result=NativePlanChain(self.prepare(plan(output='rows')),cap,evidence).run_method()
        self.assertEqual(result['answer']['rows'],[[dict(t='null')]])
        cap,driver,evidence=self.capture(read_results([[None,'x']])+read_results([]))
        with self.assertRaises(CaptureStateError):
            with cap:NativePlanChain(self.prepare(),cap,evidence).run_method()
        self.assertNotIn('method_final_saved',[event['kind'] for event in self.events(evidence)])

    def test_duplicate_key_conflicting_payload_fails_instead_of_last_write_wins(self):
        for left,right in ((rows('a',display='x')+rows('a',display='y'),[]),
                           (rows('a',display='x'),rows('a',display='y'))):
            cap,driver,evidence=self.capture(read_results(left)+read_results(right))
            with self.assertRaises(CaptureStateError):
                with cap:NativePlanChain(self.prepare(),cap,evidence).run_method()
            self.assertNotIn('method_final_saved',[event['kind'] for event in self.events(evidence)])

    def test_property_id_and_nullable_entity_cannot_replace_native_key(self):
        value=plan()
        for call in value['calls']:call['returns'][0]['field']='n.display'
        with self.assertRaises(UnsupportedSelection):self.prepare(value)
        value=plan()
        for call in value['calls']:
            call['nodes'].append(dict(id='m',label='Person'))
            call['edges']=[dict(id='r',source='n',target='m',label='LINK',optional=True)]
            call['returns']=[dict(field='m',alias='id'),dict(field='m.display',alias='display')]
        with self.assertRaises(UnsupportedSelection):self.prepare(value)

    def test_neighbour_payload_aggregated_or_truncated_inputs_are_rejected(self):
        for mode in ('neighbour','aggregate','limit','order'):
            value=plan()
            for call in value['calls']:
                if mode=='neighbour':
                    call['nodes'].append(dict(id='m',label='Person'))
                    call['edges']=[dict(id='r',source='n',target='m',label='LINK')]
                    call['returns'][1]['field']='m.display'
                elif mode=='aggregate':call['returns'][1].update(field='*',agg='count')
                elif mode=='limit':call['limit']=1
                else:call['order_by']=[dict(column='id',direction='ASC')]
            with self.subTest(mode=mode),self.assertRaises(UnsupportedSelection):self.prepare(value)

    def test_mismatched_key_kind_or_columns_are_rejected_before_reads(self):
        value=plan();value['calls'][1]['returns'][0]['alias']='different'
        with self.assertRaises(UnsupportedSelection):self.prepare(value)
        value=plan();call=value['calls'][1]
        call['nodes'].append(dict(id='m',label='Person'))
        call['edges']=[dict(id='r',source='n',target='m',label='LINK')]
        call['returns']=[dict(field='r',alias='id'),dict(field='r',alias='display')]
        with self.assertRaises(UnsupportedSelection):self.prepare(value)

    def test_disjoint_string_union_counts_share_plan_without_entity_payload_reads(self):
        value=plan()
        for call,literal in zip(value['calls'],('A','B')):
            call['where']=[dict(field='n.display',op='eq',value=literal)]
        prepared=self.prepare(value)
        cap,driver,evidence=self.capture(read_results([[2]],['n'])+read_results([[1]],['n']))
        with cap:result=NativePlanChain(prepared,cap,evidence).run_method()
        self.assertEqual(result['answer']['rows'],[[dict(t='int',v='3')]])
        self.assertEqual(result['representation'],'certified_disjoint_entity_counts')
        self.assertIsNotNone(result['certificate'])
        self.assertTrue(all('count(DISTINCT `n`)' in query for query,params in driver.tx.calls))
        self.assertEqual([params['p0'] for query,params in driver.tx.calls[1::2]],['A','B'])

    def test_numeric_temporal_negated_and_equal_literals_never_certify_disjoint(self):
        cases=[('n.age',1,2,False),('n.clock','2026-01-01T00:00:00Z','2026-01-01T00:00:00+00:00',False),
               ('n.display','A','B',True),('n.display','A','A',False)]
        for field,left,right,negate in cases:
            value=plan()
            for call,literal in zip(value['calls'],(left,right)):
                call['where']=[dict(field=field,op='eq',value=literal,negate=negate)]
            self.assertIsNone(self.prepare(value).disjoint_certificate)

    def test_count_failure_is_not_zero_or_success(self):
        value=plan()
        for call,literal in zip(value['calls'],('A','B')):call['where']=[dict(field='n.display',op='eq',value=literal)]
        cap,driver,evidence=self.capture(read_results([[None]],['n']))
        with self.assertRaises(CaptureStateError):
            with cap:NativePlanChain(self.prepare(value),cap,evidence).run_method()
        self.assertEqual(len(driver.tx.calls),2)
        self.assertNotIn('method_final_saved',[e['kind'] for e in self.events(evidence)])

    def test_live_capture_scope_is_required_even_for_scalar_count_summaries(self):
        cap,driver,evidence=self.capture(read_results(rows('a')))
        with cap:
            real=cap.read
            def foreign(*args,**kwargs):
                answer=real(*args,**kwargs);answer['scope']['capture_id']='foreign';return answer
            cap.read=foreign
            with self.assertRaises(IdentityScopeConflict):
                # Scope conflict is a ValueError subclass; no second read or
                # cached cross-transaction identity fallback may occur.
                NativePlanChain(self.prepare(),cap,evidence).run_method()
        self.assertEqual(len(driver.tx.calls),2)

    def test_controls_start_only_after_actual_method_file_is_saved(self):
        blocks=read_results(rows('a','b'))+read_results(rows('b','c'))
        cap,driver,evidence=self.capture(blocks*3+read_results([[3]],['n']))
        with cap:
            chain=NativePlanChain(self.prepare(),cap,evidence)
            with self.assertRaises(CaptureStateError):chain.run_controls()
            method=chain.run_method();before=(evidence.directory/chain.method_receipt['file']).read_bytes()
            controls=chain.run_controls()
            self.assertEqual((evidence.directory/chain.method_receipt['file']).read_bytes(),before)
            for result in controls.values():self.assertEqual(result['answer']['rows'],method['answer']['rows'])
            with self.assertRaises(CaptureStateError):chain.run_controls()
        events=self.events(evidence);kinds=[event['kind'] for event in events]
        self.assertLess(kinds.index('method_final_saved'),kinds.index('control_execution_intent'))
        purposes=[e['purpose'] for e in events if e['kind']=='answer_captured']
        self.assertEqual(purposes,['method']*2+['control']*5)
        self.assertEqual(len(driver.tx.calls),14)
        for event in events:
            if event['kind']=='control_final_saved':self.assertFalse(event['independent_model_method'])

    def test_final_persistence_failure_blocks_controls_and_keeps_partial_evidence(self):
        class BrokenFinalEvidence(PlanEvidence):
            def __call__(self,event):
                if event['kind']=='method_final_saved':
                    with patch('os.fsync',side_effect=OSError('disk unavailable')):
                        return super().__call__(event)
                return super().__call__(event)
        cap,driver,evidence=self.capture(read_results(rows('a'))+read_results([]),BrokenFinalEvidence)
        chain=None
        with self.assertRaises(EvidenceConflict):
            with cap:
                chain=NativePlanChain(self.prepare(),cap,evidence);chain.run_method()
        self.assertIsNone(chain.method_receipt)
        self.assertTrue(evidence.failed)
        with self.assertRaises(CaptureStateError):chain.run_controls()
        self.assertEqual(len(driver.tx.calls),4)
        self.assertGreater(len(list(evidence.directory.glob('*.json'))),0)

    def test_tampered_method_receipt_cannot_trigger_control_reads(self):
        cap,driver,evidence=self.capture(read_results(rows('a'))+read_results([]))
        with self.assertRaises(EvidenceConflict):
            with cap:
                chain=NativePlanChain(self.prepare(),cap,evidence);chain.run_method()
                (evidence.directory/chain.method_receipt['file']).write_bytes(b'tampered')
                chain.run_controls()
        self.assertEqual(len(driver.tx.calls),4)

    def test_evidence_files_are_exclusive_and_hash_linked(self):
        cap,driver,evidence=self.capture(read_results(rows('a'))+read_results([]))
        with cap:NativePlanChain(self.prepare(),cap,evidence).run_method()
        previous=None
        for sequence,path in enumerate(sorted(evidence.directory.glob('*.json')),1):
            raw=path.read_bytes();record=json.loads(raw)
            self.assertEqual(record['evidence_sequence'],sequence)
            self.assertEqual(record['previous_sha256'],previous)
            previous=hashlib.sha256(raw).hexdigest()
        with self.assertRaises(FileExistsError):PlanEvidence(evidence.directory,max_record_bytes=1000)

    def test_set_count_query_uses_compiler_parameter_namespaces(self):
        value=plan()
        for call,literal in zip(value['calls'],('A','B')):call['where']=[dict(field='n.display',op='eq',value=literal)]
        for operation in ('union','intersection','difference'):
            value['combine']['op']=operation
            compiled=native_set_count_query(self.prepare(value))
            self.assertEqual(compiled['parameters'],{'left_p0':'A','right_p0':'B'})
            self.assertIn('$left_p0',compiled['query']);self.assertIn('$right_p0',compiled['query'])
            self.assertEqual(read_query(compiled['query']),compiled['query'])
            if operation!='union':
                self.assertEqual(compiled['query'].count('CALL { CALL {'),2)
                self.assertIn('RETURN collect(DISTINCT `id`)',compiled['query'])

    def test_native_count_control_scope_is_explicit_for_row_plans(self):
        cap,driver,evidence=self.capture((read_results(rows('a'))+read_results([]))*3)
        with cap:
            chain=NativePlanChain(self.prepare(plan(output='rows')),cap,evidence)
            chain.run_method();controls=chain.run_controls()
        native=controls['native_set_count']
        self.assertEqual(native['answer']['status'],'NOT_APPLICABLE')
        self.assertFalse(native['answer']['complete'])
        self.assertEqual(native['completed_read_operations'],0)
        self.assertEqual(len(driver.tx.calls),12)

    def test_single_selection_keeps_original_row_multiplicity(self):
        value=dict(status='ANSWER',calls=[selection()],combine=dict(op='single'))
        cap,driver,evidence=self.capture(read_results(rows('a','a')))
        with cap:result=NativePlanChain(self.prepare(value),cap,evidence).run_method()
        self.assertEqual(len(result['answer']['rows']),2)
        self.assertEqual(result['representation'],'native_selection')

    def test_no_method_or_control_retries_after_a_failed_read(self):
        cap,driver,evidence=self.capture([ConnectionResetError('fake connection reset')])
        chain=None
        with self.assertRaises(ConnectionResetError):
            with cap:
                chain=NativePlanChain(self.prepare(),cap,evidence);chain.run_method()
        self.assertEqual(len(driver.tx.calls),1)
        with self.assertRaises(CaptureStateError):chain.run_method()
        with self.assertRaises(CaptureStateError):chain.run_controls()

    def test_method_cannot_silently_switch_to_native_control_strategy(self):
        cap,driver,evidence=self.capture([])
        with cap:
            with self.assertRaises(ValueError):
                NativePlanChain(self.prepare(),cap,evidence).run_method(strategy='native_set_count')
        self.assertEqual(driver.tx.calls,[])

    def test_closed_capture_cannot_dispatch_controls_or_new_method(self):
        cap,driver,evidence=self.capture(read_results(rows('a'))+read_results([]))
        with cap:
            chain=NativePlanChain(self.prepare(),cap,evidence);chain.run_method()
        with self.assertRaises(CaptureStateError):chain.run_controls()
        self.assertEqual(len(driver.tx.calls),4)

    def test_unsupported_reply_is_not_an_executed_empty_plan(self):
        with self.assertRaises(UnsupportedSelection):self.prepare(dict(status='UNSUPPORTED',reason='not covered'))


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',required=True);args=parser.parse_args()
    suite=unittest.defaultTestLoader.loadTestsFromTestCase(PlanChecks);planned=suite.countTestCases()
    with patch.object(socket,'create_connection',side_effect=AssertionError('No network')), \
         patch.object(socket.socket,'connect',side_effect=AssertionError('No network')):
        result=unittest.TextTestRunner(verbosity=2).run(suite)
    output=Path(args.output);output.mkdir(parents=True,exist_ok=False)
    summary=dict(kind='SYNTHETIC_NATIVE_PLAN_EVIDENCE_INTEGRATION_NOT_DB_ACCEPTANCE',planned=planned,
                 completed=result.testsRun,checks_passed=result.testsRun-len(result.failures)-len(result.errors),
                 failed=len(result.failures)+len(result.errors),model_calls=0,DB_connections=0,DB_queries=0,
                 real_Neo4j_set_count_syntax_verified=False,graph_immutability_verified=False,
                 primary_scoring_integrated=False,formal_admission=False)
    (output/'summary.json').write_bytes(encoded(summary)+b'\n')
    if not result.wasSuccessful():raise SystemExit(1)


if __name__=='__main__':main()
