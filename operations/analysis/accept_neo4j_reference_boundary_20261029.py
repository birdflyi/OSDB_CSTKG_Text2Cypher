"""Reference ordering/denominator checks with invented data and fake DB only."""

import argparse
from copy import deepcopy
from dataclasses import asdict,replace
import hashlib
import json
from pathlib import Path
import socket
import unittest
from unittest.mock import patch

from neo4j.exceptions import ClientError
from neo4j.graph import Graph,Node

from accept_neo4j_reply_consumer_20261029 import ConsumerChecks
from accept_neo4j_read_capture_20261029 import Result,Driver
from audited_model_reply_dev_20261029 import PinnedFile,ReplyConflict
from neo4j_native_answer_contract_dev_20261029 import encoded
from neo4j_read_capture_dev_20261029 import CaptureStateError,NativeReadCapture
from neo4j_native_plan_dev_20261029 import PlanEvidence
from neo4j_reference_boundary_dev_20261029 import (
    AuxiliaryTaskPolicy,PinnedReferences,ReferenceConflict,ReferenceEvaluator,summarize_auxiliary,
)


class ReferenceChecks(unittest.TestCase):
    def setUp(self):
        self.fixture=ConsumerChecks('test_direct_actual_answer_is_saved_with_original_cost_identity')
        self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        self.root=self.fixture.root
        self.package=self.root/'package';(self.package/'evaluation').mkdir(parents=True)
        self.identity=self.fixture.identity
        self.policy=AuxiliaryTaskPolicy(self.identity.benchmark,self.identity.task_id,self.identity.graph,
                                       'a'*64,'fixture',False)
        self.set_references([dict(task_id=self.identity.task_id,reference_query='RETURN 1 AS gold',
                                  return_count='999',validation_labels=dict(valid=False))])

    def set_references(self,rows):
        path=self.package/'evaluation/references.jsonl'
        path.write_bytes(b''.join(encoded(row)+b'\n' for row in rows))
        manifest=dict(positions_per_variant=1,files={'evaluation/references.jsonl':hashlib.sha256(path.read_bytes()).hexdigest()})
        target=self.package/'manifest.json';target.write_bytes(encoded(manifest))
        self.manifest_sha=hashlib.sha256(target.read_bytes()).hexdigest()

    def evaluator(self,policy=None):
        references=PinnedReferences(self.package,self.manifest_sha,[self.identity.task_id])
        return ReferenceEvaluator(references,policy or self.policy)

    def run_source(self,results=(),**options):
        return self.fixture.consume(self.fixture.source(),results,**options)

    def test_reference_query_runs_only_after_sealed_method_in_same_transaction(self):
        outcome,driver,folder=self.run_source([Result(),Result([[1]]),Result(),Result([[1]])],evaluator=self.evaluator())
        self.assertTrue(outcome['evaluation']['auxiliary_exact'])
        self.assertIsNone(outcome['evaluation']['primary_score'])
        self.assertEqual(len(driver.begin_calls),1)
        self.assertEqual([q for q,p in driver.tx.calls],['EXPLAIN RETURN 1 AS x','RETURN 1 AS x',
            'EXPLAIN RETURN 1 AS gold','RETURN 1 AS gold'])
        records=[json.loads(p.read_bytes())['event'] for p in sorted((folder/'capture').glob('*.json'))]
        method_index=next(i for i,r in enumerate(records) if r['kind']=='method_final_saved')
        reference_index=next(i for i,r in enumerate(records) if r['kind']=='read_intent' and r['purpose']=='evaluator')
        self.assertLess(method_index,reference_index)
        self.assertEqual(json.loads((folder/'evaluation/reference.json').read_bytes())['reference_answer']['scope'],
                         outcome['method']['answer']['scope'])

    def test_reference_constructor_performs_no_file_read(self):
        with patch.object(Path,'open',side_effect=AssertionError('no early reference read')):
            self.evaluator()

    def test_bad_method_never_loads_or_executes_reference(self):
        evaluator=self.evaluator()
        with patch.object(evaluator.references,'get',side_effect=AssertionError('no reference for failed method')):
            outcome,driver,_=self.fixture.consume(self.fixture.source(content='CREATE (n) RETURN n'),
                                                  evaluator=evaluator,allow_feedback=True)
        self.assertIsNone(outcome['evaluation'])
        self.assertFalse(evaluator.used)
        self.assertEqual(driver.tx.calls,[])

    def test_refusal_never_loads_reference(self):
        evaluator=self.evaluator()
        with patch.object(evaluator.references,'get',side_effect=AssertionError('no reference for refusal')):
            outcome,driver,_=self.fixture.consume(self.fixture.source(status='refused',reason='content_filter',content=None),
                                                  evaluator=evaluator)
        self.assertEqual(outcome['method']['answer']['status'],'REFUSED')
        self.assertEqual(driver.session_calls,[])

    def test_source_validation_labels_and_return_count_are_not_scores(self):
        outcome,_,_=self.run_source([Result(),Result([[1]]),Result(),Result([[2]])],evaluator=self.evaluator())
        self.assertFalse(outcome['evaluation']['auxiliary_exact'])

    def test_ordering_comes_from_explicit_task_policy(self):
        for ordered,expected in ((False,True),(True,False)):
            with self.subTest(ordered=ordered):
                evaluator=self.evaluator(replace(self.policy,ordered=ordered))
                outcome,_,_=self.run_source([Result(),Result([[1],[2]]),Result(),Result([[2],[1]])],evaluator=evaluator)
                self.assertEqual(outcome['evaluation']['auxiliary_exact'],expected)

    def test_row_multiplicity_is_preserved(self):
        outcome,_,_=self.run_source([Result(),Result([[1],[1]]),Result(),Result([[1]])],evaluator=self.evaluator())
        self.assertFalse(outcome['evaluation']['auxiliary_exact'])

    def test_scalar_bool_and_integer_are_not_silently_equated(self):
        outcome,_,_=self.run_source([Result(),Result([[True]]),Result(),Result([[1]])],evaluator=self.evaluator())
        self.assertFalse(outcome['evaluation']['auxiliary_exact'])

    def test_native_entity_answer_retains_identity_in_same_capture(self):
        node=Node(Graph(),'native-person',1,['Person'],{'display':'same'})
        outcome,_,_=self.run_source([Result(),Result([[node]]),Result(),Result([[node]])],evaluator=self.evaluator())
        self.assertTrue(outcome['method']['answer']['contains_graph_entities'])
        self.assertTrue(outcome['evaluation']['auxiliary_exact'])

    def test_foreign_capture_answer_is_rejected_before_reference_read(self):
        evaluator=self.evaluator()
        def wrong_scope(cap,path,sha,identity):
            value=json.loads(path.read_bytes())
            value['result']['answer']['scope']['capture_id']='another-transaction'
            pin=self.fixture.pin('foreign_method',value)
            return evaluator(cap,pin.path,pin.sha256,identity)
        with patch.object(evaluator.references,'get',side_effect=AssertionError('not loaded')):
            with self.assertRaises(ReferenceConflict):
                self.run_source([Result(),Result([[1]])],evaluator=wrong_scope)

    def test_reference_save_error_is_not_hidden_as_a_bad_reference(self):
        with patch('neo4j_reference_boundary_dev_20261029.save_new',side_effect=OSError('invented disk failure')):
            with self.assertRaises(OSError):
                self.run_source([Result(),Result([[1]]),Result(),Result([[1]])],evaluator=self.evaluator())

    def test_reference_failure_cannot_overwrite_method_or_trigger_feedback(self):
        outcome,_,folder=self.run_source([Result(),Result([[1]]),ClientError('invented reference failure')],
                                          evaluator=self.evaluator(),allow_feedback=True)
        self.assertEqual(outcome['method']['answer']['status'],'EXECUTED')
        self.assertEqual(outcome['evaluation']['reference_status'],'REFERENCE_EXECUTION_ERROR')
        self.assertIsNone(outcome['evaluation']['auxiliary_exact'])
        self.assertIsNone(outcome['feedback_draft'])
        self.assertEqual(json.loads((folder/'method_final.json').read_bytes())['result'],outcome['method'])

    def test_missing_reference_query_is_unknown_not_correct_empty(self):
        self.set_references([dict(task_id=self.identity.task_id,reference_query=None)])
        outcome,driver,_=self.run_source([Result(),Result([])],evaluator=self.evaluator())
        self.assertEqual(outcome['evaluation']['reference_status'],'UNRESOLVED_REFERENCE')
        self.assertIsNone(outcome['evaluation']['auxiliary_exact'])
        self.assertEqual(len(driver.tx.calls),2)

    def test_snapshot_or_task_mismatch_prevents_reference_loading(self):
        for policy in (replace(self.policy,snapshot_sha256='b'*64),replace(self.policy,task_id='other'),
                       replace(self.policy,graph='other'),replace(self.policy,database='other')):
            evaluator=self.evaluator(policy)
            with self.subTest(policy=policy),patch.object(evaluator.references,'get',side_effect=AssertionError('not loaded')):
                with self.assertRaises(ReferenceConflict):
                    self.run_source([Result(),Result([[1]])],evaluator=evaluator)

    def test_missing_or_changed_method_seal_prevents_any_reference_access(self):
        evaluator=self.evaluator();evidence=PlanEvidence(self.root/'cap',max_record_bytes=100000)
        cap=NativeReadCapture(Driver([]),snapshot_sha256='a'*64,database='fixture',transaction_timeout=75,
                              max_rows=10,max_answer_bytes=10000,emit=evidence)
        with cap,patch.object(evaluator.references,'get',side_effect=AssertionError('not loaded')):
            with self.assertRaises(FileNotFoundError):
                evaluator(cap,self.root/'missing.json','a'*64,self.identity)
        evaluator=self.evaluator();wrong=self.fixture.pin('method',{})
        cap=NativeReadCapture(Driver([]),snapshot_sha256='a'*64,database='fixture',transaction_timeout=75,
                              max_rows=10,max_answer_bytes=10000,emit=PlanEvidence(self.root/'cap2',max_record_bytes=100000))
        with cap,patch.object(evaluator.references,'get',side_effect=AssertionError('not loaded')):
            with self.assertRaises(ReplyConflict):evaluator(cap,wrong.path,'b'*64,self.identity)

    def test_reference_manifest_and_reference_bytes_are_pinned(self):
        evaluator=self.evaluator();(self.package/'evaluation/references.jsonl').write_bytes(b'changed')
        with self.assertRaises(ReferenceConflict):
            self.run_source([Result(),Result([[1]])],evaluator=evaluator)

    def test_extra_duplicate_and_missing_reference_positions_do_not_shrink_N(self):
        for rows in ([],[dict(task_id='other',reference_query='RETURN 1')],
                     [dict(task_id=self.identity.task_id)]*2):
            with self.subTest(rows=rows):
                self.set_references(rows)
                with self.assertRaises(ReferenceConflict):self.evaluator().references.get(self.identity.task_id)

    def test_unfrozen_order_and_invalid_snapshot_are_rejected(self):
        for changes in (dict(ordered=None),dict(snapshot_sha256='z'*64)):
            with self.assertRaises(ReferenceConflict):replace(self.policy,**changes)

    def test_automatic_repeated_reference_execution_is_rejected(self):
        evaluator=self.evaluator()
        self.run_source([Result(),Result([[1]]),Result(),Result([[1]])],evaluator=evaluator)
        with self.assertRaises(ReferenceConflict):evaluator(None,None,None,None)

    def test_primary_score_remains_unclaimed_with_auxiliary_bounds_and_all_positions(self):
        result=summarize_auxiliary(['a','b','c','d'],[
            dict(task_id='a',prediction_status='EXECUTED',auxiliary_exact=True),
            dict(task_id='b',prediction_status='REFUSED',auxiliary_exact=None),
            dict(task_id='c',prediction_status='EXECUTED',auxiliary_exact=None)])
        self.assertEqual((result['N'],result['correct'],result['incorrect'],result['unknown'],result['missing']),(4,1,1,2,1))
        self.assertEqual((result['auxiliary_lower_bound'],result['auxiliary_upper_bound']),(0.25,0.75))
        self.assertFalse(result['coverage_complete'])
        self.assertIsNone(result['primary_score'])

    def test_aggregation_rejects_extra_duplicate_or_numeric_truth(self):
        for rows in ([dict(task_id='other',prediction_status='EXECUTED',auxiliary_exact=True)],
                     [dict(task_id='a',prediction_status='EXECUTED',auxiliary_exact=True)]*2,
                     [dict(task_id='a',prediction_status='EXECUTED',auxiliary_exact=1)]):
            with self.subTest(rows=rows),self.assertRaises(ReferenceConflict):summarize_auxiliary(['a'],rows)

    def test_aggregation_does_not_invent_a_failed_status(self):
        with self.assertRaises(ReferenceConflict):
            summarize_auxiliary(['a'],[dict(task_id='a',prediction_status='made-up',auxiliary_exact=None)])


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',required=True);args=parser.parse_args()
    suite=unittest.defaultTestLoader.loadTestsFromTestCase(ReferenceChecks);planned=suite.countTestCases()
    with patch.object(socket,'create_connection',side_effect=AssertionError('No network')), \
         patch.object(socket.socket,'connect',side_effect=AssertionError('No network')):
        result=unittest.TextTestRunner(verbosity=2).run(suite)
    output=Path(args.output);output.mkdir(parents=True,exist_ok=False)
    summary=dict(kind='FABRICATED_REFERENCE_BOUNDARY_AND_AUXILIARY_SCORING_FAKE_DB_NOT_EXPERIMENT',
        planned=planned,completed=result.testsRun,checks_passed=result.testsRun-len(result.errors)-len(result.failures),
        failed=len(result.errors)+len(result.failures),model_calls=0,DB_connections=0,DB_queries=0,
        primary_comparator_authenticated=False,formal_admission=False)
    (output/'summary.json').write_bytes(encoded(summary)+b'\n')
    if not result.wasSuccessful():raise SystemExit(1)


if __name__=='__main__':main()
