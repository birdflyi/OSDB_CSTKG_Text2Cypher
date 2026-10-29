"""Synthetic source/answer integration; every socket connection is forbidden."""

from dataclasses import asdict, replace
import json
from pathlib import Path
import socket
import unittest
from unittest.mock import patch

import accept_neo4j_reply_consumer_20261029 as fixtures
from audited_model_reply_dev_20261029 import RecordedReply, ReplyConflict
from engine.config import canonical, digest
from engine.scheduler import sent_wire
from text2gql_receipt_consumer_dev_20261029 import consume_audited_reply


class AuditedNativeChecks(fixtures.ConsumerChecks):
    """Also regress the shared reader's original fake-Neo4j contracts."""

    def routed_source(self, *, evidence='a'*64, mutate_physical=False):
        identity = replace(self.identity,benchmark='text2gql')
        logical = dict(model=identity.model,messages=[dict(role='user',content='public fixture')])
        raw = canonical(logical)
        physical = sent_wire(raw,'served-alias')
        if mutate_physical:
            value = json.loads(physical)
            value['messages'][0]['content'] = 'changed public fixture'
            physical = canonical(value)
        logical_pin = self.pin('logical',logical,raw=raw)
        request_pin = self.pin('physical',{},raw=physical)
        body = dict(model='served-alias',choices=[dict(finish_reason='stop',message=dict(content='RETURN 7'))],
                    usage=dict(prompt_tokens=2,completion_tokens=3,total_tokens=5))
        terminal = dict(asdict(identity),model='served-alias',requested_model=identity.model,
            logical_key=identity.key,request_id='routed',budget_slot=1,attempt_no=1,charged=True,
            status='ok',http_status=200,body=body,content='RETURN 7',finish_reason='stop',usage=body['usage'],
            logical_request_sha256=digest(raw),alias_evidence_sha256='a'*64)
        slot = dict(slot=1,attempt_no=1,request_id='routed',logical_key=identity.key,
                    request_sha256=request_pin.sha256,scope='fixture')
        return RecordedReply(identity,request=request_pin,terminal=self.pin('terminal',terminal),
            slot=self.pin('slot',slot),logical_request=logical_pin,alias_evidence_sha256=evidence)

    def test_routed_source_preserves_both_model_identities(self):
        source = self.routed_source()
        self.assertEqual(source.status,'READY')
        self.assertEqual(source.request['model'],source.identity.model)
        self.assertEqual(source.source_link()['served_model'],'served-alias')
        self.assertEqual(source.terminal['body']['model'],'served-alias')

    def test_alias_proof_is_required_and_message_rewrite_is_rejected(self):
        with self.assertRaises(ReplyConflict):
            self.routed_source(evidence=None)
        with self.assertRaises(ReplyConflict):
            self.routed_source(mutate_physical=True)

    def consume_native(self, source, *, answer=None):
        calls = []
        class Journal:
            def execute(self, *args):
                calls.append(args)
                return answer or dict(status='EXECUTED',complete=True,columns=['n'],rows=[[7]])
        folder = self.root/('native-'+str(self.number))
        result = consume_audited_reply(source,None,Journal(),directory=folder,allow_feedback=True)
        return result,calls,folder

    def test_unknown_charge_preserved_without_db_or_feedback(self):
        source = self.source(identity=replace(self.identity,benchmark='text2gql'),status='unknown',charged=None)
        result,calls,folder = self.consume_native(source)
        self.assertEqual(calls,[])
        self.assertIsNone(result['source']['source_charged'])
        self.assertEqual(result['result']['final_answer']['status'],'UNKNOWN')
        self.assertIsNone(result['feedback_draft'])
        self.assertTrue((folder/'method_final.json').is_file())

    def test_native_answer_uses_audited_alias_and_keeps_source_charge(self):
        source = self.routed_source()
        result,calls,folder = self.consume_native(source)
        self.assertEqual(len(calls),1)
        self.assertEqual(result['result']['final_answer']['rows'],[[7]])
        self.assertEqual(result['source']['source_charge_key'],'routed')
        self.assertIsNone(result['monetary_cost'])
        self.assertEqual(result['new_model_calls'],0)
        self.assertEqual(json.loads((folder/'method_final.json').read_bytes())['result'],result['result'])

    def test_native_operation_feedback_is_unsent_and_saved_after_method(self):
        source = self.source(identity=replace(self.identity,benchmark='text2gql'))
        result,calls,folder = self.consume_native(source,answer=dict(status='QUERY_ERROR',
            complete=False,columns=None,rows=None,error='synthetic parser error'))
        self.assertIsNotNone(result['feedback_draft'])
        self.assertFalse(result['feedback_dispatched'])
        self.assertLessEqual((folder/'method_final.json').stat().st_mtime_ns,
                             (folder/'outcome.json').stat().st_mtime_ns)


if __name__ == '__main__':
    with patch.object(socket.socket,'connect',side_effect=AssertionError('No network')), \
         patch.object(socket,'create_connection',side_effect=AssertionError('No network')):
        unittest.main()
