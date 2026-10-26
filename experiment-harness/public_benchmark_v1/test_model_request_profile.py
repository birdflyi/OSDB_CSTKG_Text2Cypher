"""No actual model calls; injected sender uses a temporary synthetic ledger."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest

from attempt_budget import AttemptBudget
from model_transport import ModelTransport
from model_request_profile import build_model_payload


class ModelRequestProfileTests(unittest.TestCase):
    def setUp(self):
        self.config = {'endpoint': 'http://unused.invalid/v1/chat/completions',
            'api_key': 'synthetic-unit-test-only', 'model': 'deepseek-v4.1-flash-n24-112',
            'temperature': .2, 'top_p': .72,
            'chat_template_kwargs': {'thinking': True, 'reasoning_effort': 50}}

    def test_nested_profile_preserves_exact_fields_and_types(self):
        payload = build_model_payload(self.config, [{'role': 'user', 'content': 'fixture'}], max_tokens=8192)
        self.assertNotIn('reasoning_effort', payload)
        self.assertNotIn('api_key', payload)
        self.assertEqual(payload['top_p'], .72)
        self.assertIs(payload['chat_template_kwargs']['thinking'], True)
        self.assertIs(type(payload['chat_template_kwargs']['reasoning_effort']), int)
        self.assertEqual(payload['chat_template_kwargs']['reasoning_effort'], 50)
        self.config['chat_template_kwargs']['reasoning_effort'] = 2
        self.assertEqual(payload['chat_template_kwargs']['reasoning_effort'], 50)

    def test_legacy_payload_bytes_are_unchanged(self):
        config = {k:v for k,v in self.config.items() if k!='chat_template_kwargs'}
        config.update(model='legacy', top_p=.95, reasoning_effort='high')
        messages=[{'role':'user','content':'legacy fixture'}]
        old={'model':config['model'],'messages':messages,'temperature':config['temperature'],
             'top_p':config['top_p'],'reasoning_effort':config['reasoning_effort'],'max_tokens':8192}
        new=build_model_payload(config,messages,max_tokens=8192)
        encode=lambda v:json.dumps(v,ensure_ascii=False,separators=(',',':')).encode('utf-8')
        self.assertEqual(encode(new),encode(old))

    def test_ambiguous_or_malformed_profiles_fail(self):
        configs=[{**self.config,'reasoning_effort':'high'}]
        for field,value in [('thinking','true'),('reasoning_effort',True),('reasoning_effort','50')]:
            config=deepcopy(self.config);config['chat_template_kwargs'][field]=value;configs.append(config)
        config=deepcopy(self.config);config['chat_template_kwargs']['api_key']='must-not-forward';configs.append(config)
        for config in configs:
            with self.subTest(config=config):
                with self.assertRaises(ValueError):build_model_payload(config,[],max_tokens=8192)

    def test_transport_captures_profile_and_only_synthetic_attempts(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);approval=root/'approval.json'
            approval.write_text(json.dumps({'approved_cumulative_cap':3}))
            budget=AttemptBudget(root/'budget',approval)
            captured=[]
            def sender(config,wire):
                captured.append(json.loads(wire))
                return {'http_status':200,'body':{'choices':[{'finish_reason':'stop',
                    'message':{'content':'synthetic response'}}]}}
            client=ModelTransport(self.config,budget,root/'run',send=sender)
            self.config['chat_template_kwargs']['thinking']=False
            client.complete([],purpose='injected unit test; not a real call')
            self.assertTrue(captured[0]['chat_template_kwargs']['thinking'])
            self.assertEqual(budget.inspect()['reserved'],1)
            policy=json.loads((root/'run/transport_policy.json').read_text())
            self.assertEqual(policy['wire_generation_profile']['chat_template_kwargs'],
                             {'thinking':True,'reasoning_effort':50})
            self.assertNotIn('api_key',policy['wire_generation_profile'])
            self.assertEqual(policy['transport_parallelism'],1)


if __name__=='__main__':unittest.main()
