"""Subprocess fault/serialization + allowlist checks; no model or database."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0,str(Path(__file__).parent))
from local_native_worker import LocalLineTransport,encode
from frozen_path_resolver import FrozenEvidenceResolver

FAKE='''import json,sys,time
print(json.dumps({"kind":"READY","graph":sys.argv[1]}),flush=True)
for line in sys.stdin:
    value=json.loads(line)
    if value.get("mode")=="exit":sys.exit(2)
    if value.get("mode")=="invalid":print("not-json",flush=True);continue
    if value.get("mode")=="slow":time.sleep(.15)
    print(json.dumps({"status":"CANDIDATES","candidates":[{"z":2,"a":{"z":3,"a":"中文"}}],"echo":value},ensure_ascii=False),flush=True)
'''


class LocalCutoverChecks(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='t1-local-ipc-')
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.fake=self.root/'fake.py'
        self.fake.write_text(FAKE,encoding='utf-8')

    def worker(self,**kwargs):
        worker=LocalLineTransport([sys.executable,str(self.fake),'fixture'],'fixture',self.root/'stderr.log',**kwargs)
        self.addCleanup(worker.close)
        return worker

    def test_canonical_recursive_candidate_order_and_utf8_exact(self):
        worker=self.worker()
        reply=worker.request({'question':'中文请求','parameters':{'z':1,'a':2}})
        self.assertEqual(list(reply['candidates'][0]),['a','z'])
        self.assertEqual(list(reply['candidates'][0]['a']),['a','z'])
        self.assertEqual(reply['echo']['question'],'中文请求')
        receipt=json.loads((worker.directory/'000000.receipt.json').read_text())
        self.assertEqual(receipt['native_writes'],1)
        saved=json.loads((worker.directory/'000000.result.json').read_text(encoding='utf-8'))
        self.assertEqual(encode(saved['candidates']),encode(reply['candidates']))

    def test_native_death_blocks_second_request_and_has_no_second_dispatch(self):
        worker=self.worker()
        with self.assertRaises(RuntimeError):worker.request({'mode':'exit'})
        with self.assertRaises(RuntimeError):worker.request({'mode':'again'})
        self.assertEqual(len(list(worker.directory.glob('*.dispatch.json'))),1)
        self.assertTrue((worker.directory/'000000.unknown.json').exists())

    def test_invalid_response_poisoned_channel(self):
        worker=self.worker()
        with self.assertRaises(ValueError):worker.request({'mode':'invalid'})
        with self.assertRaises(RuntimeError):worker.request({'query':'do not resend'})
        self.assertFalse((worker.directory/'000001.request.json').exists())

    def test_timeout_does_not_use_late_result_as_next_request(self):
        worker=self.worker(request_timeout=.02)
        with self.assertRaises(RuntimeError):worker.request({'mode':'slow'})
        with self.assertRaises(RuntimeError):worker.request({'question':'do not send'})
        self.assertEqual(worker.sequence,0)

    def test_wrong_ready_graph_fails_and_child_closes(self):
        with self.assertRaises(RuntimeError):
            LocalLineTransport([sys.executable,str(self.fake),'wrong'],'fixture',self.root/'stderr.log')
        self.assertTrue((self.root/'native_ipc/closed.json').exists())

    def test_existing_trace_prevents_implicit_worker_restart(self):
        worker=self.worker()
        with self.assertRaises(FileExistsError):
            LocalLineTransport([sys.executable,str(self.fake),'fixture'],'fixture',self.root/'another.log')
        self.assertEqual(worker.sequence,0)

    def test_original_journal_payload_bound_precedes_dispatch(self):
        worker=self.worker()
        with self.assertRaises(ValueError):worker.request({'query':'x'*(4*1024*1024)})
        self.assertEqual(list(worker.directory.glob('*.dispatch.json')),[])

    def evidence_fixture(self,target='evidence/a.json'):
        packet=self.root/'packet';(packet/'control').mkdir(parents=True);(packet/'evidence').mkdir()
        data=b'{"answer_json":"[[1]]"}'
        (packet/'evidence/a.json').write_bytes(data)
        name=r'C:\original\evidence.json'
        (packet/'control/evidence_path_map.json').write_text(json.dumps({name:target}))
        freeze=self.root/'freeze.json'
        freeze.write_text(json.dumps({'evidence_files':{name:hashlib.sha256(data).hexdigest()}}))
        return packet,freeze,name,data

    def test_evidence_exact_sha_and_inline_answer_not_rewritten(self):
        packet,freeze,name,data=self.evidence_fixture()
        resolver=FrozenEvidenceResolver(packet,freeze)
        self.assertEqual(resolver.sha(name),hashlib.sha256(data).hexdigest())
        self.assertEqual(resolver.resolve(name).read_bytes(),data)
        with self.assertRaises(ValueError):resolver.sha(r'C:\unlisted\secret.json')

    def test_modified_evidence_rejected(self):
        packet,freeze,name,data=self.evidence_fixture()
        resolver=FrozenEvidenceResolver(packet,freeze)
        (packet/'evidence/a.json').write_text('{}')
        with self.assertRaises(ValueError):resolver.sha(name)

    def test_evidence_path_escape_rejected(self):
        packet,freeze,_,_=self.evidence_fixture('../outside.json')
        with self.assertRaises(ValueError):FrozenEvidenceResolver(packet,freeze)


if __name__=='__main__':unittest.main(verbosity=2)
