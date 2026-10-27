"""Ownership/corruption and cancellation boundaries; no real DB/model calls."""
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import json, threading, time, unittest
from osdb_owned_graph_factory_dev import OwnedGraphFactory, bounded_database_call, ClientDeadlineExceeded
from osdb_scoped_attempts_dev import sha


class FactoryBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.tmp=TemporaryDirectory(); self.root=Path(self.tmp.name)
        self.source=self.root/'cold'; (self.source/'data/transactions').mkdir(parents=True)
        (self.source/'data/node').write_bytes(b'actual cold bytes')
        self.cold=self.source/'new_store_manifest_after_shutdown.json'
        self.cold.write_text(json.dumps({'data/node':{'bytes':17,'sha256':'wrong'}}))
        profile=self.root/'profile.json';profile.write_text('{}')
        self.profiles={g:{'path':str(profile),'sha256':sha(profile)} for g in ('redis','duckdb','iotdb','elasticsearch')}
        self.inventory=self.root/'inventory.json'
        self.inventory.write_text(json.dumps({'cold_receipts':{g:{'root':str(self.source),
            'cold_manifest_sha256':sha(self.cold)} for g in self.profiles}}))
        self.lock=self.root/'factory.lock'
    def tearDown(self): self.tmp.cleanup()
    def factory(self):
        return OwnedGraphFactory(self.root/'runtime',self.inventory,sha(self.inventory),
            self.profiles,self.root/'home',self.lock)
    def test_foreign_factory_lock_is_preserved_without_starting_process(self):
        self.lock.write_bytes(b'foreign')
        with patch('osdb_owned_graph_factory_dev.live',return_value=False),patch('subprocess.Popen',side_effect=AssertionError('No boot')):
            with self.assertRaises(FileExistsError):
                with self.factory(): pass
        self.assertEqual(self.lock.read_bytes(),b'foreign')
    def test_corrupted_cold_bytes_fail_before_process_and_release_own_lock(self):
        with patch('osdb_owned_graph_factory_dev.live',return_value=False),patch('subprocess.Popen',side_effect=AssertionError('No boot')):
            with self.assertRaisesRegex(RuntimeError,'Actual cold source bytes differ'):
                with self.factory() as factory:
                    with factory('redis'): self.fail('Never admit corrupted bytes')
        self.assertFalse(self.lock.exists())
        self.assertEqual((self.source/'data/node').read_bytes(),b'actual cold bytes')
        self.assertTrue((self.root/'runtime/redis/failure.json').exists())
    def test_deadline_aborts_once_and_classifies_unknown_io_without_replay(self):
        signal=threading.Event(); calls=[]
        def io():
            calls.append('io');self.assertTrue(signal.wait(2));raise ConnectionError('Owned process stopped')
        def abort(): calls.append('abort');signal.set()
        with self.assertRaises(ClientDeadlineExceeded): bounded_database_call(io,abort,.02)
        self.assertEqual(calls,['io','abort'])
    def test_completed_call_cancels_watchdog_before_return(self):
        aborted=[]
        self.assertEqual(bounded_database_call(lambda:7,lambda:aborted.append(True),.03),7)
        time.sleep(.05);self.assertEqual(aborted,[])


if __name__=='__main__': unittest.main()
