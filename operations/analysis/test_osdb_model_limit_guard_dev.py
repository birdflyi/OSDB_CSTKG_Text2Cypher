"""Synthetic permits only, with socket creation forbidden; no real approvals."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch
import json,tempfile,threading,time,unittest
from osdb_model_limit_guard_dev import CentralModelGate,LimitedModelClient,admitted_policy


class GuardTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.policy=Path(self.temp.name)/'policy.json';self.save(False)
  self.sockets=patch('socket.socket',side_effect=AssertionError('Offline guard must not use sockets'));self.socket_guard=self.sockets.start()
 def tearDown(self):
  self.assertEqual(self.socket_guard.call_count,0);self.sockets.stop();self.temp.cleanup()
 def save(self,waiting):self.policy.write_text(json.dumps({'max_total_model_requests_inflight':64,'allow_128':False,
  'waiting_for_user_node_restart':waiting,'model_dispatch_during_restart':False,'kind':'SYNTHETIC_NOT_HUMAN_APPROVAL'}))
 def test_restart_rejects_before_creating_lock(self):
  self.save(True)
  with self.assertRaisesRegex(RuntimeError,'restart'):CentralModelGate(self.policy,64)
  self.assertFalse((self.policy.parent/'.t1-global-model-dispatch.lock').exists())
 def test_64_allowed_128_65_and_bool_rejected(self):
  for n in (1,16,32,64):self.assertTrue(admitted_policy(self.policy,n))
  for n in (65,128,True,0):
   with self.assertRaises(RuntimeError):admitted_policy(self.policy,n)
 def test_global_lease_excludes_second_controller_preserves_first(self):
  with CentralModelGate(self.policy,4) as first:
   before=first.path.read_bytes()
   with self.assertRaises(FileExistsError):
    with CentralModelGate(self.policy,4):pass
   self.assertEqual(first.path.read_bytes(),before)
  self.assertFalse(first.path.exists())
 def test_shared_permits_bound_all_methods_and_drain(self):
  entered=threading.Event();release=threading.Event()
  with CentralModelGate(self.policy,4) as gate:
   def model(method):
    with gate.slot(wait_seconds=2):
     if gate.inflight==4:entered.set()
     release.wait(1)
   with ThreadPoolExecutor(max_workers=12) as pool:
    futures=[pool.submit(model,'direct' if i%2 else 'ours') for i in range(12)]
    self.assertTrue(entered.wait(1));self.assertEqual(gate.peak,4);release.set()
    for future in futures:future.result()
   self.assertEqual(gate.inflight,0);self.assertEqual(gate.peak,4)
 def test_policy_change_stops_new_slots_after_existing_drain(self):
  with CentralModelGate(self.policy,4) as gate:
   with gate.slot():
    self.save(True)
    with self.assertRaisesRegex(RuntimeError,'restart'):
     with gate.slot():pass
   self.assertEqual(gate.inflight,0)
 def test_unknown_preserves_dispatch_lock_and_blocks_next_lease(self):
  with CentralModelGate(self.policy,4) as gate:
   with gate.slot():gate.preserve_unknown('synthetic_unknown__direct__initial')
   with self.assertRaises(RuntimeError):gate.check()
  self.assertTrue(gate.path.exists());self.assertEqual(len(list(self.policy.parent.glob('model_unknown_*.json'))),1)
  with self.assertRaises(FileExistsError):
   with CentralModelGate(self.policy,4):pass
 def test_foreign_lock_bytes_never_cleared(self):
  gate=CentralModelGate(self.policy,4);gate.__enter__();gate.path.write_bytes(b'foreign')
  with self.assertRaisesRegex(RuntimeError,'Foreign'):gate.__exit__(None,None,None)
  self.assertEqual(gate.path.read_bytes(),b'foreign')
 def test_limited_client_does_not_touch_budget_without_permit(self):
  from osdb_budgeted_model_client_dev import BudgetedModelClient
  with CentralModelGate(self.policy,1) as gate:
   client=object.__new__(LimitedModelClient);client.model_gate=gate
   self.save(True)
   with patch.object(BudgetedModelClient,'generate',side_effect=AssertionError('Reservation not allowed')) as parent:
    with self.assertRaises(RuntimeError):client.generate('fixture__direct','initial')
    self.assertEqual(parent.call_count,0)


if __name__=='__main__':unittest.main()
