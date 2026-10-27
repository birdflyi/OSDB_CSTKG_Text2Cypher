"""Offline-accepted central-launch guard; never a restart or network client.

One owned controller lease beside the policy; every method shares its slots.
Remote workers must route through that controller, not independent policy copies.
The existing workers remain unchanged; integrating this guard is a launch gate.
"""
from contextlib import contextmanager
import os,socket,threading,uuid
from pathlib import Path
from osdb_scoped_attempts_dev import read,sha,wire,immutable
from osdb_budgeted_model_client_live_v2 import BudgetedModelClient


def admitted_policy(path,requested):
 p=read(Path(path))
 if (type(requested) is not int or not 1<=requested<=64 or
  type(p.get('max_total_model_requests_inflight')) is not int or p['max_total_model_requests_inflight']!=64 or
  p.get('allow_128') is not False):raise RuntimeError('Human cap64 required; requests above64 rejected')
 if p.get('waiting_for_user_node_restart') is not False:raise RuntimeError('User node restart pending; no dispatch')
 if p.get('model_dispatch_during_restart') is not False:raise RuntimeError('Invalid restart policy')
 return sha(path)


class CentralModelGate:
 def __init__(self,policy,requested):
  self.policy=Path(policy).resolve();self.requested=requested
  self.policy_sha=admitted_policy(self.policy,requested)
  self.path=self.policy.parent/'.t1-global-model-dispatch.lock'
  self.mutex=threading.RLock();self.semaphore=threading.BoundedSemaphore(requested)
  self.active=False;self.inflight=0;self.peak=0;self.stopped=False;self.unknown=[]
  self.id=uuid.uuid4().hex
  self.lock_wire=wire({'pid':os.getpid(),'hostname':socket.gethostname(),'policy_sha256':self.policy_sha,
   'limit':requested,'human_global_cap':64,'lease_id':self.id,'single_central_model_dispatch_controller':True})
 def __enter__(self):
  if admitted_policy(self.policy,self.requested)!=self.policy_sha:raise RuntimeError('Policy changed before lease')
  descriptor=os.open(self.path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
  with os.fdopen(descriptor,'wb') as f:f.write(self.lock_wire);f.flush();os.fsync(f.fileno())
  self.active=True
  try:self.check()
  except BaseException:
   if self.path.read_bytes()==self.lock_wire:self.path.unlink()
   self.active=False;raise
  return self
 def check(self):
  if not self.active or self.stopped:raise RuntimeError('Central model gate closed/stopped')
  try:
   if admitted_policy(self.policy,self.requested)!=self.policy_sha:raise RuntimeError('Policy changed; stop new dispatch')
   if self.path.read_bytes()!=self.lock_wire:raise RuntimeError('Foreign/changed dispatch lock; preserve for audit')
  except BaseException:self.stopped=True;raise
 @contextmanager
 def slot(self,wait_seconds=1):
  self.check()
  if not self.semaphore.acquire(timeout=wait_seconds):raise RuntimeError('Model slot unavailable; no reservation or request sent')
  try:
   with self.mutex:self.check();self.inflight+=1;self.peak=max(self.peak,self.inflight)
   try:yield
   finally:
    with self.mutex:self.inflight-=1
  finally:self.semaphore.release()
 def preserve_unknown(self,request_id):
  with self.mutex:self.stopped=True;self.unknown.append(request_id)
 def __exit__(self,*unused):
  with self.mutex:
   self.active=False
   if self.inflight:raise RuntimeError('In-flight model work not drained; lock retained')
   if self.path.read_bytes()!=self.lock_wire:raise RuntimeError('Foreign/changed dispatch lock retained')
   if self.unknown:
    immutable(self.policy.parent/('model_unknown_'+self.id+'.json'),{'lease_id':self.id,'requests':self.unknown,
     'dispatch_lock_retained':True,'automatic_resume':False})
   else:self.path.unlink()


class LimitedModelClient(BudgetedModelClient):
 """Acquire central permit BEFORE any legacy reservation/claim or transport."""
 def __init__(self,*args,model_gate,**kwargs):
  model_gate.check();self.model_gate=model_gate;super().__init__(*args,**kwargs)
 def generate(self,path_key,phase,outcome=None):
  with self.model_gate.slot():
   try:return super().generate(path_key,phase,outcome)
   except BaseException:
    self.model_gate.preserve_unknown(path_key+'__'+phase);raise
