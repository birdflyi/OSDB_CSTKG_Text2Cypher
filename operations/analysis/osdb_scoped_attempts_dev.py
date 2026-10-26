"""A separate OSDB allowance over the UNCHANGED cumulative attempt ledger.

No approval writer, network, restart or lock recovery. The original approval
does not enable this scope. Approval fixtures belong in temporary test stores.
"""
import hashlib,json,os,re,socket,threading,uuid
from pathlib import Path

SCOPE='OSDB_EXPOSED_DEV_V1_20261027'


def wire(value):return (json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode('utf-8')
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def read(path):return json.loads(Path(path).read_text(encoding='utf-8-sig'))
def immutable(path,value):
 path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
 with path.open('xb') as f:f.write(wire(value));f.flush();os.fsync(f.fileno())


class ScopedAttempts:
 def __init__(self,legacy,directory,plan_sha256,allowed_paths):
  self.legacy=legacy;self.directory=Path(directory);self.plan_sha256=plan_sha256
  self.allowed_paths=set(allowed_paths);self.mutex=threading.RLock();self.active=False
  self.reserved={};self.terminals={};self.claimed=set();self.stopped=False;self.consecutive_infra=0

 def approval(self):
  a=read(self.legacy.approval);pool=a.get('osdb_exposed_development') or {}
  if (a.get('approved_cumulative_cap')!=12096 or pool.get('approved') is not True or
   type(pool.get('approved_additional_attempts')) is not int or pool['approved_additional_attempts']!=96 or
   pool.get('scope_id')!=SCOPE or pool.get('plan_manifest_sha256')!=self.plan_sha256):
   raise RuntimeError('Explicit matching separate OSDB 96-attempt approval is absent')
  return sha(self.legacy.approval)

 def release(self):
  if self.lock_path.read_bytes()!=self.lock_wire:raise RuntimeError('Foreign/changed budget lock retained for audit')
  self.lock_path.unlink()

 def __enter__(self):
  self.approval()  # Fail before creating a campaign or touching the budget lock.
  self.directory.mkdir(parents=True,exist_ok=False)
  self.lock_path=Path(self.legacy.ledger)/'.t1-reservation.lock'
  self.lock_wire=wire({'pid':os.getpid(),'hostname':socket.gethostname(),'run_id':uuid.uuid4().hex,'scope':SCOPE})
  descriptor=os.open(self.lock_path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
  with os.fdopen(descriptor,'wb') as f:f.write(self.lock_wire);f.flush();os.fsync(f.fileno())
  try:
   self.approval_sha256=self.approval();snapshot=self.legacy.inspect()
   if snapshot['pending']:raise RuntimeError('Unterminated physical attempt; no resend')
   scope_records=[];base=0;seen=set()
   for p in Path(self.legacy.ledger).glob('*.json'):
    r=read(p);scope=r.get('scope_id')
    if scope is None:base+=1
    elif scope==SCOPE:
     if r.get('plan_manifest_sha256')!=self.plan_sha256:raise RuntimeError('Scope/plan history mismatch')
     if r['request_id'] in seen:raise RuntimeError('Duplicate logical generation history')
     seen.add(r['request_id']);scope_records.append(r)
    else:raise RuntimeError('Unadmitted scoped history')
   if base>12000 or len(scope_records)>=96:raise RuntimeError('Approved pool exhausted')
   self.used_ids=seen;self.used_scope=len(scope_records);self.base=snapshot['reserved']
   self.before={k:snapshot[k] for k in ('reserved','cap','remaining','pending')}
   immutable(self.directory/'scope_lease.json',{'scope':SCOPE,'plan_manifest_sha256':self.plan_sha256,
    'approval_sha256':self.approval_sha256,'budget_before':self.before,'prior_scope_attempts':self.used_scope,
    'max_total_scope_attempts':96,'same_request_max_attempts':1,'automatic_transport_retries':0})
   self.active=True;return self
  except BaseException:self.release();raise

 def reserve(self,request_id,body):
  with self.mutex:
   if not self.active or self.stopped:raise RuntimeError('Campaign closed/stopped')
   if sha(self.legacy.approval)!=self.approval_sha256:raise RuntimeError('Approval changed during campaign')
   if not re.fullmatch(r'[A-Za-z0-9_-]+__(?:direct|ours)__(?:initial|feedback)',request_id):raise ValueError('Unsafe request identity')
   path_key,phase=request_id.rsplit('__',1)
   if path_key not in self.allowed_paths:raise RuntimeError('Request outside sealed development plan')
   fingerprint=hashlib.sha256(wire(body)).hexdigest()
   if request_id in self.reserved:
    if self.reserved[request_id]['request_sha256']!=fingerprint:raise RuntimeError('Idempotency conflict')
    return dict(self.reserved[request_id])
   if request_id in self.used_ids:raise RuntimeError('Historical logical generation cannot be resent')
   if self.used_scope+len(self.reserved)>=96:raise RuntimeError('Separate OSDB allowance exhausted')
   p=self.directory/'attempts'/request_id/'request.json';immutable(p,body)
   slot=self.base+len(self.reserved)+1
   record={'request_path':str(p.resolve()),'request_sha256':sha(p),'pid':os.getpid(),
    'scope_id':SCOPE,'request_id':request_id,'plan_manifest_sha256':self.plan_sha256,
    'purpose':'OSDB exposed development '+request_id,'approval_sha256':self.approval_sha256,
    'approved_cap_at_reservation':12096}
   immutable(Path(self.legacy.ledger)/f'{slot:04d}.json',record)
   ack={'slot':slot,'request_sha256':fingerprint,'request_path':str(p)};self.reserved[request_id]=ack
   return dict(ack)

 def claim_once(self,request_id):
  with self.mutex:
   if not self.active or self.stopped:raise RuntimeError('Campaign closed/stopped before claim')
   if request_id not in self.reserved or request_id in self.claimed:raise RuntimeError('Unknown/already dispatched physical attempt')
   p=Path(self.reserved[request_id]['request_path']).with_name('dispatch_claim.json')
   immutable(p,{'request_id':request_id,'budget_slot':self.reserved[request_id]['slot'],'automatic_retry':False})
   self.claimed.add(request_id)

 def terminal(self,request_id,value,infrastructure_failure):
  with self.mutex:
   if request_id not in self.claimed or value['budget_slot']!=self.reserved[request_id]['slot']:raise RuntimeError('Unclaimed/wrong physical terminal')
   fingerprint=hashlib.sha256(wire(value)).hexdigest()
   if request_id in self.terminals:
    if self.terminals[request_id]!=fingerprint:raise RuntimeError('Conflicting terminal retained for audit')
    return
   immutable(Path(self.reserved[request_id]['request_path']).with_name('terminal.json'),value)
   self.terminals[request_id]=fingerprint
   self.consecutive_infra=self.consecutive_infra+1 if infrastructure_failure else 0
   if self.consecutive_infra>=3:self.stopped=True
   # Append immutable state receipts; no monitoring client can resume dispatch.
   immutable(self.directory/'terminal_states'/f'{len(self.terminals):04d}.json',
    {'stopped':self.stopped,'consecutive_infrastructure_failures':self.consecutive_infra,
     'reserved':len(self.reserved),'terminal':len(self.terminals),'pending':len(self.reserved)-len(self.terminals)})

 def __exit__(self,*unused):
  with self.mutex:
   self.active=False
   try:
    snapshot=self.legacy.inspect()
    immutable(self.directory/'scope_close.json',{k:snapshot[k] for k in ('reserved','cap','remaining','pending')})
   finally:self.release()
