"""Budget-first one-shot model transport and OSDB final-answer orchestration.

Construction sends nothing. No CLI/resume, approval writer or remote service
manager. Real gateway/model+graph acceptance remains required before use.
"""
from copy import deepcopy
import hashlib,http.client,json,socket,threading,time
from pathlib import Path
from urllib.parse import urlsplit
from osdb_scoped_attempts_dev import immutable,read,sha,wire,SCOPE
from osdb_response_boundary_dev import execute_response,feedback_messages,decode

VERSION='osdb-budgeted-model-client-dev-1'


def load_model_inputs(directory,manifest_sha256):
 """Read only hash-pinned message files; never open evaluator_only.json."""
 directory=Path(directory).resolve();manifest=directory/'model_path_manifest.json'
 if sha(manifest)!=manifest_sha256:raise RuntimeError('Model path manifest changed')
 rows=read(manifest)
 if type(rows) is not list or len(rows)!=48:raise RuntimeError('Expected all 48 initial development paths')
 result={}
 for row in rows:
  key=row['case_id']+'__'+row['arm'];p=Path(row['model_input_path']).resolve()
  if key in result or row['arm'] not in ('direct','ours'):raise RuntimeError('Duplicate/unadmitted model path')
  if p.parent!=(directory/'model_inputs').resolve() or sha(p)!=row['model_input_sha256']:
   raise RuntimeError('Model input path/hash differs')
  payload=read(p)
  if set(payload)!={'messages'}:raise RuntimeError('Model payload must contain messages only')
  result[key]=payload['messages']
 cases={r['case_id'] for r in rows}
 if len(cases)!=24 or any({r['arm'] for r in rows if r['case_id']==case}!={'direct','ours'} for case in cases):
  raise RuntimeError('Incomplete method pairing')
 return result


class OneShotHTTP:
 """One connection and POST, zero SDK, redirect or transport retries."""
 def __init__(self,private):
  self.private=private
  endpoint=urlsplit(private['endpoint'])
  if endpoint.scheme not in ('http','https') or not endpoint.hostname or endpoint.username or endpoint.fragment:
   raise ValueError('Explicit HTTP(S) endpoint required')
  self.endpoint=endpoint

 def __call__(self,body):
  endpoint=self.endpoint
  cls=http.client.HTTPSConnection if endpoint.scheme=='https' else http.client.HTTPConnection
  connection=cls(endpoint.hostname,endpoint.port,timeout=10);started=time.monotonic();phase='connecting';written=False
  terminal={};deadline=threading.Event();timer=None
  try:
   connection.connect();phase='sending_request';owned_socket=connection.sock
   def abort():
    deadline.set()
    if owned_socket is not None:
     try:owned_socket.shutdown(socket.SHUT_RDWR)
     except OSError:pass
   timer=threading.Timer(240,abort);timer.daemon=True;timer.start()
   headers={'Authorization':'Bearer '+self.private['api_key'],'Content-Type':'application/json; charset=utf-8'}
   if self.private.get('host_header'):headers['Host']=self.private['host_header']
   target=(endpoint.path or '/')+('?' + endpoint.query if endpoint.query else '')
   connection.request('POST',target,body=wire(body),headers=headers)
   written=True;phase='waiting_headers'
   if connection.sock:connection.sock.settimeout(240)
   response=connection.getresponse();terminal['http_status']=response.status
   phase='reading_body';raw=response.read(4*1024**2+1);terminal['raw_response_bytes']=raw
   if len(raw)>4*1024**2:raise ValueError('Response size limit exceeded')
   if getattr(response,'length',None) not in (None,0):raise http.client.IncompleteRead(raw)
   try:
    def reject(value):raise ValueError('Non-finite response JSON')
    parsed=json.loads(raw,parse_constant=reject)
    wire(parsed)  # Also reject exponent overflow (e.g. 1e999).
    terminal['body']=parsed
   except (ValueError,UnicodeError):terminal['body']=None;terminal['response_parse_error']=True
  except (OSError,http.client.HTTPException,ValueError) as exc:
   terminal.update(http_status=terminal.get('http_status'),transport_error=type(exc).__name__)
  finally:
   if timer:timer.cancel()
   connection.close()
  terminal.update(seconds=time.monotonic()-started,last_network_phase=phase,request_write_completed=written,
   remote_outcome_unknown=bool(terminal.get('transport_error') and phase!='connecting'),
   post_connect_deadline_seconds=240,deadline_expired=deadline.is_set())
  return terminal


def token(value):return value if type(value) is int and value>=0 else None


def normalized_reply(terminal,expected_model):
 body=terminal.get('body');choice=None
 if type(body) is dict and type(body.get('choices')) is list and len(body['choices'])==1:
  choice=body['choices'][0]
 valid=(terminal.get('http_status')==200 and not terminal.get('transport_error') and not terminal.get('response_parse_error')
  and type(body) is dict and body.get('model')==expected_model and type(choice) is dict
  and type(choice.get('message')) is dict and (type(choice['message'].get('content')) is str or
   (choice.get('finish_reason')=='length' and choice['message'].get('content') is None))
  and choice.get('finish_reason') in ('stop','length'))
 usage=body.get('usage') if type(body) is dict and type(body.get('usage')) is dict else {}
 prompt_details=usage.get('prompt_tokens_details') if type(usage.get('prompt_tokens_details')) is dict else {}
 completion_details=usage.get('completion_tokens_details') if type(usage.get('completion_tokens_details')) is dict else {}
 metrics={k:token(usage.get(k)) for k in ('prompt_tokens','completion_tokens','total_tokens')}
 metrics.update(cached_tokens=token(prompt_details.get('cached_tokens')),reasoning_tokens=token(completion_details.get('reasoning_tokens')))
 return {'http_status':terminal.get('http_status'),'transport_error':None if valid else 'InfrastructureOrEnvelopeFailure',
  'finish_reason':choice.get('finish_reason') if valid else None,'content':(choice['message']['content'] or '') if valid else None,
  'usage':metrics,'response_characters':len(choice['message']['content'] or '') if valid else None}


class BudgetedModelClient:
 def __init__(self,lease,private,initial_messages,admission,transport=None):
  if (admission.get('scope_id')!=SCOPE or admission.get('plan_manifest_sha256')!=lease.plan_sha256 or
   admission.get('route_repair_verified') is not True or admission.get('online_launch_accepted') is not True or
   admission.get('formal_prediction') is not False):raise RuntimeError('Matching route/online development launch admission absent')
  expected={'model':'ling-3.0-flash-n72-75','temperature':0.2,'top_p':0.95,'reasoning_effort':'high','max_tokens':8192}
  if any(private.get(k)!=v for k,v in expected.items()):raise RuntimeError('Model/sampling configuration differs from development proposal')
  if admission.get('model_configuration_sha256')!=hashlib.sha256(wire(private)).hexdigest():raise RuntimeError('Private model configuration changed')
  if set(initial_messages)!=lease.allowed_paths:raise RuntimeError('Model input identities differ from sealed plan')
  for key,items in initial_messages.items():
   if (type(items) is not list or len(items)!=2 or [m.get('role') for m in items]!=['system','user'] or
    any(set(m)!={'role','content'} or type(m.get('content')) is not str for m in items)):
    raise RuntimeError('Invalid sealed initial messages')
   if (admission.get('initial_messages_sha256') or {}).get(key)!=hashlib.sha256(wire(items)).hexdigest():
    raise RuntimeError('Initial message bytes changed after launch admission')
  self.lease=lease;self.private=private;self.initial=deepcopy(initial_messages);self.replies={}
  self.transport=OneShotHTTP(private) if transport is None else transport

 def generate(self,path_key,phase,outcome=None):
  if path_key not in self.initial or phase not in ('initial','feedback'):raise RuntimeError('Unsealed generation identity')
  initial=self.initial[path_key]
  if phase=='initial':messages=initial
  else:
   first=self.replies.get(path_key+'__initial')
   if first is None or first.get('transport_error') or first.get('finish_reason')!='stop':raise RuntimeError('No eligible terminated initial reply')
   messages=feedback_messages(initial,first['content'],outcome or {})
   if messages is None:raise RuntimeError('No repair for valid/declined/infrastructure/length outcome')
  body={k:self.private[k] for k in ('model','temperature','top_p','reasoning_effort','max_tokens')};body['messages']=messages
  request_id=path_key+'__'+phase
  with self.lease.mutex:
   ack=self.lease.reserve(request_id,body);self.lease.claim_once(request_id)
  started=time.monotonic();started_epoch=time.time()
  try:terminal=self.transport(body)
  except Exception as exc:terminal={'http_status':None,'transport_error':type(exc).__name__}
  # BaseException/crash intentionally leaves a pending reservation; no resend.
  terminal=deepcopy(terminal);raw=terminal.pop('raw_response_bytes',None)
  if raw is not None:
   with Path(ack['request_path']).with_name('response.raw').open('xb') as f:f.write(raw)
  reply=normalized_reply(terminal,self.private['model'])
  terminal.update(budget_slot=ack['slot'],scope_id=SCOPE,physical_attempt_in_request=1,
   automatic_retry=False,provider_wall_seconds=time.monotonic()-started,
   started_epoch=started_epoch,ended_epoch=time.time(),
   prompt_characters=sum(len(m['content']) for m in messages),normalized_reply=reply)
  self.lease.terminal(request_id,terminal,bool(reply['transport_error']))
  reply.update(budget_slot=ack['slot'],terminal_path=str(Path(ack['request_path']).with_name('terminal.json')))
  self.replies[request_id]=reply;return deepcopy(reply)

 def stop_for_unknown_read(self,path_key):
  with self.lease.mutex:
   self.lease.stopped=True
   p=self.lease.directory/'read_infrastructure_stop.json'
   if not p.exists():immutable(p,{'path_key':path_key,'stop_reason':'Unknown data-read infrastructure state; no further dispatch or automatic query retry'})


def outcome(arm,record,schema,execute):
 if record.get('transport_error'):return {'status':'INFRASTRUCTURE_ERROR','complete':False,'rows':None,'columns':None}
 if record['finish_reason']=='length':return {'status':'MODEL_TRUNCATED','complete':False,'rows':None,'columns':None}
 return execute_response(arm,record['content'],schema,execute)


def run_development_path(client,path_key,arm,schema,execute):
 """No evaluator reference/score argument; one operational repair maximum."""
 if arm not in ('direct','ours') or not path_key.endswith('__'+arm):raise ValueError('Method identity mismatch')
 started=time.monotonic();first=client.generate(path_key,'initial');first_result=outcome(arm,first,schema,execute)
 if not first.get('transport_error') and first_result['status']=='INFRASTRUCTURE_ERROR':client.stop_for_unknown_read(path_key)
 result=first_result;final_record=first;records=[first]
 if feedback_messages(client.initial[path_key],first.get('content') or '',first_result) is not None:
  final_record=client.generate(path_key,'feedback',first_result);records.append(final_record)
  result=outcome(arm,final_record,schema,execute)
  if not final_record.get('transport_error') and result['status']=='INFRASTRUCTURE_ERROR':client.stop_for_unknown_read(path_key)
 controls={}
 if arm=='ours' and result['status']=='EXECUTED' and result.get('complete') is True:
  # Same final planner reply, no additional model call. No control-driven repair.
  detailed=execute_response('ours',final_record['content'],schema,execute,strategy='full_detail')
  if detailed['status']=='INFRASTRUCTURE_ERROR':client.stop_for_unknown_read(path_key)
  controls={'full_detail':{'result':detailed,'shared_planner':True,'extra_model_calls':0},
   'simple_correct':{'same_result_as':'ours_final','reason':'Current fixed static safe-count/full-identity rule equals contract policy',
    'shared_planner':True,'extra_model_calls':0,'extra_database_execution':False}}
 saved={'version':VERSION,'path_key':path_key,'arm':arm,'records':records,'first_result':first_result,'final':result,
  'shared_plan_controls':controls,'attached_ledger_attempts':len(records),'formal_prediction':False,
  'evaluator_reference_read_by_this_function':False,'monetary_cost':None,'path_wall_seconds':time.monotonic()-started}
 immutable(client.lease.directory/'paths'/(path_key+'.json'),saved)
 return saved
