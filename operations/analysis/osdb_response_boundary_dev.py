"""Offline response->execution->one-feedback replay. No live model client.

Gold is accepted only by separate scoring, after final answers. This module
cannot send HTTP, reserve a model call or resume a campaign. Live model/ledger,
exposure, normalization and full freeze admission remain separate gates.
"""
from collections import Counter
from copy import deepcopy
import json,math
import observed_neo4j_services_dev_v2 as compiler
from osdb_common_input_v3 import FEEDBACK_POLICY
from osdb_shared_read_executor_dev import read_query

VERSION='osdb-response-boundary-offline-dev-1.1'


class ProtocolError(ValueError):pass


def strict_json(text):
 if type(text) is not str or not text.strip() or len(text)>200000:raise ProtocolError('Expected one bounded JSON object')
 def pairs(items):
  out={}
  for k,v in items:
   if k in out:raise ProtocolError('Duplicate object key')
   out[k]=v
  return out
 def constant(value):raise ProtocolError('Non-finite JSON number')
 def finite_float(value):
  number=float(value)
  if not math.isfinite(number):raise ProtocolError('Non-finite JSON number')
  return number
 try:value=json.loads(text,object_pairs_hook=pairs,parse_constant=constant,parse_float=finite_float)
 except (ValueError,RecursionError) as exc:raise ProtocolError('Invalid strict JSON: '+str(exc)[:300])
 if type(value) is not dict:raise ProtocolError('Expected a JSON object')
 return value


def decode(arm,text):
 if arm not in ('direct','ours'):raise ProtocolError('Unknown arm')
 value=strict_json(text);status=value.get('status')
 if status in ('UNSUPPORTED','INSUFFICIENT_EVIDENCE'):
  if set(value)!={'status','reason'} or type(value['reason']) is not str or not value['reason'].strip() or len(value['reason'])>2000:
   raise ProtocolError('A decline needs status and a bounded nonempty reason only')
  return value
 if status!='ANSWER':raise ProtocolError('Unknown answer status')
 if arm=='direct':
  if set(value)!={'status','query'} or type(value['query']) is not str:raise ProtocolError('Direct ANSWER needs status/query only')
 else:
  if set(value)!={'status','calls','combine'}:raise ProtocolError('Plan ANSWER needs status/calls/combine only')
 return value


def error(status,message):return {'status':status,'complete':False,'rows':None,'columns':None,'operational_error':str(message)[:2000]}


def execute_response(arm,text,schema,execute,*,strategy='contract'):
 try:value=decode(arm,text)
 except ProtocolError as exc:return error('MODEL_PROTOCOL_ERROR',exc)
 if value['status']!='ANSWER':return dict(value,complete=False,rows=None,columns=None)
 try:
  if arm=='direct':result=execute(read_query(value['query']),{})
  else:result=compiler.execute_plan(value,schema,execute,strategy=strategy)
 except compiler.UnsupportedPlan as exc:return error('PLAN_VALIDATION_ERROR',exc)
 except compiler.IncompleteEvidence as exc:return error('TRUNCATED_RESULT',exc)
 except compiler.ServiceExecutionError as exc:return error('EXECUTION_ERROR',exc)
 except ValueError as exc:
  if type(exc).__name__=='ReadOnlyViolation':return error('READ_POLICY_ERROR',exc)
  raise
 except Exception as exc:
  code=getattr(exc,'code','') or ''
  if 'TransactionTimedOut' in code:return error('EXECUTION_TIMEOUT',exc)
  if code.startswith('Neo.ClientError.Statement.') or code.startswith('Neo.ClientError.Schema.'):
   return error('EXECUTION_ERROR',exc)
  if isinstance(exc,(ConnectionError,TimeoutError,OSError)) or type(exc).__name__ in ('ServiceUnavailable','SessionExpired'):
   return error('INFRASTRUCTURE_ERROR',type(exc).__name__)
  # Unexpected adapter bugs stop replay instead of being silently model failures.
  raise
 observations=result.get('observations')
 complete=result.get('complete') if 'complete' in result else bool(observations) and all(o.get('complete') is True for o in observations)
 if result.get('status')=='TRUNCATED' or complete is False:return error('TRUNCATED_RESULT','Complete requested result exceeded output limits; do not add an implicit LIMIT')
 if result.get('status')!='EXECUTED' or complete is not True:raise RuntimeError('Unrecognized execution contract')
 return dict(result,complete=True)


def feedback_messages(initial,previous_text,outcome):
 if outcome['status'] not in FEEDBACK_POLICY['eligible']:return None
 detail=outcome.get('operational_error','Operational validation failed')[:2000]
 return deepcopy(initial)+[{'role':'assistant','content':previous_text},
  {'role':'user','content':'One repair is allowed using only this operational error: '+detail+
   '\nKeep the requested meaning and answer extent; no implicit LIMIT, no invented facts, no reference or expected answer is supplied. Return the same JSON protocol.'}]


def replay_case(arm,initial,schema,response_records,execute,*,strategy='contract'):
 """Consume <=2 PRE-SUPPLIED simulated/archived replies; never call a model."""
 if not isinstance(response_records,list) or not 1<=len(response_records)<=2:raise ValueError('One initial reply and at most one feedback reply')
 history=[];next_messages=None
 for index,record in enumerate(response_records):
  if record.get('transport_error') or record.get('http_status',200)!=200:
   outcome=error('INFRASTRUCTURE_ERROR','Transport/HTTP failure retained; no model repair')
  elif record.get('finish_reason')=='length':outcome=error('MODEL_TRUNCATED','Generation reached the fixed completion limit')
  elif record.get('finish_reason')!='stop':outcome=error('MODEL_PROTOCOL_ERROR','Expected one stopped JSON completion')
  else:outcome=execute_response(arm,record.get('content'),schema,execute,strategy=strategy)
  history.append({'reply_index':index,'record':deepcopy(record),'outcome':outcome})
  next_messages=feedback_messages(initial,record.get('content') or '',outcome) if index==0 else None
  if next_messages is None or len(response_records)==1:break
 return {'version':VERSION,'final':outcome,'history':history,'response_records_consumed':len(history),
  'next_messages_if_feedback_pending':next_messages,'physical_model_calls':0,'is_response_replay':True,
  'formal_admission':False,'shared_plan_strategy':strategy if arm=='ours' else None}


def typed(value):
 if value is None:return ['null']
 if type(value) is bool:return ['bool',value]
 if type(value) is int:return ['int',value]
 if type(value) is float and math.isfinite(value):return ['float',value]
 if type(value) is str:return ['str',value]
 if type(value) is list:return ['list',[typed(v) for v in value]]
 if type(value) is dict:return ['map',[[k,typed(v)] for k,v in sorted(value.items())]]
 raise ValueError('Value lacks an admitted scoring representation')


def score_numeric(final,expected_rows):
 """Exact typed row bags for exposed numeric developer references only.

 Column aliases do not define answer meaning; row widths must still agree.
 No float tolerance, truthy coercion, missing-to-zero, order or decline shortcut.
 """
 if expected_rows is None:return {'numeric_EX':None,'reason':'NO_NUMERIC_REFERENCE_NOT_A_CORRECT_ANSWER'}
 if final.get('status')!='EXECUTED' or final.get('complete') is not True:return {'numeric_EX':False,'reason':'NO_COMPLETE_EXECUTED_ANSWER'}
 actual=final.get('rows');columns=final.get('columns')
 if type(actual) is not list or type(columns) is not list:return {'numeric_EX':False,'reason':'MALFORMED_ANSWER'}
 width=len(columns)
 if any(type(r) is not list or len(r)!=width for r in actual+expected_rows):return {'numeric_EX':False,'reason':'ANSWER_WIDTH_DIFFERS'}
 key=lambda r:json.dumps(typed(r),ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
 return {'numeric_EX':Counter(key(r) for r in actual)==Counter(key(r) for r in expected_rows),
  'reason':'EXACT_TYPED_ROW_BAG','developer_only_not_formal_metric_freeze':True}
