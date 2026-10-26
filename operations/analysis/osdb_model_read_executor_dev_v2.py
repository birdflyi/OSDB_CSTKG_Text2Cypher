"""Model-facing read boundary over the accepted semantic snapshot.

Keep the original executor unchanged. Completed EXPLAIN policy denials and
known rolled-back read statement errors may receive one model repair. Unknown
transport/admission failures still invalidate the executor; no query retry.
"""
from pathlib import Path
from osdb_shared_read_executor_dev import SharedReadExecutor,canonical,normalize,_guard

VERSION='osdb-model-read-boundary-dev-2'
ReadOnlyViolation=_guard.ReadOnlyViolation


def operators(plan):
 if type(plan) is not dict or type(plan.get('operatorType')) is not str:raise ValueError('Missing engine plan operator')
 result=[plan['operatorType']]
 for child in plan.get('children',[]):result.extend(operators(child))
 return result


def data_read_operators(names):
 return bool(names) and all(not n.split('@',1)[0].lower().startswith(('show','administration','system','procedurecall')) for n in names)


def known_rolled_back_read_error(exc):
 code=getattr(exc,'code','') or ''
 return code.startswith('Neo.ClientError.Statement.') or 'TransactionTimedOut' in code


class ModelReadExecutor(SharedReadExecutor):
 def __init__(self,*args,**kwargs):
  super().__init__(*args,**kwargs)
  self._save(self.folder/'model_boundary_policy.json',{'version':VERSION,'server_explain_type_r_and_data_plan_required':True,
   'known_read_statement_errors_do_not_invalidate_snapshot':True,'unknown_or_transport_errors_do_invalidate_snapshot':True,
   'no_automatic_statement_retry':True,'formal_admission':False})

 def _statement(self,query,params,kind,consumer=None,timeout=60):
  was_admitted=self.admitted;was_failed=self.failed
  try:return super()._statement(query,params,kind,consumer,timeout=timeout)
  except Exception as exc:
   if was_admitted and not was_failed and kind in ('explain','read') and known_rolled_back_read_error(exc):
    # The server returned a terminal error for a read-only auto-commit statement.
    # This is not a lock clear or a request replay; the failed terminal stays.
    self.failed=False
    self._save(self.folder/f'{self.number:04d}_{kind}'/'read_error_classification.json',
     {'known_rolled_back_read_error':True,'snapshot_still_admitted':True,'automatic_retry':False})
   raise

 def execute(self,query,params=None):
  if not self.admitted or self.failed:raise RuntimeError('No active admitted semantic snapshot')
  clean=_guard.read_query(query);params={} if params is None else params
  if type(params) is not dict:raise ValueError('Parameter map required')
  def inspect(cursor):return {'operators':operators(cursor.consume().plan)}
  explained,kind=self._statement('EXPLAIN '+clean,params,'explain',inspect,timeout=30)
  if kind!='r' or not data_read_operators(explained['operators']):
   self._save(self.folder/f'{self.number:04d}_explain'/'policy_rejection.json',
    {'query_type':kind,'operators':explained['operators'],'physical_answer_query_dispatched':False})
   raise ReadOnlyViolation('Engine EXPLAIN is not an ordinary data-read plan')
  def collect(cursor):
   columns=list(cursor.keys());rows=[];size=2;complete=True
   for record in cursor:
    row=[normalize(record[c]) for c in columns];addition=len(canonical(row).encode('utf-8'))+(1 if rows else 0)
    if len(rows)>=self.max_rows or size+addition>self.max_bytes:complete=False;break
    rows.append(row);size+=addition
   return {'status':'EXECUTED' if complete else 'TRUNCATED','complete':complete,'columns':columns,'rows':rows,'result_bytes':size}
  result,kind=self._statement(clean,params,'read',collect,timeout=60)
  if kind!='r':self.failed=True;raise RuntimeError('Actual read classification differs; stop and audit')
  return result
