"""Shared client access for a physically semantic-only, owned snapshot.

The same instance serves native Direct reads and compiled service reads. Not a
network security boundary or formal admission: snapshot isolation/ownership and
the complete task/model/scoring protocol still need independent acceptance.
"""
from collections.abc import Mapping
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import time

from osdb_common_input import observed_type

_path=Path(__file__).resolve().parents[2]/'experiment-harness/public_benchmark_v1/public_read_guard.py'
_spec=importlib.util.spec_from_file_location('_osdb_existing_public_guard',_path)
_guard=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(_guard)
read_query=_guard.read_query
VERSION='osdb-shared-semantic-read-executor-dev-1'


class AdmissionError(ValueError):pass


def canonical(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
def digest(value):return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


def admitted_kind(actual,expected):
 if not expected.startswith('mixed['):return actual==expected
 depth=0;start=0;parts=[];body=expected[6:-1]
 for i,char in enumerate(body):
  if char=='[':depth+=1
  elif char==']':depth-=1
  elif char=='|' and depth==0:parts.append(body[start:i]);start=i+1
 parts.append(body[start:])
 return actual in parts


def normalize(value):
 if value is None or type(value) in (str,int,bool):return value
 if type(value) is float and math.isfinite(value):return value
 if isinstance(value,Mapping):return {str(k):normalize(v) for k,v in value.items()}
 if isinstance(value,(list,tuple)):return [normalize(v) for v in value]
 if hasattr(value,'iso_format'):return value.iso_format()
 raise TypeError('Result type needs a separately admitted normalization contract')


class SharedReadExecutor:
 def __init__(self,driver,schema,evidence,*,database='neo4j',max_rows=1000000,max_bytes=64*1024*1024):
  self.driver=driver;self.schema=json.loads(canonical(schema));self.database=database
  self.folder=Path(evidence);self.folder.mkdir(parents=True,exist_ok=False)
  self.max_rows=max_rows;self.max_bytes=max_bytes;self.number=0;self.admitted=False;self.failed=False
  if type(max_rows) is not int or not 0<max_rows<=1000000:raise ValueError('Invalid row cap')
  if type(max_bytes) is not int or not 2<=max_bytes<=64*1024*1024:raise ValueError('Invalid byte cap')
  self._save(self.folder/'policy.json',{'version':VERSION,'schema_sha256':digest(schema),
   'database':database,'max_rows':max_rows,'max_bytes':max_bytes,'read_guard_sha256':hashlib.sha256(_path.read_bytes()).hexdigest(),
   'no_automatic_statement_retry':True,'physical_semantic_snapshot_required':True})

 def _save(self,path,value):
  with path.open('x',encoding='utf-8',newline='\n') as f:f.write(canonical(value)+'\n')

 def _statement(self,query,params,kind,consumer=None,timeout=60):
  from neo4j import Query
  self.number+=1;folder=self.folder/f'{self.number:04d}_{kind}';folder.mkdir()
  self._save(folder/'request.json',{'query':query,'parameters':params,'kind':kind,'timeout':timeout})
  started=time.perf_counter();record={'complete':False};result=None
  try:
   # Session.run is a single auto-commit statement; no execute_read retry loop.
   with self.driver.session(database=self.database,default_access_mode='READ') as session:
    cursor=session.run(Query(query,timeout=timeout),params)
    result=consumer(cursor) if consumer else None
    summary=cursor.consume()
    record.update(complete=True,query_type=summary.query_type,result=result)
   return result,summary.query_type
  except Exception as exc:
   self.failed=True;record.update(error_type=type(exc).__name__,error=str(exc)[:1200]);raise
  finally:
   record['client_seconds']=time.perf_counter()-started;self._save(folder/'terminal.json',record)

 def admit(self):
  if self.admitted or self.failed:raise AdmissionError('Admission is one-shot; audit a failed snapshot before a new executor')
  entities={e['label']:e for e in self.schema['entities']}
  relations={(r['label'],r['subj_label'],r['obj_label']):r for r in self.schema['relations']}
  if len(entities)!=len(self.schema['entities']) or len(relations)!=len(self.schema['relations']):raise AdmissionError('Duplicate schema')
  nodes={};entity_keys=set();edge_keys=set();row_hashes=[]
  def properties(props,declaration):
   if not isinstance(props,dict) or props.keys()-declaration['properties'].keys():
    raise AdmissionError('Undeclared physical property remains; do not merely hide metadata')
   for key,value in props.items():
    actual=observed_type(value);expected=declaration['properties'][key]
    if not admitted_kind(actual,expected):raise AdmissionError('Physical property type differs from declared schema')
   key=declaration['identity_property'];identity=props.get(key)
   if declaration['properties'].get(key) not in ('str','int'):raise AdmissionError('Declared scalar identity required')
   if declaration['properties'][key]=='str' and (type(identity) is not str or not identity):raise AdmissionError('Missing string identity')
   if declaration['properties'][key]=='int' and type(identity) is not int:raise AdmissionError('Invalid integer identity')
   return identity
  def scan_nodes(cursor):
   for row in cursor:
    labels=row['labels'];props=row['props']
    if len(labels)!=1 or labels[0] not in entities:raise AdmissionError('Unknown/overlapping physical label; semantic-only materialization required')
    label=labels[0];identity=properties(props,entities[label]);key=(label,identity)
    if key in entity_keys:raise AdmissionError('Duplicate typed entity identity')
    entity_keys.add(key);nodes[row['storage_id']]=key;row_hashes.append(digest(['node',label,props]))
   return {'nodes':len(nodes)}
  self._statement('MATCH (n) RETURN elementId(n) AS storage_id, labels(n) AS labels, properties(n) AS props',{},'admit_nodes',scan_nodes,timeout=240)
  def scan_edges(cursor):
   for row in cursor:
    a=nodes[row['a']];b=nodes[row['b']];signature=(row['label'],a[0],b[0])
    if signature not in relations:raise AdmissionError('Unknown physical relationship signature')
    identity=properties(row['props'],relations[signature]);key=(signature,identity)
    if key in edge_keys:raise AdmissionError('Duplicate typed physical relationship identity')
    edge_keys.add(key);row_hashes.append(digest(['edge',row['label'],a,b,row['props']]))
   return {'edges':len(edge_keys)}
  self._statement('MATCH(a)-[r]->(b) RETURN elementId(a) AS a, type(r) AS label, elementId(b) AS b, properties(r) AS props',{},'admit_edges',scan_edges,timeout=240)
  snapshot=hashlib.sha256(''.join(sorted(row_hashes)).encode()).hexdigest()
  self.receipt={'nodes':len(nodes),'edges':len(edge_keys),'semantic_snapshot_sha256':snapshot,
   'schema_sha256':digest(self.schema),'all_physical_labels_properties_types_and_signatures_admitted':True,
   'formal_admission':False,'requires_exclusive_snapshot_owner':True}
  self._save(self.folder/'semantic_snapshot_receipt.json',self.receipt);self.admitted=True
  return self.receipt

 def execute(self,query,params=None):
  if not self.admitted or self.failed:raise AdmissionError('No active admitted semantic snapshot')
  clean=read_query(query);params={} if params is None else params
  if type(params) is not dict:raise ValueError('Parameter map required')
  _,kind=self._statement('EXPLAIN '+clean,params,'explain',timeout=30)
  if kind!='r':self.failed=True;raise AdmissionError('Database EXPLAIN did not certify read-only')
  def collect(cursor):
   columns=list(cursor.keys());rows=[];size=2;complete=True
   for record in cursor:
    row=[normalize(record[c]) for c in columns];addition=len(canonical(row).encode('utf-8'))+(1 if rows else 0)
    if len(rows)>=self.max_rows or size+addition>self.max_bytes:complete=False;break
    rows.append(row);size+=addition
   return {'status':'EXECUTED' if complete else 'TRUNCATED','complete':complete,
    'columns':columns,'rows':rows,'result_bytes':size}
  result,kind=self._statement(clean,params,'read',collect,timeout=60)
  if kind!='r':self.failed=True;raise AdmissionError('Execution summary differs from read contract')
  return result
