"""Create a fixed independent fixture specification and upload a fresh CPU job."""
from pathlib import Path
from datetime import datetime
import argparse,base64,csv,hashlib,io,json,re,subprocess

BASE=Path('D:/ch7/reports/text2gql_native_engine_20261026')
parser=argparse.ArgumentParser();parser.add_argument('--job-version',choices=['v1','v2','v3','v4'],default='v1')
parser.add_argument('--profile',choices=['default','legacy'],default='default');args=parser.parse_args()
OUT=Path('D:/ch7/reports/text2gql_native_fixtures_'+args.job_version+'_20261026');OUT.mkdir(exist_ok=True)
cases=[
 {'id':'same_match_relationship_uniqueness','query':"MATCH (a:Person)-[r:LINK]->(b:Person)<-[s:LINK]-(a) WHERE a.id='p1' RETURN count(*) AS c",'expected_rows':[[2]]},
 {'id':'separate_match_relationship_reuse','query':"MATCH (a:Person)-[r:LINK]->(b:Person) MATCH (a)-[s:LINK]->(b) WHERE a.id='p1' RETURN count(*) AS c",'expected_rows':[[4]]},
 {'id':'null_negation','query':"MATCH (p:Person) WHERE NOT p.optional_value='present' RETURN p.id AS id ORDER BY id",'expected_rows':[]},
 {'id':'optional_zero_degree','query':"MATCH (p:Person) OPTIONAL MATCH (p)-[r:LINK]->(:Person) RETURN p.id AS id, count(r) AS c ORDER BY id",'expected_rows':[['p1',2],['p2',1],['p3',1],['p4',0]]},
 {'id':'entity_grouping','query':"MATCH (p:Person) OPTIONAL MATCH (p)-[r:LINK]->(:Person) RETURN p.id AS id,p.name AS name,count(r) AS c ORDER BY id",'expected_rows':[['p1','Twin',2],['p2','Twin',1],['p3','测试',1],['p4','Empty',0]]},
 {'id':'parallel_edge_bag','query':"MATCH (p:Person)-[r:LINK]->(:Person) RETURN p.id AS id ORDER BY id",'expected_rows':[['p1'],['p1'],['p2'],['p3']]},
 {'id':'edge_datetime_filter','query':"MATCH (p:Person)-[r:LINK]->(:Person) WHERE r.occurred>=datetime('2020-01-02T00:00:00') RETURN p.id AS id,r.weight AS weight ORDER BY weight",'expected_rows':[['p1',3],['p2',5],['p3',7]]},
 {'id':'vertex_date_filter','query':"MATCH (p:Person) WHERE p.birthday>=date('2020-01-02') RETURN p.id AS id ORDER BY id",'expected_rows':[['p2'],['p3']]},
]
schema={'schema':[
 {'type':'VERTEX','label':'Person','primary':'id','properties':[
   {'name':'id','type':'STRING','optional':False,'unique':True,'index':True},
   {'name':'name','type':'STRING','optional':True},
   {'name':'optional_value','type':'STRING','optional':True},
   {'name':'birthday','type':'DATE','optional':True}]},
 {'type':'EDGE','label':'LINK','constraints':[['Person','Person']],
  'properties':[{'name':'weight','type':'INT32','optional':True},{'name':'occurred','type':'DATETIME','optional':True}]}],
 'files':[{'path':'Person.csv','label':'Person','format':'CSV','header':1,'columns':['id','name','optional_value','birthday']},
          {'path':'LINK.csv','label':'LINK','format':'CSV','header':1,'SRC_ID':'Person','DST_ID':'Person',
           'columns':['SRC_ID','DST_ID','weight','occurred']}]}
def csv_bytes(rows):
    out=io.StringIO(newline='');csv.writer(out,lineterminator='\n').writerows(rows);return out.getvalue().encode('utf-8')
files={
 'Person.csv':csv_bytes([['id','name','optional_value','birthday'],['p1','Twin','','2020-01-01'],['p2','Twin','present','2020-01-02'],['p3','测试','','2020-01-03'],['p4','Empty','','']]),
 'LINK.csv':csv_bytes([['src','dst','weight','occurred'],['p1','p2',2,'2020-01-01 00:00:00'],['p1','p2',3,'2020-01-02 00:00:00'],['p2','p3',5,'2020-01-03 00:00:00'],['p3','p1',7,'2020-01-04 00:00:00']]),
 'import_config.json':(json.dumps(schema,ensure_ascii=False,indent=2)+'\n').encode('utf-8')}
spec={'kind':'FIXED_INDEPENDENT_ENGINE_SEMANTIC_FIXTURES_NOT_BENCHMARK_TASKS',
 'time_local':datetime.now().astimezone().isoformat(),'cases':cases,
 'server_config':{'host':'127.0.0.1','port':18870,'bolt_port':0,'enable_rpc':False,'enable_ha':False,
  'thread_limit':2,'verbose':1,'log_file_size':16,'max_n_log_files':2,'enable_audit_log':False},
 'source_files':{name:{'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)} for name,raw in files.items()},
 'model_calls':0,'TEST_queries_used':False}
if args.job_version in ['v3','v4']:
    spec['reuse_fixture_job']='fixtures_v2'
    spec['server_config']['is_cypher_v2']=args.profile!='legacy'
    cases.extend([
      {'id':'property_null_state','query':"MATCH (p:Person) RETURN p.id AS id,p.optional_value AS value,p.optional_value IS NULL AS isnull,p.birthday AS birthday,p.birthday IS NULL AS date_isnull ORDER BY id",'expected_rows':None,'diagnostic_only':True},
      {'id':'literal_null_logic','query':"RETURN NULL='present' AS eq,NOT(NULL='present') AS neg",'expected_rows':[[None,None]],'diagnostic_only':True},
      {'id':'datetime_space_constructor','query':"MATCH (p:Person)-[r:LINK]->(:Person) WHERE r.occurred>=datetime('2020-01-02 00:00:00') RETURN p.id AS id,r.weight AS weight ORDER BY weight",'expected_rows':[['p1',3],['p2',5],['p3',7]],'diagnostic_only':True},
      {'id':'unicode_roundtrip','query':"MATCH (p:Person) WHERE p.id='p3' RETURN p.name AS name",'expected_rows':[['测试']],'diagnostic_only':True}])
files['spec.json']=(json.dumps(spec,ensure_ascii=False,indent=2)+'\n').encode('utf-8')
for name,raw in files.items():
    path=OUT/name;assert not path.exists(),'Keep previous fixture version unchanged';path.write_bytes(raw)
# Read a published initial credential from the cached official Bolt documentation.
# It is used only in the NEW private fixture database, never printed or committed.
doc=(BASE/'upstream/docs/en-US/source/7.client-tools/5.bolt-client.md').read_text(encoding='utf-8')
m=re.search(r'AuthTokens.basic\("([^"\n]+)",\s*"([^"\n]+)"\)',doc);assert m
private=json.dumps({'user':m[1],'password':m[2]}).encode()
worker=Path('D:/ch7/reports/text2gql_fixture_worker_20261026.py').read_bytes()
bundle={name:base64.b64encode(raw).decode() for name,raw in files.items()}
bundle['worker.py']=base64.b64encode(worker).decode();bundle['.client-private.json']=base64.b64encode(private).decode()
code=r'''
from pathlib import Path
import json,base64,platform,os,subprocess
root=Path('/mnt/data/louzehua/ch7_t1_public_benchmark_20261023')
assert platform.node()==json.loads((root/'execution_owner.json').read_text())['hostname']
assert not (root/'.public-runtime.lock').exists()
base=root/'text2gql_reference_audit_20261026';base.mkdir(exist_ok=True)
job=base/JOBNAME;assert not job.exists(),'Existing fixture requires audit, not replay'
job.mkdir(mode=0o700)
for name,encoded in BUNDLE.items():
    target=job/name;assert target.parent==job
    fd=os.open(str(target),os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
    with os.fdopen(fd,'wb') as f:f.write(base64.b64decode(encoded))
run=subprocess.run(['python3',str(job/'worker.py'),str(job)],capture_output=True,text=True,timeout=160)
print(json.dumps({'returncode':run.returncode,'stdout':run.stdout,'stderr':run.stderr[:500]}))
'''.replace('BUNDLE',repr(bundle),1).replace('JOBNAME',repr('fixtures_'+args.job_version),1)
run=subprocess.run(['ssh','-i','C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key','-p','2491',
    '-o','BatchMode=yes','-o','ConnectTimeout=15','root@117.50.195.94','python3 -'],
    input=code,capture_output=True,text=True,timeout=180)
result={'ssh_returncode':run.returncode,'stdout':run.stdout,'stderr':run.stderr[:500]}
(OUT/'client_result.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps(result,ensure_ascii=True))
