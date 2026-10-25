"""Fresh independent native BOOL/DATETIME fixture; no benchmark tasks or model."""
from pathlib import Path
from datetime import datetime
from copy import deepcopy
import ast,base64,csv,hashlib,io,json,re,subprocess,sys

ROOT=Path('D:/ch7');SOURCE=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph/experiment-harness/public_benchmark_v1'
sys.path.insert(0,str(SOURCE))
from tugraph_schema import schema_from_import_config
from tugraph_dialect import compile_rest_selection
OUT=ROOT/'reports/text2gql_temporal_fixture_v1_20261026'
assert not OUT.exists(),'Do not replay an existing fixture'
OUT.mkdir()
old=ROOT/'reports/text2gql_native_fixtures_v4_20261026'
config=json.loads((old/'import_config.json').read_text(encoding='utf-8'))
config['schema'][0]['properties'] += [
 {'name':'active','type':'BOOL','optional':True},
 {'name':'created','type':'DATETIME','optional':True}]
config['files'][0]['columns'] += ['active','created']
schema=schema_from_import_config(config)
def csv_bytes(rows):
    stream=io.StringIO(newline='');csv.writer(stream,lineterminator='\n').writerows(rows)
    return stream.getvalue().encode('utf-8')
files={'Person.csv':csv_bytes([
 ['id','name','optional_value','birthday','active','created'],
 ['p1','Twin','','2020-01-01','true','2020-01-01 00:00:00'],
 ['p2','Twin','present','2020-01-02','false','2020-01-02 00:00:00.123456'],
 ['p3','\u6d4b\u8bd5','','2020-01-03','','2020-01-03 00:00:00'],
 ['p4','Empty','','','true','']]),
 'LINK.csv':(old/'LINK.csv').read_bytes(),
 'import_config.json':(json.dumps(config,ensure_ascii=False,indent=2)+'\n').encode('utf-8')}
cases=[]
def selection():
    return {'nodes':[{'id':'p','label':'Person'}],'edges':[],'where':[],
            'returns':[{'field':'p.id','alias':'id'}],
            'order_by':[{'column':'id','direction':'ASC'}]}
def case(key,expected,field=None,op='eq',value=None,negate=False,other=None,mutate=None):
    c=selection()
    if field:
        p={'field':field,'op':op}
        if other:p['other_field']=other
        elif op not in ('is_null','not_null'):p['value']=value
        if negate:p['negate']=True
        c['where']=[p]
    if mutate:mutate(c)
    compiled=compile_rest_selection(c,schema)
    cases.append({'id':key,'selection':c,'query':compiled['script'],
      'parameterized_query':compiled['query'],'parameters':compiled['parameters'],'expected_rows':expected})
case('bool_true',[['p1'],['p4']],'p.active',value=True)
case('bool_false',[['p2']],'p.active',value=False)
case('bool_negated_true',[['p2']],'p.active',value=True,negate=True)
case('bool_not_equal_true',[['p2']],'p.active',op='ne',value=True)
case('bool_membership',[['p1'],['p2'],['p4']],'p.active',op='in',value=[True,False])
case('bool_null',[['p3']],'p.active',op='is_null')
case('bool_defined',[['p1'],['p2'],['p4']],'p.active',op='not_null')
case('bool_defined_via_not_null_predicate',[['p1'],['p2'],['p4']],'p.active',op='is_null',negate=True)
case('bool_property_comparison',[['p1'],['p2'],['p4']],'p.active',other='p.active')
case('datetime_inclusive_microsecond',[['p2'],['p3']],'p.created',op='ge',value='2020-01-02T00:00:00.123456')
case('datetime_exclusive_microsecond',[['p3']],'p.created',op='gt',value='2020-01-02T00:00:00.123456')
case('datetime_one_microsecond_below',[['p1']],'p.created',op='le',value='2020-01-02T00:00:00.123455')
case('datetime_exact',[['p2']],'p.created',value='2020-01-02T00:00:00.123456')
case('datetime_neq_excludes_null',[['p1'],['p3']],'p.created',op='ne',value='2020-01-02T00:00:00.123456')
case('datetime_negation',[['p1']],'p.created',op='ge',value='2020-01-02T00:00:00.123456',negate=True)
case('datetime_fraction_padding',[['p2'],['p3']],'p.created',op='ge',value='2020-01-02T00:00:00.1')
case('datetime_property_comparison',[['p1'],['p2'],['p3']],'p.created',other='p.created')
def combined(c):
    c['where']=[{'field':'p.birthday','op':'ge','value':'2020-01-02'},
                {'field':'p.created','op':'lt','value':'2020-01-03T00:00:00'}]
case('date_and_datetime_binding',[['p2']],mutate=combined)
def edge(c):
    c['nodes'].append({'id':'q','label':'Person'})
    c['edges']=[{'id':'r','source':'p','target':'q','label':'LINK'}]
case('edge_datetime_boundary',[['p2'],['p3']], 'r.occurred',op='ge',value='2020-01-02T00:00:00.000001',mutate=edge)
def project_bool(c):c['returns'].append({'field':'p.active','alias':'active'})
case('bool_json_types',[['p1',True],['p2',False],['p3',None],['p4',True]],mutate=project_bool)
cases += [
 {'id':'temporal_rest_types','query':'MATCH (p:Person) RETURN p.id AS id,p.birthday AS birthday,p.created AS created ORDER BY id','expected_rows':None,'diagnostic_only':True},
 {'id':'optional_pattern_comprehension','query':'MATCH (p:Person) RETURN p.id AS id,size([(p)-[r:LINK]->(q:Person) | r]) AS degree ORDER BY id',
  'expected_rows':[['p1',2],['p2',1],['p3',1],['p4',0]],'diagnostic_only':True}]
spec={'kind':'INDEPENDENT_NATIVE_TEMPORAL_BOOL_FIXTURES_V1',
 'time_local':datetime.now().astimezone().isoformat(),'cases':cases,'schema':schema,
 'server_config':{'host':'127.0.0.1','port':18875,'bolt_port':0,'enable_rpc':False,'enable_ha':False,
 'thread_limit':2,'verbose':1,'log_file_size':16,'max_n_log_files':2,'enable_audit_log':False,'is_cypher_v2':True},
 'source_files':{name:{'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)} for name,raw in files.items()},
 'compiler_source_sha256':{name:hashlib.sha256((SOURCE/name).read_bytes()).hexdigest()
 for name in ('schema_services.py','tugraph_dialect.py','tugraph_schema.py')},
 'model_calls':0,'TEST_queries_used':False,'typed_expected_comparison':True}
worker=(ROOT/'reports/text2gql_compiler_fixture_v3_20261026/worker.py').read_text(encoding='utf-8')
worker=worker.replace("body.get('result')==case['expected_rows']", "json.dumps(body.get('result'),sort_keys=True,ensure_ascii=False)==json.dumps(case['expected_rows'],sort_keys=True,ensure_ascii=False)")
worker=worker.replace("result['phase']='IMPORTING_FIXTURE';save()", "result['phase']='IMPORTING_FIXTURE';save()\n        for filename,expected in spec['source_files'].items():\n            assert hashlib.sha256((job/filename).read_bytes()).hexdigest()==expected['sha256']")
ast.parse(worker)
files.update({'spec.json':(json.dumps(spec,ensure_ascii=False,indent=2)+'\n').encode('utf-8'),'worker.py':worker.encode('utf-8')})
for name,raw in files.items():(OUT/name).write_bytes(raw)
doc=(ROOT/'reports/text2gql_native_engine_20261026/upstream/docs/en-US/source/7.client-tools/5.bolt-client.md').read_text(encoding='utf-8')
auth=re.search(r'AuthTokens.basic\("([^"\n]+)",\s*"([^"\n]+)"\)',doc);assert auth
files['.client-private.json']=json.dumps({'user':auth[1],'password':auth[2]}).encode()
bundle={name:base64.b64encode(raw).decode() for name,raw in files.items()}
code=r'''
from pathlib import Path
import base64,json,os,platform,subprocess
root=Path('/mnt/data/louzehua/ch7_t1_public_benchmark_20261023')
assert platform.node()==json.loads((root/'execution_owner.json').read_text())['hostname']=='interactive-xuyl0q80gc01-6d6f5ddb9b-xtpfj'
assert not (root/'.public-runtime.lock').exists()
for p in Path('/proc').glob('[0-9]*/cmdline'):
 try:
  args=p.read_bytes().split(b'\0')
  if str(root).encode() in b' '.join(args):
   assert not any(Path(a.decode(errors='ignore')).name in ('worker.py','lgraph_server','lgraph_import') for a in args),'Active native worker'
 except OSError:pass
job=root/'text2gql_reference_audit_20261026'/'temporal_bool_v1'
assert not job.exists(),'Audit prior job, do not replay'
job.mkdir(mode=0o700)
for name,encoded in BUNDLE.items():
 p=job/name;assert p.parent==job
 fd=os.open(str(p),os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
 with os.fdopen(fd,'wb') as f:f.write(base64.b64decode(encoded))
run=subprocess.run(['python3',str(job/'worker.py'),str(job)],capture_output=True,text=True,timeout=170)
(job/'launch_result.json').write_text(json.dumps({'returncode':run.returncode,'stdout':run.stdout,'stderr':run.stderr[:1000]}))
receipt=json.loads((job/'receipt.json').read_text())
responses={p.name:base64.b64encode(p.read_bytes()).decode() for p in (job/'responses').glob('*')}
print(json.dumps({'returncode':run.returncode,'receipt':receipt,'responses':responses},ensure_ascii=True))
'''.replace('BUNDLE',repr(bundle),1)
run=subprocess.run(['ssh','-i','C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key',
 '-p','2491','-o','BatchMode=yes','-o','ConnectTimeout=15','root@117.50.195.94','python3 -'],
 input=code,text=True,encoding='utf-8',capture_output=True,timeout=190)
(OUT/'client_result.json').write_text(json.dumps({'returncode':run.returncode,'stderr':run.stderr[:500],
 'stdout':run.stdout},indent=2)+'\n',encoding='utf-8')
assert run.returncode==0,'Partial job preserved, audit before continuing'
data=json.loads(run.stdout);r=data['receipt']
(OUT/'receipt.json').write_text(json.dumps(r,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
(OUT/'responses').mkdir()
for name,encoded in data['responses'].items():
 assert Path(name).name==name
 (OUT/'responses'/name).write_bytes(base64.b64decode(encoded))
for index,item in enumerate(r['cases'],1):
 p=list((OUT/'responses').glob(str(index).zfill(3)+'.*'))
 assert len(p)==1 and hashlib.sha256(p[0].read_bytes()).hexdigest()==item['response_sha256']
print(json.dumps({'phase':r['phase'],'queries':r['fixture_queries'],
 'fixed_matches':r.get('matches_expected'),'fixed_denominator':r.get('fixed_check_denominator'),
 'failed':[x for x in r['cases'] if not x.get('diagnostic_only') and not x['matches_expected']],
 'diagnostics':[x for x in r['cases'] if x.get('diagnostic_only')],
 'children_closed':r['child_pids_absent']},ensure_ascii=True))
