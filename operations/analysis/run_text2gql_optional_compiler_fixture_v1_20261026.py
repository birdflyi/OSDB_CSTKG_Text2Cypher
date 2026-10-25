"""One-shot independent compiler fixture. No TEST task or model configuration read."""
from pathlib import Path
from datetime import datetime
from copy import deepcopy
import base64, hashlib, json, re, subprocess, sys

ROOT = Path('D:/ch7')
SOURCE = ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph/experiment-harness/public_benchmark_v1'
sys.path.insert(0, str(SOURCE))
from tugraph_dialect import compile_rest_selection
from test_tugraph_dialect import SCHEMA, selection

OUT = ROOT/'reports/text2gql_optional_compiler_fixture_v1_20261026'
assert not OUT.exists(), 'Inspect completed or partial job; never blindly replay'
OUT.mkdir()
old = ROOT/'reports/text2gql_native_fixtures_v4_20261026'
oldspec = json.loads((old/'spec.json').read_text(encoding='utf-8'))

from tugraph_schema import schema_from_import_config
SCHEMA=schema_from_import_config(json.loads((old/'import_config.json').read_text(encoding='utf-8')))
cases=[]
def call():
    return {'nodes':[{'id':'p','label':'Person'},{'id':'q','label':'Person'}],
     'edges':[{'id':'r','source':'p','target':'q','label':'LINK','optional':True}],
     'where':[],'returns':[{'field':'p.id','alias':'id'},
                          {'field':'q.id','agg':'count','alias':'degree'}],
     'order_by':[{'column':'id','direction':'ASC'}]}
def case(key,expected,mutate=None):
    c=call()
    if mutate:mutate(c)
    compiled=compile_rest_selection(c,SCHEMA)
    cases.append({'id':key,'selection':c,'query':compiled['script'],
      'parameterized_query':compiled['query'],'parameters':compiled['parameters'],
      'expected_rows':expected})
degrees=[['p1',2],['p2',1],['p3',1],['p4',0]]
case('zero_neighbor_primary_count',degrees)
case('zero_neighbor_node_count',degrees,lambda c:c['returns'][1].update(field='q'))
def entity_group(c):
    c['returns'].insert(1,{'field':'p.name','alias':'name'});c['group_by']=['p.id']
case('same_display_distinct_entity_groups',[['p1','Twin',2],['p2','Twin',1],['p3','\u6d4b\u8bd5',1],['p4','Empty',0]],entity_group)
def value_group(c):
    c['returns'][0]={'field':'p.name','alias':'name'};c['group_by']=['p.name']
    c['order_by']=[{'column':'name','direction':'ASC'}]
case('display_value_group',[['Empty',0],['Twin',3],['\u6d4b\u8bd5',1]],value_group)
def where(field,op,value=None,negate=False):
    pred={'field':field,'op':op}
    if op not in ('is_null','not_null'):pred['value']=value
    if negate:pred['negate']=True
    return lambda c:c.update(where=[pred])
case('global_optional_null_filter',[['p4',0]],where('q.id','is_null'))
case('global_optional_defined_filter',[['p1',2],['p2',1],['p3',1]],where('q.id','not_null'))
case('required_root_filter',[['p1',2]],where('p.id','eq','p1'))
case('nullable_negation_excludes_unknown',[['p2',1]],where('q.name','eq','Twin',True))
def anchor(c):
    c['nodes'].insert(1,{'id':'x','label':'Person'})
    c['edges'].insert(0,{'id':'a','source':'p','target':'x','label':'LINK'})
    c['where']=[{'field':'p.id','op':'eq','value':'p1'},{'field':'a.weight','op':'eq','value':2}]
case('anchor_and_optional_edge_reuse',[['p1',2]],anchor)
def bound(c):
    c['edges'].insert(0,{'id':'a','source':'p','target':'q','label':'LINK'})
    c['where']=[{'field':'p.id','op':'eq','value':'p1'}]
case('both_endpoints_bound_bag',[['p1',4]],bound)
def distinct_bound(c):
    bound(c);c['returns'][1].update(field='q',distinct=True)
case('both_endpoints_bound_distinct',[['p1',1]],distinct_bound)
case('inverse_optional_direction',[['p1',1],['p2',2],['p3',1],['p4',0]],
     lambda c:c['edges'][0].update(source='q',target='p'))
def self_loop(c):
    c['nodes']=c['nodes'][:1];c['edges'][0].update(target='p')
    c['returns'][1]['field']='r.weight'
case('missing_self_loop_preserves_root',[['p1',0],['p2',0],['p3',0],['p4',0]],self_loop)
def distinct_roots(c):
    c['returns']=c['returns'][:1];c['distinct']=True
case('optional_root_set_keeps_empty_entity',[['p1'],['p2'],['p3'],['p4']],distinct_roots)
def payload(c):
    c['returns'][1]={'field':'q.id','alias':'neighbor'}
    c['order_by'].append({'column':'neighbor','direction':'ASC'})
case('optional_payload_bag',[['p1','p2'],['p1','p2'],['p2','p3'],['p3','p1'],['p4',None]],payload)
def empty(c):
    c['where']=[{'field':'p.id','op':'eq','value':'missing'}]
    c['returns']=c['returns'][1:];c['order_by']=[]
case('empty_required_count',[[0]],empty)
case('negated_empty_membership',[['p1',2],['p2',1],['p3',1]],where('q.id','in',[],True))
case('explicit_optional_limit',[['p1',2],['p2',1]],lambda c:c.update(limit=2))

spec = {**oldspec, 'kind': 'INDEPENDENT_NATIVE_OPTIONAL_COMPILER_FIXTURES_V1',
        'time_local': datetime.now().astimezone().isoformat(), 'cases': cases,
        'reuse_fixture_job': 'fixtures_v2', 'model_calls': 0,
        'TEST_queries_used': False, 'native_profile_formally_admitted': False,
        'compiler_source_sha256': {name: hashlib.sha256((SOURCE/name).read_bytes()).hexdigest()
          for name in ('schema_services.py', 'tugraph_dialect.py', 'test_tugraph_dialect.py','tugraph_schema.py')}}
spec['server_config']['port']=18877
worker = (old/'worker_original.py').read_text(encoding='utf-8')
worker = worker.replace("return json.loads(raw),hashlib.sha256(raw).hexdigest()",
    "if endpoint == '/cypher':\n"
    "                folder=job/'responses';folder.mkdir(exist_ok=True)\n"
    "                (folder/(str(result['fixture_queries']).zfill(3)+'.json')).write_bytes(raw)\n"
    "            return json.loads(raw),hashlib.sha256(raw).hexdigest()")
worker = worker.replace("raw=exc.read();item.update", "raw=exc.read();folder=job/'responses';folder.mkdir(exist_ok=True);"
    "(folder/(str(result['fixture_queries']).zfill(3)+'.error')).write_bytes(raw);item.update")
compile(worker, '<independent-native-worker>', 'exec')
files = {'spec.json': (json.dumps(spec, ensure_ascii=False, indent=2)+'\n').encode('utf-8'),
         'worker.py': worker.encode('utf-8')}
for name, raw in files.items():
    (OUT/name).write_bytes(raw)
doc = (ROOT/'reports/text2gql_native_engine_20261026/upstream/docs/en-US/source/7.client-tools/5.bolt-client.md').read_text(encoding='utf-8')
auth = re.search(r'AuthTokens.basic\("([^"\n]+)",\s*"([^"\n]+)"\)', doc)
assert auth
files['.client-private.json'] = json.dumps({'user': auth[1], 'password': auth[2]}).encode()
bundle = {name: base64.b64encode(raw).decode() for name, raw in files.items()}
code = r'''
from pathlib import Path
import base64,json,os,platform,subprocess
root=Path('/mnt/data/louzehua/ch7_t1_public_benchmark_20261023')
owner=json.loads((root/'execution_owner.json').read_text())
assert platform.node()==owner['hostname']=='interactive-xuyl0q80gc01-6d6f5ddb9b-xtpfj'
assert not (root/'.public-runtime.lock').exists()
active=[]
for path in Path('/proc').glob('[0-9]*/cmdline'):
    try:
        args=path.read_bytes().split(b'\0')
        if len(args)>1 and str(root).encode() in b' '.join(args) and any(x in (b'lgraph_server',b'lgraph_import',b'worker.py') for x in [Path(a.decode(errors='ignore')).name.encode() for a in args]):
            active.append(int(path.parent.name))
    except (OSError,ValueError):pass
assert not active,active
job=root/'text2gql_reference_audit_20261026'/'optional_compiler_v1'
assert not job.exists(),'Existing one-shot job must be inspected'
job.mkdir(mode=0o700)
for name,raw in BUNDLE.items():
    target=job/name;assert target.parent==job
    fd=os.open(str(target),os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
    with os.fdopen(fd,'wb') as f:f.write(base64.b64decode(raw))
run=subprocess.run(['python3',str(job/'worker.py'),str(job)],capture_output=True,text=True,timeout=170)
(job/'launch_result.json').write_text(json.dumps({'returncode':run.returncode,'stdout':run.stdout,'stderr':run.stderr[:1000]}))
receipt=json.loads((job/'receipt.json').read_text())
responses={p.name:base64.b64encode(p.read_bytes()).decode() for p in (job/'responses').glob('*')}
print(json.dumps({'returncode':run.returncode,'receipt':receipt,'responses':responses,
                 'worker_stdout':run.stdout,'worker_stderr':run.stderr[:300]},ensure_ascii=True))
'''.replace('BUNDLE', repr(bundle), 1)
run = subprocess.run(['ssh','-i','C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key',
    '-p','2491','-o','BatchMode=yes','-o','ConnectTimeout=15','root@117.50.195.94','python3 -'],
    input=code,capture_output=True,text=True,encoding='utf-8',timeout=190)
(OUT/'client_result.json').write_text(json.dumps({'returncode':run.returncode,
    'stderr':run.stderr[:500],'stdout':run.stdout},indent=2)+'\n',encoding='utf-8')
assert run.returncode == 0, 'Remote result preserved, audit before retry'
data=json.loads(run.stdout)
(OUT/'receipt.json').write_text(json.dumps(data['receipt'],ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
(OUT/'responses').mkdir()
for name, encoded in data['responses'].items():
    assert Path(name).name==name
    (OUT/'responses'/name).write_bytes(base64.b64decode(encoded))
receipt=data['receipt']
for index, item in enumerate(receipt['cases'],1):
    rawpaths=list((OUT/'responses').glob(str(index).zfill(3)+'.*'))
    assert len(rawpaths)==1
    assert hashlib.sha256(rawpaths[0].read_bytes()).hexdigest()==item['response_sha256']
print(json.dumps({'phase':receipt['phase'],'queries':receipt['fixture_queries'],
 'matches':receipt.get('matches_expected'),'denominator':receipt.get('fixed_check_denominator'),
 'failed':[{k:v for k,v in c.items() if k in ('id','status','response','error','matches_expected')}
           for c in receipt['cases'] if not c['matches_expected']],
 'closed':receipt['child_pids_absent']},ensure_ascii=True))
