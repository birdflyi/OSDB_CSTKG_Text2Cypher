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

OUT = ROOT/'reports/text2gql_optional_with_probe_v1_20261026'
assert not OUT.exists(), 'Inspect completed or partial job; never blindly replay'
OUT.mkdir()
old = ROOT/'reports/text2gql_native_fixtures_v4_20261026'
oldspec = json.loads((old/'spec.json').read_text(encoding='utf-8'))
cases=[]
variants=[
 ('with_explicit_optional_zero', 'MATCH (p:Person) WITH p OPTIONAL MATCH (p)-[r:LINK]->(q:Person) RETURN p.id AS id,count(r) AS degree ORDER BY id', [['p1',2],['p2',1],['p3',1],['p4',0]]),
 ('with_star_optional_zero', 'MATCH (p:Person) WITH * OPTIONAL MATCH (p)-[r:LINK]->(q:Person) RETURN p.id AS id,count(r) AS degree ORDER BY id', [['p1',2],['p2',1],['p3',1],['p4',0]]),
 ('with_entity_grouping', 'MATCH (p:Person) WITH p OPTIONAL MATCH (p)-[r:LINK]->(q:Person) RETURN p.id AS id,p.name AS name,count(r) AS degree ORDER BY id', [['p1','Twin',2],['p2','Twin',1],['p3','\u6d4b\u8bd5',1],['p4','Empty',0]]),
 ('post_optional_global_null_filter', 'MATCH (p:Person) WITH p OPTIONAL MATCH (p)-[r:LINK]->(q:Person) WITH * WHERE q.id IS NULL RETURN p.id AS id ORDER BY id', [['p4']]),
 ('anchor_filter_then_optional_reuses_edge', "MATCH (p:Person)-[a:LINK]->(x:Person) WHERE p.id='p1' AND a.weight=2 WITH p OPTIONAL MATCH (p)-[r:LINK]->(q:Person) RETURN p.id AS id,count(r) AS degree ORDER BY id", [['p1',2]]),
 ('both_endpoints_already_bound', "MATCH (p:Person)-[a:LINK]->(q:Person) WHERE p.id='p1' WITH p,q,a OPTIONAL MATCH (p)-[r:LINK]->(q) RETURN p.id AS id,count(r) AS degree ORDER BY id", [['p1',4]])
]
for key,query,expected in variants:
    cases.append({'id':key,'query':query,'expected_rows':expected,'diagnostic_only':True})

spec = {**oldspec, 'kind': 'INDEPENDENT_OPTIONAL_QUERY_PART_PROBES_V1',
        'time_local': datetime.now().astimezone().isoformat(), 'cases': cases,
        'reuse_fixture_job': 'fixtures_v2', 'model_calls': 0,
        'TEST_queries_used': False, 'native_profile_formally_admitted': False,
        'compiler_source_sha256': {name: hashlib.sha256((SOURCE/name).read_bytes()).hexdigest()
          for name in ('schema_services.py', 'tugraph_dialect.py', 'test_tugraph_dialect.py')}}
spec['server_config']['port']=18876
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
job=root/'text2gql_reference_audit_20261026'/'optional_with_probe_v1'
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
