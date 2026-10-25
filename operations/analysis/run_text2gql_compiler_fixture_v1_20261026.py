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

OUT = ROOT/'reports/text2gql_compiler_fixture_v1_20261026'
assert not OUT.exists(), 'Inspect completed or partial job; never blindly replay'
OUT.mkdir()
old = ROOT/'reports/text2gql_native_fixtures_v4_20261026'
oldspec = json.loads((old/'spec.json').read_text(encoding='utf-8'))
cases = []
def case(key, expected, mutate=None):
    call = selection()
    if mutate:
        mutate(call)
    compiled = compile_rest_selection(call, SCHEMA)
    cases.append({'id': key, 'query': compiled['script'], 'expected_rows': expected,
                  'selection': call, 'parameterized_query': compiled['query'],
                  'parameters': compiled['parameters']})
def where(field, op, value=None, **extra):
    p = {'field': field, 'op': op, **extra}
    if op not in ('is_null', 'not_null') and 'other_field' not in extra:
        p['value'] = value
    return lambda call: call.update(where=[p])
allids = [['p1'], ['p2'], ['p3'], ['p4']]
case('declared_identity', allids)
case('null_negation', [], where('p.optional_value', 'eq', 'present', negate=True))
case('nullable_positive_eq', [['p2']], where('p.optional_value', 'eq', 'present'))
case('nullable_positive_ne', [], where('p.optional_value', 'ne', 'present'))
case('null_predicate_negation', [['p2']], where('p.optional_value', 'is_null', negate=True))
case('null_predicate_positive', [['p1'], ['p3'], ['p4']], where('p.optional_value', 'is_null'))
case('date_boundary', [['p2'], ['p3']], where('p.birthday', 'ge', '2020-01-02'))
case('property_comparison', [['p2']], where('p.optional_value', 'eq', other_field='p.name', negate=True))
case('finite_in_list', [['p1'], ['p3']], where('p.id', 'in', ['p1', 'p3']))
case('negated_empty_list', allids, where('p.id', 'in', [], negate=True))
case('unicode_literal', [['p3']], where('p.name', 'eq', '\u6d4b\u8bd5'))
case('hostile_string_remains_literal', [], where('p.name', 'eq', 'x" RETURN 1 // $p1 \\ \n'))
case('literal_limit', [['p1']], lambda c: c.update(limit=1))
def edges(call):
    call['nodes'].append({'id': 'q', 'label': 'Person'})
    call['edges'] = [{'id': 'r', 'source': 'p', 'target': 'q', 'label': 'LINK'}]
case('parallel_bag', [['p1'], ['p1'], ['p2'], ['p3']], edges)
def summary(agg, field):
    def mutate(call):
        edges(call)
        call['returns'] = [{'field': field, 'agg': agg, 'alias': 'value'}]
        call['order_by'] = []
    return mutate
case('sum_edge_weight', [[17]], summary('sum', 'r.weight'))
case('count_distinct_required_entity', [[3]], lambda c: (
    summary('count', 'p')(c), c['returns'][0].update(distinct=True)))
def unique(call):
    edges(call)
    call['edges'].append({'id': 's', 'source': 'p', 'target': 'q', 'label': 'LINK'})
    call['where'] = [{'field': 'p.id', 'op': 'eq', 'value': 'p1'}]
    call['returns'] = [{'field': '*', 'agg': 'count', 'alias': 'value'}]
    call['order_by'] = []
case('single_match_edge_uniqueness', [[2]], unique)
def group(call):
    edges(call)
    call['returns'] = [{'field': 'p.id', 'alias': 'id'},
                       {'field': 'r.weight', 'agg': 'count', 'alias': 'degree'}]
    call['group_by'] = ['p.id']
case('explicit_entity_group', [['p1', 2], ['p2', 1], ['p3', 1]], group)

spec = {**oldspec, 'kind': 'INDEPENDENT_COMPILER_PROFILE_FIXTURES_V1',
        'time_local': datetime.now().astimezone().isoformat(), 'cases': cases,
        'reuse_fixture_job': 'fixtures_v2', 'model_calls': 0,
        'TEST_queries_used': False, 'native_profile_formally_admitted': False,
        'compiler_source_sha256': {name: hashlib.sha256((SOURCE/name).read_bytes()).hexdigest()
          for name in ('schema_services.py', 'tugraph_dialect.py', 'test_tugraph_dialect.py')}}
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
job=root/'text2gql_reference_audit_20261026'/'compiler_profile_v1'
assert not job.exists(),'Existing one-shot job must be inspected'
job.mkdir(mode=0o700)
for name,raw in BUNDLE.items():
    target=job/name;assert target.parent==job
    fd=os.open(str(target),os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
    with os.fdopen(fd,'wb') as f:f.write(base64.b64decode(raw))
run=subprocess.run(['python3',str(job/'worker.py'),str(job)],capture_output=True,text=True,timeout=170)
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
