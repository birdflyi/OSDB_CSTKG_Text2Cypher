"""One-shot native identity/composition checks on independent development rows."""
from pathlib import Path
from datetime import datetime
from copy import deepcopy
import ast,base64,hashlib,json,re,subprocess,sys

ROOT=Path('D:/ch7');SOURCE=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph/experiment-harness/public_benchmark_v1'
sys.path.insert(0,str(SOURCE))
from tugraph_schema import schema_from_import_config
from schema_services import execute_plan
OUT=ROOT/'reports/text2gql_identity_fixture_v1_20261026'
assert not OUT.exists(),'Audit prior job instead of replaying'
OUT.mkdir()
old=ROOT/'reports/text2gql_native_fixtures_v4_20261026'
prior=json.loads((old/'spec.json').read_text(encoding='utf-8'))
config=json.loads((old/'import_config.json').read_text(encoding='utf-8'))
schema=schema_from_import_config(config)
def selection(field,op,value,negate=False):
    return {'nodes':[{'id':'p','label':'Person'}],'edges':[],
      'where':[{'field':field,'op':op,'value':value,**({'negate':True} if negate else {})}],
      'returns':[{'field':'p.id','alias':'id'},{'field':'p.name','alias':'name'}]}
def ids(values):return selection('p.id','in',values)
def plan(a,b,op='union',output='count'):
    return {'status':'ANSWER','calls':[a,b],
      'combine':{'op':op,'key':'id','output':output,
                 **({'alias':'n'} if output=='count' else {'columns':['id','name']})}}
cases=[
 {'id':'disjoint_name_summary','plan':plan(selection('p.name','eq','Twin'),selection('p.name','eq','\u6d4b\u8bd5')),'expected_rows':[[3]]},
 {'id':'overlap_union','plan':plan(ids(['p1','p2']),ids(['p2','p3'])),'expected_rows':[[3]]},
 {'id':'overlap_intersection','plan':plan(ids(['p1','p2']),ids(['p2','p3']),'intersection'),'expected_rows':[[1]]},
 {'id':'overlap_difference','plan':plan(ids(['p1','p2']),ids(['p2','p3']),'difference'),'expected_rows':[[1]]},
 {'id':'same_display_two_identities','plan':plan(ids(['p1']),ids(['p2'])),'expected_rows':[[2]]},
 {'id':'full_payload_union','plan':plan(ids(['p1','p2']),ids(['p2','p3']),output='rows'),
   'expected_rows':[['p1','Twin'],['p2','Twin'],['p3','\u6d4b\u8bd5']]},
 {'id':'empty_positive_set','plan':plan(ids([]),ids(['p4'])),'expected_rows':[[1]]},
 {'id':'nullable_negative_not_set_difference','plan':plan(selection('p.optional_value','eq','present',True),ids(['p1','p2'])),'expected_rows':[[2]]},
]
spec={**prior,'kind':'INDEPENDENT_NATIVE_IDENTITY_COMPOSITION_FIXTURES_V1',
 'time_local':datetime.now().astimezone().isoformat(),'schema':schema,'cases':cases,
 'reuse_fixture_job':'fixtures_v2','model_calls':0,'TEST_queries_used':False,
 'compiler_source_sha256':{name:hashlib.sha256((SOURCE/name).read_bytes()).hexdigest()
   for name in ('schema_services.py','tugraph_dialect.py','tugraph_schema.py')}}
spec['server_config']['port']=18874
worker=(ROOT/'reports/text2gql_compiler_fixture_v3_20261026/worker.py').read_text(encoding='utf-8')
worker=worker.replace('def main():','from schema_services import execute_plan\nfrom tugraph_dialect import render_compiler_query\n\ndef main():',1)
begin=worker.index("        for case in spec['cases']:")
end=worker.index("        result['phase']='FIXTURE_CHECKS_FINISHED'",begin)
loop=r'''
        result['database_calls']=[]
        for case in spec['cases']:
            for strategy in ['contract','full_detail']:
                item={'id':case['id'],'strategy':strategy,'expected_rows':case['expected_rows']}
                def execute(query,parameters):
                    result['fixture_queries']+=1
                    physical={'id':result['fixture_queries'],'case':case['id'],'strategy':strategy,
                              'query':query,'parameters':parameters}
                    script=render_compiler_query(query,parameters);physical['script']=script
                    try:
                        body,digest=post('/cypher',{'graph':'fixture_v1','script':script,'timeout':15},token)
                        physical.update({'status':'EXECUTED','response_sha256':digest})
                        columns=[x['name'] for x in body['header']]
                        rows=body['result']
                        assert body['size']==len(rows)
                        result['database_calls'].append(physical);save()
                        return {'columns':columns,'rows':rows,'complete':True,'status':'EXECUTED',
                                'response_sha256':digest}
                    except urllib.error.HTTPError as exc:
                        raw=exc.read();folder=job/'responses';folder.mkdir(exist_ok=True)
                        (folder/(str(result['fixture_queries']).zfill(3)+'.error')).write_bytes(raw)
                        physical.update({'status':'QUERY_ERROR','response_sha256':hashlib.sha256(raw).hexdigest()})
                        result['database_calls'].append(physical);save();raise
                try:
                    outcome=execute_plan(case['plan'],spec['schema'],execute,
                                         backend='tugraph-4.5.2',strategy=strategy)
                    item.update({'status':outcome['status'],'rows':outcome['rows'],
                      'representation':outcome['representation'],'matches_expected':outcome['rows']==case['expected_rows']})
                except Exception as exc:
                    item.update({'status':'PLAN_OR_EXECUTION_ERROR','error_type':type(exc).__name__,
                                 'error':str(exc).replace(auth['password'],'[REDACTED]')[:300],
                                 'matches_expected':False})
                result['cases'].append(item);save()
        result['matches_expected']=sum(x['matches_expected'] for x in result['cases'])
        result['fixed_check_denominator']=len(spec['cases'])*2
'''
worker=worker[:begin]+loop+worker[end:]
ast.parse(worker)
files={'spec.json':(json.dumps(spec,ensure_ascii=False,indent=2)+'\n').encode('utf-8'),
       'worker.py':worker.encode('utf-8')}
for name in ('schema_services.py','tugraph_dialect.py','tugraph_schema.py'):
    files[name]=(SOURCE/name).read_bytes()
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
for path in Path('/proc').glob('[0-9]*/cmdline'):
 try:
  args=path.read_bytes().split(b'\0')
  if str(root).encode() in b' '.join(args):
   assert not any(Path(a.decode(errors='ignore')).name in ('lgraph_server','lgraph_import','worker.py') for a in args),'Existing native process'
 except OSError:pass
job=root/'text2gql_reference_audit_20261026'/'identity_composition_v1'
assert not job.exists(),'Do not repeat previous job'
job.mkdir(mode=0o700)
for name,encoded in BUNDLE.items():
 target=job/name;assert target.parent==job
 fd=os.open(str(target),os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
 with os.fdopen(fd,'wb') as f:f.write(base64.b64decode(encoded))
run=subprocess.run(['python3',str(job/'worker.py'),str(job)],capture_output=True,text=True,timeout=170)
(job/'launch_result.json').write_text(json.dumps({'returncode':run.returncode,'stdout':run.stdout,'stderr':run.stderr[:1000]}))
receipt=json.loads((job/'receipt.json').read_text())
responses={p.name:base64.b64encode(p.read_bytes()).decode() for p in (job/'responses').glob('*')}
print(json.dumps({'returncode':run.returncode,'receipt':receipt,'responses':responses},ensure_ascii=True))
'''.replace('BUNDLE',repr(bundle),1)
run=subprocess.run(['ssh','-i','C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key',
 '-p','2491','-o','BatchMode=yes','-o','ConnectTimeout=15','root@117.50.195.94','python3 -'],
 input=code,capture_output=True,text=True,encoding='utf-8',timeout=190)
(OUT/'client_result.json').write_text(json.dumps({'returncode':run.returncode,
 'stderr':run.stderr[:500],'stdout':run.stdout},indent=2)+'\n',encoding='utf-8')
assert run.returncode==0,'Audit preserved partial job before any restart'
data=json.loads(run.stdout);r=data['receipt']
(OUT/'receipt.json').write_text(json.dumps(r,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
(OUT/'responses').mkdir()
for name,encoded in data['responses'].items():
 assert Path(name).name==name
 (OUT/'responses'/name).write_bytes(base64.b64decode(encoded))
assert r['phase']=='FIXTURE_CHECKS_FINISHED' and all(r['child_pids_absent'].values())
assert r['fixture_queries']==len(r['database_calls'])
for physical in r['database_calls']:
 paths=list((OUT/'responses').glob(str(physical['id']).zfill(3)+'.*'))
 assert len(paths)==1 and hashlib.sha256(paths[0].read_bytes()).hexdigest()==physical['response_sha256']
print(json.dumps({'checks':r['fixed_check_denominator'],'matched':r['matches_expected'],
 'native_queries':r['fixture_queries'],'failed':[x for x in r['cases'] if not x['matches_expected']],
 'children_closed':r['child_pids_absent']},ensure_ascii=True))
