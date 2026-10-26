"""Keep all 24 exposed positions; materialize equal user bytes, not predictions."""
from copy import deepcopy
import hashlib,json
from pathlib import Path
from osdb_common_input_v2 import common_input,messages,VERSION
from accept_osdb_actor_graph_references_20261026 import budget,runtime,now

ROOT=Path('D:/ch7');REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
CANON=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
OUT=ROOT/'reports/osdb_shared_input_dev_v2_20261027'
ORIGINAL=ROOT/'reports/osdb_common_input_dev_v1_20261026'
PROFILES=ROOT/'reports/osdb_observed_profile_dev_v2_20261027'
REGISTRY=ROOT/'reports/osdb_actor_candidate_intake_v1_20261026/candidate_registry.json'


def read(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def write(p,x):
 p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('x',encoding='utf-8',newline='\n') as f:json.dump(x,f,ensure_ascii=False,indent=2);f.write('\n')


def main():
 assert not OUT.exists();registry_sha=sha(REGISTRY)
 assert registry_sha=='26f24882852e446ae0d28957d3f60b0edaf5d2ec68e66d275d0159a83ac183b3'
 rows=read(REGISTRY);assert len(rows)==24
 old={str(p.relative_to(ORIGINAL)):sha(p) for p in ORIGINAL.rglob('*.json')}
 profiles={g:read(PROFILES/(g+'_profile.json')) for g in ('redis','duckdb','iotdb','elasticsearch')}
 before_budget=budget();before_runtime=runtime();assert not before_budget['pending']
 freeze=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
 assert all(sha(CANON/n)==v for n,v in freeze.items())
 OUT.mkdir();sources=[Path(__file__),Path(__file__).with_name('osdb_common_input_v2.py')]
 write(OUT/'protocol.json',{'sealed_local':now(),'version':VERSION,'planned_exposed_positions':24,
  'registry_sha256':registry_sha,'original_input_and_schema_hashes':old,
  'sources':{p.name:sha(p) for p in sources},'profile_sha256':{g:sha(PROFILES/(g+'_profile.json')) for g in profiles},
  'allowed_task_fields':['task_id','project','question'],'no_gold_or_reference_fields_passed_to_builder':True,
  'same_user_bytes_not_same_method_system_prompt':True,'candidates_empty_both_arms':True,
  'budget_before':before_budget,'runtime_before':before_runtime,'canonical_before':freeze,
  'no_network_or_model_call':True,'formal_admission':False})
 for p in sources:write(OUT/'source_hashes'/(p.name+'.json'),{'sha256':sha(p)})
 manifest=[]
 for row in rows:
  args={'task_id':row['task_id'],'graph':row['project'],'question':row['question'],'schema':deepcopy(profiles[row['project']])}
  common=common_input(**args);direct=messages('direct',**args);ours=messages('ours',**args)
  assert direct['messages'][1]==ours['messages'][1] and direct['common_input_sha256']==ours['common_input_sha256']==common['sha256']
  assert set(common['payload'])=={'task_id','graph','question','schema','entity_candidates','database_notes','access_contract'}
  assert common['payload']['entity_candidates']==[]
  for declaration in common['payload']['schema']['entities']+common['payload']['schema']['relations']:
   assert set(declaration)<= {'label','identity_property','properties','temporal_properties','subj_label','obj_label'}
  write(OUT/'common_inputs'/(row['task_id']+'.json'),common)
  write(OUT/'method_messages'/(row['task_id']+'.json'),{'direct':direct,'ours':ours})
  manifest.append({'task_id':row['task_id'],'graph':row['project'],'shared_sha256':common['sha256'],
   'shared_characters':len(common['serialized']),'shared_utf8_bytes':len(common['serialized'].encode('utf-8')),
   'system_characters':{arm:len(m['messages'][0]['content']) for arm,m in [('direct',direct),('ours',ours)]},
   'same_user_bytes':True,'formal_admission':False})
 assert len({r['task_id'] for r in manifest})==24
 assert all(sha(ORIGINAL/n)==v for n,v in old.items()) and sha(REGISTRY)==registry_sha
 assert runtime()==before_runtime and all(sha(CANON/n)==v for n,v in freeze.items())
 after_budget=budget();assert before_budget==after_budget
 write(OUT/'manifest.json',manifest)
 summary={'kind':'EXPOSED_OSDB_SHARED_INPUT_PREPARATION_NOT_FORMAL','version':VERSION,
  'planned':24,'prepared':len(manifest),'failed':0,'unprepared':0,'actual_model_dispatched':0,
  'same_user_bytes_all_positions':True,'candidate_suggestions_per_arm':0,
  'profiles':{g:{'entities':len(s['entities']),'relationship_signatures':len(s['relations'])} for g,s in profiles.items()},
  'common_input_character_range':[min(x['shared_characters'] for x in manifest),max(x['shared_characters'] for x in manifest)],
  'common_input_utf8_byte_range':[min(x['shared_utf8_bytes'] for x in manifest),max(x['shared_utf8_bytes'] for x in manifest)],
  'system_characters':manifest[0]['system_characters'],'model_calls':0,'graph_queries':0,'network_requests':0,
  'budget_before':before_budget,'budget_after':after_budget,'old_common_inputs_and_source_schemas_unchanged':True,
  'canonical32_and_adapter46_unchanged':True,'real_four_graphs_admitted':0,
  'formal_N':None,'formal_admission':False,'entire_T1_complete':False,'manifest_sha256':sha(OUT/'manifest.json')}
 write(OUT/'summary.json',summary);print(json.dumps(summary,ensure_ascii=True),flush=True)


if __name__=='__main__':main()
