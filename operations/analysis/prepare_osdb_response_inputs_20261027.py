"""Prepare equal v3 input bytes for the unchanged 24 EXPOSED developer slots.

No inference, graph queries, numeric references or independent-test claims.
"""
from copy import deepcopy
import json
from pathlib import Path
from osdb_common_input_v3 import common_input,messages,VERSION
from prepare_osdb_shared_inputs_20261027 import read,sha,write,ROOT,CANON,PROFILES,REGISTRY
from accept_osdb_actor_graph_references_20261026 import budget,runtime,now

OUT=ROOT/'reports/osdb_response_input_dev_v3_20261027'
PREVIOUS=ROOT/'reports/osdb_shared_input_dev_v2_20261027'


def main():
 assert not OUT.exists()
 registry_sha=sha(REGISTRY);assert registry_sha=='26f24882852e446ae0d28957d3f60b0edaf5d2ec68e66d275d0159a83ac183b3'
 rows=read(REGISTRY);assert len(rows)==24
 previous={str(p.relative_to(PREVIOUS)):sha(p) for p in PREVIOUS.rglob('*.json')}
 profiles={g:read(PROFILES/(g+'_profile.json')) for g in ('redis','duckdb','iotdb','elasticsearch')}
 before_budget=budget();before_runtime=runtime();assert not before_budget['pending']
 freeze=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
 assert all(sha(CANON/n)==v for n,v in freeze.items())
 OUT.mkdir()
 sources=[Path(__file__),Path(__file__).with_name('osdb_common_input_v3.py')]
 write(OUT/'protocol.json',{'sealed_local':now(),'version':VERSION,'planned_exposed_positions':24,
  'registry_sha256':registry_sha,'previous_v2_hashes':previous,'sources':{p.name:sha(p) for p in sources},
  'profile_sha256':{g:sha(PROFILES/(g+'_profile.json')) for g in profiles},
  'allowed_task_fields':['task_id','project','question'],'no_gold_or_reference_fields_passed_to_builder':True,
  'same_user_bytes_and_outer_status_contract':True,'candidate_policy':'empty both arms, task-independent policy still requires admission',
  'budget_before':before_budget,'runtime_before':before_runtime,'canonical_before':freeze,
  'no_network_or_model_call':True,'formal_admission':False})
 manifest=[]
 for row in rows:
  args={'task_id':row['task_id'],'graph':row['project'],'question':row['question'],'schema':deepcopy(profiles[row['project']])}
  shared=common_input(**args);arms={arm:messages(arm,**args) for arm in ('direct','ours')}
  assert arms['direct']['messages'][1]==arms['ours']['messages'][1]
  assert all(m['common_input_sha256']==shared['sha256'] for m in arms.values())
  assert set(shared['payload'])=={'task_id','graph','question','schema','entity_candidates','database_notes','access_contract','response_contract'}
  assert shared['payload']['entity_candidates']==[]
  assert shared['payload']['response_contract']['statuses']==['ANSWER','UNSUPPORTED','INSUFFICIENT_EVIDENCE']
  write(OUT/'common_inputs'/(row['task_id']+'.json'),shared)
  write(OUT/'method_messages'/(row['task_id']+'.json'),arms)
  manifest.append({'task_id':row['task_id'],'graph':row['project'],'common_sha256':shared['sha256'],
   'shared_characters':len(shared['serialized']),'shared_utf8_bytes':len(shared['serialized'].encode('utf-8')),
   'system_characters':{arm:len(m['messages'][0]['content']) for arm,m in arms.items()},
   'same_user_bytes':True,'formal_admission':False})
 assert len({r['task_id'] for r in manifest})==24
 assert all(sha(PREVIOUS/n)==v for n,v in previous.items()) and sha(REGISTRY)==registry_sha
 assert runtime()==before_runtime and all(sha(CANON/n)==v for n,v in freeze.items())
 after_budget=budget();assert before_budget==after_budget
 write(OUT/'manifest.json',manifest)
 summary={'kind':'EXPOSED_OSDB_RESPONSE_INPUT_PREPARATION_NOT_FORMAL','version':VERSION,
  'planned':24,'prepared':24,'failed':0,'unprepared':0,'model_dispatched':0,'model_completed':0,
  'same_user_bytes_all_positions':True,'same_outer_status_contract':True,'candidate_suggestions_per_arm':0,
  'common_input_character_range':[min(x['shared_characters'] for x in manifest),max(x['shared_characters'] for x in manifest)],
  'common_input_utf8_byte_range':[min(x['shared_utf8_bytes'] for x in manifest),max(x['shared_utf8_bytes'] for x in manifest)],
  'system_characters':manifest[0]['system_characters'],'model_calls':0,'graph_queries':0,'network_requests':0,
  'budget_before':before_budget,'budget_after':after_budget,'previous_v2_inputs_and_registry_unchanged':True,
  'canonical32_and_adapter46_unchanged':True,'formal_N':None,'formal_admission':False,'entire_T1_complete':False,
  'manifest_sha256':sha(OUT/'manifest.json')}
 write(OUT/'summary.json',summary);print(json.dumps(summary,ensure_ascii=True),flush=True)


if __name__=='__main__':main()
