"""Six explicit source-motivated candidates, not a confirmation/test admission.

Preserve duplicates, unsupported requirements and all four historical projects.
Build independent CSV/set and SQLite references plus unexecuted manual Cypher.
No Ours compiler, model, graph DB, SSH, prediction output or score is accessed.
"""
from collections import Counter
from datetime import datetime,timezone,timedelta
import csv
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

import requests
from bs4 import BeautifulSoup
from osdb_actor_reference import KINDS,python_answers,sqlite_answers,manual_cypher,utc_microseconds

ROOT=Path('D:/ch7');REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
CANON=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
BASE=ROOT/'experiments/ch7_private/continuation_20261021'
OUT=ROOT/'reports/osdb_actor_candidate_intake_v1_20261026'
PRIV=ROOT/'experiments/ch7_private'
GRAPHS={
 'redis':(ROOT/'code/OSDB_CSTKG_Text2Cypher/data_real/d1_typed_projection_staging_v1_2/execution_csv',PRIV/'automated_dev_pilot_v1_20261021/neo4j_runtime/import_receipt.json'),
 'duckdb':(PRIV/'duckdb_projection_publication_v1_20261021/execution_csv',PRIV/'publication_duckdb_runtime_v1_20261021/import_receipt.json'),
 'iotdb':(BASE/'e8_iotdb_projection/execution_csv',BASE/'e8_iotdb_runtime_v1/import_receipt.json'),
 'elasticsearch':(BASE/'e8_elasticsearch_projection/execution_csv',BASE/'e8_elasticsearch_runtime_v1/import_receipt.json')}
SOURCES={k:'https://www.chaoss.community/kb/metric-'+k+'/' for k in
    ('contributors','inactive-contributors','new-contributors','organizational-diversity')}
PARAMS={'history_start':'2023-01-01T00:00:00Z','cutoff':'2023-07-01T00:00:00Z','end':'2023-08-01T00:00:00Z'}
# Labels are reviewed provenance dispositions, never model capability/gold inputs.
CANDIDATES=[
 {'id':'ACT01','kind':'union_openers','source':'contributors','disposition':'DEVELOPMENT_OLD_GOAL_PROJECTION',
  'question':'Count distinct Actor accounts observed opening an Issue or PullRequest during July 2023 UTC in the supplied full snapshot. Count an account once across both artifact types. Only outgoing artifact-to-Actor EVENT_ACTION edges with service_rel_type OPENED_BY count. Do not infer other contribution types.',
  'old_overlap':'source:opening_actor_attribution already returns per-Actor Issue/PR counts; union is a projection of that goal, not an independent goal.'},
 {'id':'ACT02','kind':'both_opening_domains','source':'contributors','disposition':'DEVELOPMENT_OLD_GOAL_FILTER',
  'question':'Count distinct Actor accounts with at least one observed Issue opening and at least one observed PullRequest opening during July 2023 UTC in the supplied full snapshot. Use only artifact-to-Actor EVENT_ACTION OPENED_BY records; count each account once.',
  'old_overlap':'Positive Issue and PR count filtering of source:opening_actor_attribution; no new semantic-goal certification.'},
 {'id':'ACT03','kind':'observed_opening_gap','source':'inactive-contributors','disposition':'CANDIDATE_PENDING_SEMANTIC_AND_GRAPH_ACCEPTANCE',
  'question':'At 2023-08-01T00:00:00Z, count Actor accounts with at least one observed Issue or PullRequest opening in [2023-01-01,2023-07-01) UTC and none in [2023-07-01,2023-08-01) UTC in the supplied full snapshot. Only artifact-to-Actor EVENT_ACTION OPENED_BY records count. Report absence of these recorded openings only, without inferring that a person is inactive or made no other contributions.',
  'old_overlap':'Temporal anti-membership over earlier/later incoming opening facts differs from the old one-window per-Actor count and literal outgoing-Actor boundary. Historical missing-activity/unknown-goal material still requires semantic review; new family not certified.'},
 {'id':'ACT04','kind':'first_observed_opening','source':'new-contributors','disposition':'DEVELOPMENT_SCOPED_HISTORY_PROXY',
  'question':'Within observed Issue and PullRequest opening records in [2023-01-01,2023-08-01) UTC, count Actor accounts whose earliest recorded opening is in July 2023. Use the supplied full snapshot and artifact-to-Actor EVENT_ACTION OPENED_BY records only. Ignore records before the stated observation interval; do not call the result first-ever contributors.',
  'old_overlap':'role_boundary:BLIND3_NEW_CONTRIB already covers true first contribution with incomplete history. This bounded-history rewrite changes the evidence contract, not a certified independent goal.'},
 {'id':'ACT05','kind':'true_first_ever','source':'new-contributors','disposition':'DEVELOPMENT_OLD_BOUNDARY_INSUFFICIENT_HISTORY',
  'question':'How many people made their first-ever contribution of any kind to the project in July 2023, using only this supplied 2023 snapshot? Do not assume complete prior history or identify accounts as people.',
  'old_overlap':'Existing role_boundary:BLIND3_NEW_CONTRIB and missing_prior_history. No numeric gold; first-observed counts are not a substitute.'},
 {'id':'ACT06','kind':'organization_share','source':'organizational-diversity','disposition':'DEVELOPMENT_ATTRIBUTION_BOUNDARY_PENDING_EVIDENCE',
  'question':'What percentage of all July 2023 project contributions were made by employees of each organization, using only the supplied snapshot? Do not infer employment from account names or repository ownership.',
  'old_overlap':'Earlier contribution attribution explicitly excludes organization affiliation. Requires authoritative time-valid affiliations, people identity and contribution denominator, not just repository ownership.'}]


def read(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def write(p,v):
    p.parent.mkdir(parents=True,exist_ok=True)
    with p.open('x',encoding='utf-8',newline='\n') as f:json.dump(v,f,ensure_ascii=False,indent=2);f.write('\n')
def stamp():return datetime.now(timezone(timedelta(hours=8))).isoformat()
def runtime():
    folder=REPO/'experiment-harness/public_benchmark_v1'
    return {str(p.relative_to(folder)):sha(p) for p in folder.rglob('*.py')}
def inspect_budget():
    sys.path.insert(0,str(CANON/'experiment-harness/public_benchmark_v1'))
    from attempt_budget import AttemptBudget
    b=AttemptBudget(BASE/'budget',ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json',
        path_rewrites=[(r'C:\Users\10651\Documents\trae_projects\ch7_private',PRIV)]).inspect()
    return {k:b[k] for k in ('reserved','cap','remaining','pending')}


def main():
    assert not OUT.exists(),'Preserve previous intake; never overwrite or rerun blindly'
    checks=subprocess.run([sys.executable,'-m','unittest','test_osdb_actor_reference'],
        cwd=Path(__file__).parent,text=True,capture_output=True)
    assert checks.returncode==0 and 'Ran 8 tests' in checks.stderr
    OUT.mkdir();before=runtime();budget=inspect_budget();assert not budget['pending']
    frozen=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
    assert all(sha(CANON/p)==s for p,s in frozen.items())
    lineage=ROOT/'reports/osdb_task_lineage_v1_20261026/registry.json'
    old=read(lineage);assert len(old)==167
    old_sources=set(x['source_url'] for x in old if x['source_url'])
    protocol={'kind':'SOURCE_MOTIVATED_DEVELOPER_INTAKE_NOT_INDEPENDENT_CONFIRMATION',
        'created_local':stamp(),'source_pages':SOURCES,'parameters_fixed_before_scan':PARAMS,
        'candidates':CANDIDATES,'projects':list(GRAPHS),'planned_bindings':24,
        'rules':['All six candidates and four historical projects retained regardless of counts.',
            'Two projected/filtered old goals and existing true-first-ever boundary stay development.',
            'One potential temporal-gap goal requires semantic and actual graph acceptance.',
            'No family label or code-string mismatch certifies independence.',
            'All current candidates and references are exposed to the same assistant; no independent human or blind-review claim.',
            'Whole supplied snapshots; no silently inferred main-repository prefix.',
            'Actor.entity_id is an account key, not resolved person identity; no bot removal.',
            'Reference registry, source labels, status and expected counts are evaluator-only, never model inputs.',
            'Current source pages are captured with timestamp+SHA, not claimed historic commit versions.',
            'SQLite validates offline relational agreement only; manual Cypher is not executed.'],
        'historical_lineage_sha256':sha(lineage),'budget_before':budget,'runtime_before':before,
        'canonical_frozen':frozen,'unit_checks':checks.stderr.strip(),
        'analysis_sources':{p.name:sha(p) for p in (Path(__file__),Path(__file__).with_name('osdb_actor_reference.py'),Path(__file__).with_name('test_osdb_actor_reference.py'))}}
    write(OUT/'protocol.json',protocol)
    source_receipts={};session=requests.Session()
    for key,url in SOURCES.items():
        response=session.get(url,timeout=(10,30));response.raise_for_status()
        assert len(response.content)<2_000_000
        p=OUT/'source_cache'/(key+'.html');p.parent.mkdir(exist_ok=True);p.write_bytes(response.content)
        soup=BeautifulSoup(response.content,'html.parser')
        source_receipts[key]={'url':url,'final_url':response.url,'retrieved_local':stamp(),
            'http_status':response.status_code,'bytes':p.stat().st_size,'sha256':sha(p),
            'heading_inventory':[x.get_text(' ',strip=True) for x in soup.find_all(['h1','h2','h3'])],
            'locator':'Question, Overview and Data Collection/Filters',
            'exact_url_previously_registered':url in old_sources,
            'absence_of_exact_url_does_not_prove_no_prior_exposure':True}
    write(OUT/'source_receipts.json',source_receipts)
    history,cutoff,end=(utc_microseconds(PARAMS[k]) for k in ('history_start','cutoff','end'))
    csv.field_size_limit(64*1024**2);projects={};registry=[]
    for project,(folder,receipt_path) in GRAPHS.items():
        start=time.monotonic();receipt=read(receipt_path)
        hashes={name:sha(folder/name) for name in ('nodes.csv','edges.csv')}
        assert hashes==receipt['source_hashes']
        nodes={};keys=set();actor_fields=Counter()
        with (folder/'nodes.csv').open(encoding='utf-8-sig',newline='') as f:
            for row in csv.DictReader(f):
                assert row['node_uid'] not in nodes
                props=json.loads(row['properties_json']);identity=(row['label'],row['entity_id'])
                assert identity not in keys and type(props['entity_id']) is str and props['entity_id']==row['entity_id']
                keys.add(identity);nodes[row['node_uid']]=identity
                if row['label']=='Actor':actor_fields.update(props.keys())
        assert len(nodes)==receipt['nodes']
        rows=[];physical=0;count=0;event_ids=set();diagnostics=Counter()
        with (folder/'edges.csv').open(encoding='utf-8-sig',newline='') as f:
            for row in csv.DictReader(f):
                count+=1;assert row['source_node_uid'] in nodes and row['target_node_uid'] in nodes
                source,target=nodes[row['source_node_uid']],nodes[row['target_node_uid']]
                if row['native_rel_type']!='EVENT_ACTION' or source[0] not in ('Issue','PullRequest') or target[0]!='Actor' or row['service_rel_type']!='OPENED_BY':continue
                physical+=1;props=json.loads(row['properties_json'])
                assert props['service_rel_type']=='OPENED_BY' and props.get('query_eligible') is not False
                when=utc_microseconds(props.get('source_event_time'))
                key=json.dumps(['Actor','str',target[1]],ensure_ascii=False,separators=(',',':'))
                rows.append((key,source[0],when))
                eid=props.get('edge_uid');assert type(eid) is str and eid and eid not in event_ids
                event_ids.add(eid)
                diagnostics['outside_reference_history_rows']+=not(history<=when<end)
        assert count==receipt['edges']
        answer=python_answers(rows,history,cutoff,end);sql=sqlite_answers(rows,history,cutoff,end)
        assert answer==sql
        for candidate in CANDIDATES:
            task={'task_id':project+'_'+candidate['id'],'project':project,**candidate,
                'source_receipt':source_receipts[candidate['source']],
                'input_scope':'whole supplied snapshot, account IDs, no human identity resolution',
                'snapshot_csv_sha256':hashes,'parameters':PARAMS,
                'all_projects_and_data_census_previously_exposed':True,
                'confirmation_admitted':False,'new_model_outputs_seen':False,
                'reference_graph_execution':'PENDING_NOT_EXECUTED',
                'provenance':'AI-assisted developer operationalization; not a sourced real-user question or exact CHAOSS metric',
                'formal_split':None,'formal_N':None}
            if candidate['kind'] in KINDS:
                task.update(columns=['account_count'],csv_expected_rows=[[answer[candidate['kind']]]],
                    sqlite_expected_rows=[[sql[candidate['kind']]]],
                    reference_query=manual_cypher(candidate['kind']),
                    expected_status='SNAPSHOT_NUMERIC_REFERENCE_PREPARED_NOT_GRAPH_ACCEPTED')
            else:
                task.update(columns=None,csv_expected_rows=None,sqlite_expected_rows=None,reference_query=None,
                    expected_status='INSUFFICIENT_EVIDENCE_CANDIDATE_NOT_ADJUDICATED')
            registry.append(task)
        assert all(sha(folder/name)==s for name,s in hashes.items())
        projects[project]={'csv_sha256':hashes,'import_receipt_sha256':sha(receipt_path),
            'nodes':len(nodes),'edges':count,'opening_event_rows':physical,
            'actor_projection_property_names':dict(actor_fields),'reference_counts':answer,
            'all_four_Python_SQLite_agree':True,'diagnostics':dict(diagnostics),
            'offline_reference_wall_seconds':time.monotonic()-start}
        print(json.dumps({'project':project,'reference_counts':answer}),flush=True)
    write(OUT/'candidate_registry.json',registry);write(OUT/'project_reference_evidence.json',projects)
    assert runtime()==before and all(sha(CANON/p)==s for p,s in frozen.items())
    b2=inspect_budget();assert b2==budget
    summary={'kind':'OSDB_SOURCE_CANDIDATE_INTAKE_NOT_CONFIRMATION_OR_MODEL_RESULT',
        'completed_local':stamp(),'candidate_goals_registered':6,'project_bindings':len(registry),
        'dispositions_goals':dict(Counter(x['disposition'] for x in CANDIDATES)),
        'numeric_reference_bindings':16,'insufficient_evidence_candidate_bindings':8,
        'possible_temporal_goal_bindings_pending_review':4,'confirmation_admitted':0,
        'formal_N':None,'model_planned':None,'model_dispatched':0,'model_completed':0,'model_failed':0,'model_undispatched':None,
        'CSV_SQLite_agreements':16,'real_graph_reference_queries_executed':0,
        'source_pages_captured':source_receipts,'projects':projects,
        'unit_semantics_checks':8,'budget_before':budget,'budget_after':b2,
        'frozen_canonical_files_unchanged':len(frozen),'runtime_files_unchanged':len(before),
        'new_model_DB_SSH_retired_service_requests':0,'new_public_source_HTTP_requests':4,
        'entire_T1_complete':False,'T2_to_T5_started':False,'conditional_Text2GQL_budget_triggered':False,
        'public_code_sha256':protocol['analysis_sources'],'protocol_sha256':sha(OUT/'protocol.json'),
        'private_registry_sha256':sha(OUT/'candidate_registry.json'),
        'needed_next':['Historical semantic overlap review including56unknown-goal positions',
            'Authorized graph-host execution of the manual references',
            'Develop-only generic adaptation, then freeze and separate confirmation provenance',
            'Same-model inference/resource/feedback budget and prespecified score admission'],
        'not_claimed':['24independent tasks','Real user question sample','Unseen projects',
            'Exact CHAOSS metrics','Model accuracy','Certified abstention gold','Ours benefit','Complete T1']}
    write(OUT/'summary.json',summary)
    write(ROOT/'reports/t1_osdb_actor_candidate_intake_20261026.json',summary)
    print(json.dumps({k:summary[k] for k in ('candidate_goals_registered','project_bindings',
        'CSV_SQLite_agreements','confirmation_admitted','budget_after','entire_T1_complete')}))


if __name__=='__main__':main()
