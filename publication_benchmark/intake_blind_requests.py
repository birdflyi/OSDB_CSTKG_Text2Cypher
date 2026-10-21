"""Preserve all role proposals; freeze only six clear evidence-boundary originals."""
import argparse
import json
from pathlib import Path
import re
import run_baselines as b


DISPOSITIONS={
 'BLIND1_001':('BOUNDARY','missing_text_and_current_state','Issue title, Actor name, current open status and assignment evidence are not available as requested.'),
 'BLIND1_002':('BOUNDARY','missing_text','Required PullRequest title is not available.'),
 'BLIND1_003':('BOUNDARY','missing_authorship','Referenced Commit identity is available; commit authorship is not. Push actor is not author. The paraphrase also changes the timed event.'),
 'BLIND3_NEW_CONTRIB':('BOUNDARY','missing_prior_history','First-ever contribution cannot be established from this snapshot; attribution fields also need definition.'),
 'BLIND3_INACTIVE_CONTRIB':('UNRESOLVED','undefined_inactivity','Inactivity threshold, activity types and historical cohort are undefined.'),
 'BLIND3_ISSUE_RESOLUTION':('UNRESOLVED','resolution_scope_and_pair_drift','Resolved vs closed, complete vs observed history, and timed cohort need adjudication; paraphrase changes the concept.'),
 'BLIND4_COMMIT_PROVENANCE':('BOUNDARY','missing_file_changes','No File/change-set evidence identifies the commit modifying multi.py; complete author provenance cannot be inferred.'),
 'BLIND4_BUG_FIX_CHAIN':('BOUNDARY','missing_fix_causality','No actual bug-fixing relation identifies the requested commit; references are not fixes.'),
 'BLIND4_DEVELOPER_HISTORY':('UNRESOLVED','activity_attribution_and_pair_drift','Contribution counting and Commit attribution need explicit definitions; paraphrase drops the observed time window. Ingestion identity validation is not human verification.')}


def read_proposals(root):
    proposals,flow=[],[]
    for folder in sorted((Path(root)/'calls').iterdir()):
        terminal=json.loads((folder/'terminal.json').read_text(encoding='utf-8'))
        choices=terminal.get('body',{}).get('choices') or []
        finish=choices[0].get('finish_reason') if choices else None
        content=choices[0].get('message',{}).get('content') if choices else None
        if finish!='stop' or not isinstance(content,str):
            flow.append({'role':folder.name,'received':0,'requested':3,'finish_reason':finish,
                         'status':'INCOMPLETE_RESPONSE','terminal_sha256':b.sha(folder/'terminal.json')})
            continue
        blocks=re.findall(r'```(?:json)?\s*([\s\S]*?)\s*```',content)
        rows=json.loads(blocks[0] if len(blocks)==1 else content)
        if not isinstance(rows,list) or len(rows)!=3:raise ValueError('Unexpected full role output')
        for row in rows:
            if set(row)!={'id','role','question','paraphrase','columns','purpose','source_motivation'}:
                raise ValueError('Unexpected proposal fields')
            kind,reason_code,reason=DISPOSITIONS[row['id']]
            proposals.append({**row,'intake_status':kind,'reason_code':reason_code,'reason':reason,
                'paraphrase_used_as_equivalent_test':False,
                'generation_terminal_sha256':b.sha(folder/'terminal.json')})
        flow.append({'role':folder.name,'received':len(rows),'requested':3,'finish_reason':finish,
                     'status':'FULL_STRUCTURED_RESPONSE','terminal_sha256':b.sha(folder/'terminal.json')})
    if {x['id'] for x in proposals}!=set(DISPOSITIONS):raise ValueError('Do not silently lose or invent proposals')
    return proposals,flow


def run(a):
    root,out=Path(a.generation),Path(a.output)
    if out.exists():raise FileExistsError('No overwrite of intake version')
    proposals,flow=read_proposals(root)
    b.save(out/'all_received_proposals.json',proposals)
    b.save(out/'intake.json',{'mode':'ROLE_GENERATION_INTAKE_NOT_REAL_USERS','requested':12,'received':9,
        'clear_boundary_originals':6,'unresolved_originals':3,'missing_due_truncation':3,
        'role_flow':flow,'no_regeneration_of_original_batch':True,
        'paraphrases_used_for_robustness':0,
        'limits':'AI-assisted adjudication. Missing records not fabricated. Several paraphrases alter intent; none is asserted equivalent in evaluation. Six boundary originals do not establish useful-answer accuracy or user representativeness.'})
    sources=Path(a.source_tasks)
    for project in ('redis','duckdb','iotdb','elasticsearch'):
        source=sources/project
        manifest=json.loads((source/'manifest.json').read_text(encoding='utf-8'))
        repo=manifest['repo_id']
        specs=[]
        for row in proposals:
            if row['intake_status']!='BOUNDARY':continue
            columns=['actor_id' if c=='{{ACTOR_ID}}' else c for c in row['columns']]
            question=row['question']+f' Context: the provided graph snapshot is for project {project}; its focal repository ID is {repo}. No additional conversation, complete prior history or external data is supplied. Return only supported graph evidence; do not invent missing facts.'
            specs.append({'id':project+'_'+row['id'],'project':project,'family':row['reason_code'],
                'semantic_goal':'role_boundary:'+row['id'],'group':'role_boundary:'+row['id'],'variant_index':0,
                'question':question,'columns':columns,'expected_status':'UNSUPPORTED',
                'accepted_nonanswer_statuses':['UNSUPPORTED','INSUFFICIENT_EVIDENCE'],'expected_rows':[],
                'service_expressible':False,'information_expressible':False,
                'boundary_reason':row['reason'],'source_motivation':row['source_motivation'],
                'raw_generation_id':row['id'],'format_adaptation':'Actor placeholder column name normalized to actor_id where present; original meaning retained. Canonical original only, no paraphrase scoring.',
                'provenance':'Catalog-blind same-provider synthetic role proposal with AI-assisted evidence-boundary adjudication; not real users or human review.'})
        target=out/'tasks'/project
        b.save(target/'specifications.json',specs)
        b.save(target/'shared_schema.json',json.loads((source/'shared_schema.json').read_text(encoding='utf-8')))
        b.save(target/'manifest.json',{'mode':'E7_BLIND_ROLE_EVIDENCE_BOUNDARY_STRATUM','project':project,'repo_id':repo,
            'source_hashes':manifest['source_hashes'],'cases':6,'task_sha256':b.sha(target/'specifications.json'),
            'generation_manifest_sha256':b.sha(root/'manifest.json'),'intake_sha256':b.sha(out/'intake.json'),
            'limits':'Six clear unanswerable originals. Refusal decisions only; no useful-answer or paraphrase-robustness claim.'})
    print('Intake: received9, boundary6, unresolved3, truncated missing3; four project bindings frozen.',flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    for key in ('generation','source-tasks','output'):parser.add_argument('--'+key,required=True)
    run(parser.parse_args())
