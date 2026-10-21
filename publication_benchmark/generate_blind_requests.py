"""Catalog-blind synthetic request proposals; not gold or a user study.

Exactly four generation requests, no automatic retries. Keep unfinished output
verbatim; all proposals enter intake before eligibility/capability annotation.
"""
import argparse
import json
from pathlib import Path
import subprocess
import time

import requests
import run_baselines as b
from run_protocol_control import reserve,derived


ROLES=('triage maintainer','pull request reviewer','collaboration analyst','provenance investigator')
SYSTEM='''You propose developer information needs, not answers or database queries.
Return exactly one JSON array containing three objects. Each object has exactly
id,role,question,paraphrase,columns,purpose,source_motivation. question and paraphrase
must request exactly the same result. columns is an ordered list of output names.
Do not supply a reference answer, Cypher, service name or implementation.
Use the supplied graph meanings and source questions as motivation. Preserve the
difference between references and causality, observed events and complete history.
A request can require unavailable information; do not silently replace it by a
nearby available task. Ask meaningful software collaboration/maintenance questions
appropriate for the role, including relationships and attribution where useful.
Use only supplied binding tokens for concrete entity identifiers and repository
prefixes. All time-bounded requests use [2023-04-01,2023-04-08) UTC. Make scope,
fields, time and duplicate/count semantics explicit enough to evaluate. Request
complete results where needed, not a hidden sample. Do not claim these are real
user questions. Each paraphrase should change natural wording, not the task.
source_motivation is an existing source entry ID or CORE_COLLABORATION_EXTENSION;
the latter denotes a new synthetic need motivated by the collaboration schema.
'''


def inputs(schema,census,role,index):
    return {'role':role,'id_prefix':'BLIND'+str(index)+'_',
        'graph_metadata':schema,
        'source_questions':[{k:x[k] for k in ('source_entry_id','title','source_question')} for x in census['candidates']],
        'binding_tokens':{'issue_id':'{{ISSUE_ID}}','pr_id':'{{PR_ID}}','actor_id':'{{ACTOR_ID}}',
                          'commit_id':'{{COMMIT_ID}}','issue_prefix':'I_{{REPO_ID}}#','pr_prefix':'PR_{{REPO_ID}}#'},
        'scope':'Provided projected snapshot only; no assumed file contents, complete GitHub history, current state, bug labels or fix/introduce causality. You may ask for these but must not silently redefine them.'}


def run(a):
    out=Path(a.output)
    if out.exists():
        raise FileExistsError('No silent regeneration; use a separately documented generation version')
    repo=Path(__file__).resolve().parents[1]
    if subprocess.check_output(['git','status','--porcelain'],cwd=repo,text=True).strip():
        raise ValueError('Freeze generation prompt before dispatch')
    cfg=json.loads(Path(a.config).read_text(encoding='utf-8'))
    schema=json.loads(Path(a.schema).read_text(encoding='utf-8'))
    census=json.loads(Path(a.census).read_text(encoding='utf-8'))
    b.save(out/'manifest.json',{'mode':'CATALOG_BLIND_SYNTHETIC_PROPOSALS_NOT_REAL_USERS',
        'git_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),
        'method_frozen_commit':'3d6bd22','roles':ROLES,'requested_proposals':12,'calls':4,
        'schema_sha256':b.sha(a.schema),'census_sha256':b.sha(a.census),'prompt':SYSTEM,
        'max_tokens':4000,'automatic_retries':0,
        'limits':'Same provider model as evaluation. Role prompts do not create human subjects, independent experts or a representative user sample.'})
    consecutive=0
    for index,role in enumerate(ROLES,1):
        folder=out/'calls'/('role_'+str(index))
        payload={k:cfg[k] for k in ('model','temperature','top_p','reasoning_effort')}
        payload.update(max_tokens=4000,messages=[{'role':'system','content':SYSTEM},
            {'role':'user','content':json.dumps(inputs(schema,census,role,index),ensure_ascii=False)}])
        reserve(a.budget_dir,folder/'request.json')
        b.save(folder/'request.json',payload)
        start=time.monotonic()
        try:
            response=requests.post(cfg['endpoint'],headers={'Authorization':'Bearer '+cfg['api_key']},
                                   json=payload,timeout=(10,120),allow_redirects=False)
            try:
                body=response.json()
            except ValueError:
                body={'non_json_body':response.text}
            terminal={'http_status':response.status_code,'body':body,'seconds':time.monotonic()-start}
            infra=response.status_code==429 or response.status_code>=500
        except requests.RequestException as exc:
            terminal={'transport_error':type(exc).__name__,'seconds':time.monotonic()-start}
            infra=True
        b.save(folder/'terminal.json',terminal)
        consecutive=consecutive+1 if infra else 0
        derived(out/'progress.json',{'calls_completed':index,'status':'RUNNING' if index<4 else 'DISPATCHES_COMPLETED'})
        print(role,'INFRASTRUCTURE' if infra else terminal['http_status'],flush=True)
        if consecutive>=3:
            derived(out/'progress.json',{'calls_completed':index,'status':'STOPPED_INFRASTRUCTURE'})
            break


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    for key in ('schema','census','config','output','budget-dir'):
        parser.add_argument('--'+key,required=True)
    run(parser.parse_args())
