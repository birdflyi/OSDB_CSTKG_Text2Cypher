"""One bounded schema-grounded supplement, never catalog- or outcome-filtered."""
import argparse
import json
from pathlib import Path
import subprocess
import time
import requests

import run_baselines as b
from run_protocol_control import reserve,derived


STRATA=('one-hop projected facts','aggregation and multiplicity','two-hop collaboration joins','event-time conditions')
SYSTEM='''Propose exactly two meaningful OSS graph-snapshot information requests
for the supplied structural stratum. Return ONLY a JSON array; each object has
exactly id,question,paraphrase,columns,purpose. Do not produce an answer, query,
service call or implementation. Use only the supplied graph schema/properties.
Do not request file contents, issue/PR titles, actor names, organization names,
current states, authored commit dates, first-ever contributions, complete history,
bug-fix/introduce causality or any other absent evidence. Push actor is not commit
author. Ingestion identity_status/identity_validation is not real-world identity
verification. REFERENCES is not RESOLVES. Ask concrete collaboration/reference
facts using typed graph meanings; full lists/counts, not arbitrary samples.
Each question must specify returned fields and duplicate/aggregation grain. Use
the supplied binding tokens for IDs. For temporal queries use edge event times
and the half-open UTC window [2023-04-01,2023-04-08); explain which event is timed.
No fuzzy words such as recent, most or inactive without a precise definition.
The paraphrase must keep exactly the same scope, fields, multiplicity and time
conditions. It may change natural wording. Preserve all constraints explicitly.
Do not refer to another task, a prior answer or an unspecified actor/project.
These are synthetic proposals, not real-user observations.
'''


def run(a):
    out=Path(a.output)
    if out.exists():raise FileExistsError('No reroll or silent overwrite')
    repo=Path(__file__).resolve().parents[1]
    if subprocess.check_output(['git','status','--porcelain'],cwd=repo,text=True).strip():
        raise ValueError('Freeze a clean generation protocol')
    cfg=json.loads(Path(a.config).read_text(encoding='utf-8'))
    schema=json.loads(Path(a.schema).read_text(encoding='utf-8'))
    b.save(out/'manifest.json',{'mode':'ONE_SHOT_SCHEMA_GROUNDED_SYNTHETIC_SUPPLEMENT',
        'git_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),
        'strata':STRATA,'requested_goals':8,'generation_calls':4,'schema_sha256':b.sha(a.schema),
        'prompt':SYSTEM,'max_tokens':6000,'automatic_retries':0,
        'limits':'One same-provider model, AI-assisted intake, not humans/user distribution. No tested catalog/results/gold supplied; preserve failures and unsupported proposals.'})
    consecutive=0
    for index,stratum in enumerate(STRATA,1):
        payload={k:cfg[k] for k in ('model','temperature','top_p','reasoning_effort')}
        data={'structural_stratum':stratum,'id_prefix':'GROUNDED'+str(index)+'_',
            'graph_metadata':schema,
            'binding_tokens':{'issue_id':'{{ISSUE_ID}}','pr_id':'{{PR_ID}}','actor_id':'{{ACTOR_ID}}','commit_id':'{{COMMIT_ID}}',
                'issue_prefix':'I_{{REPO_ID}}#','pr_prefix':'PR_{{REPO_ID}}#','repo_entity_id':'R_{{REPO_ID}}'},
            'scope':'Only the supplied projected snapshot. Scope project Issue/PR requests by the literal prefixes; do not equate those prefixes with graph repository membership. No service catalog or example query is available.'}
        payload.update(max_tokens=6000,messages=[{'role':'system','content':SYSTEM},
            {'role':'user','content':json.dumps(data,ensure_ascii=False)}])
        folder=out/'calls'/('stratum_'+str(index))
        reserve(a.budget_dir,folder/'request.json')
        b.save(folder/'request.json',payload)
        start=time.monotonic()
        try:
            response=requests.post(cfg['endpoint'],headers={'Authorization':'Bearer '+cfg['api_key']},json=payload,timeout=(10,120),allow_redirects=False)
            try:body=response.json()
            except ValueError:body={'non_json_body':response.text}
            terminal={'http_status':response.status_code,'body':body,'seconds':time.monotonic()-start}
            infra=response.status_code==429 or response.status_code>=500
        except requests.RequestException as exc:
            terminal={'transport_error':type(exc).__name__,'seconds':time.monotonic()-start};infra=True
        b.save(folder/'terminal.json',terminal)
        consecutive=consecutive+1 if infra else 0
        derived(out/'progress.json',{'completed_calls':index,'status':'RUNNING' if index<4 else 'DISPATCHES_COMPLETED'})
        print(stratum,'INFRASTRUCTURE' if infra else terminal['http_status'],flush=True)
        if consecutive>=3:
            derived(out/'progress.json',{'completed_calls':index,'status':'STOPPED_INFRASTRUCTURE'})
            break


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    for key in ('schema','config','output','budget-dir'):parser.add_argument('--'+key,required=True)
    run(parser.parse_args())
