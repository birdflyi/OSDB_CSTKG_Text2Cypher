"""Freeze all eight new observed-snapshot source goals, without support filtering."""
import argparse
import json
from pathlib import Path

import run_baselines as b
from e3_scope_pilot import schema
import source_snapshot_references as ref


def build(data,project,census_path,output):
    data,out=Path(data),Path(output)
    if out.exists():
        raise FileExistsError('Do not overwrite frozen task bindings')
    census=json.loads(Path(census_path).read_text(encoding='utf-8'))
    sources={x['source_entry_id']:x for x in census['candidates']}
    oracle=ref.SnapshotReference(data)
    repo=oracle.repo_id()
    expected=oracle.expected(repo)
    queries=ref.queries(repo)
    texts=ref.wordings(repo)
    specs=[]
    for name,source_id,columns in ref.GOALS:
        rows=expected[name]
        specs.append({'id':project+'_SOURCE_'+name,'project':project,'family':name,
            'group':'source:'+name,'semantic_goal':'source:'+name,'variant_index':0,
            'question':texts[name],'columns':columns,'expected_status':'ANSWER','expected_rows':rows,
            'reference_query':queries[name]['query'],'reference_parameters':queries[name]['parameters'],
            'service_expressible':False,'information_expressible':False,
            'source_entry':source_id,'source_url':sources[source_id]['url'],
            'provenance':'AI-assisted developer operationalization of a complete prelisted set of eight new partial source candidates after method freeze. Not real-user data or exact source-metric replication.',
            'coverage_stratum':'graph-answerable beyond both fixed service catalogs',
            'scope_limits':'Observed records in supplied snapshot; not complete history or current state. Reference implementation and independent manual Cypher, no tested compiler used.',
            'output_contract_eligible':len(rows)<=500})
    b.save(out/'specifications.json',specs)
    b.save(out/'public_tasks.json',[{k:x[k] for k in ('id','question','columns')} for x in specs])
    b.save(out/'shared_schema.json',schema(data))
    b.save(out/'manifest.json',{'mode':'E7_NEW_SOURCE_PROXY_COVERAGE_STRATUM_NOT_USER_SAMPLE',
        'project':project,'repo_id':repo,'cases':len(specs),'independent_semantic_goals':8,
        'method_frozen_commit':'3d6bd22','compact_context_frozen_commit':'4ea628e',
        'source_hashes':{n:b.sha(data/n) for n in ('nodes.csv','edges.csv')},
        'task_sha256':b.sha(out/'specifications.json'),'source_census_sha256':b.sha(census_path),
        'reference_sha256':b.sha(Path(ref.__file__)),'builder_sha256':b.sha(__file__),
        'output_eligible':sum(x['output_contract_eligible'] for x in specs),
        'selection':'Retain all eight prelisted new partial candidates; no filtering by service support. One explicit specification per goal; not padded with rewrites.',
        'limits':'This stratum studies graph answerability versus catalog coverage, not a matched-capability primary accuracy contest. Eight tasks are partial operationalizations, not exact CHAOSS/RepoQA reproduction or an independent human sample.'})
    print(json.dumps({'project':project,'repo_id':repo,'goals':len(specs),'rows':{x['family']:len(x['expected_rows']) for x in specs},'all_output_eligible':all(x['output_contract_eligible'] for x in specs)}),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    for key in ('data','project','census','output'):
        parser.add_argument('--'+key,required=True)
    args=parser.parse_args()
    build(args.data,args.project,args.census,args.output)
