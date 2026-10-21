"""Freeze all received grounded proposals with explicit adaptation provenance."""
import argparse
import json
from pathlib import Path
import re
import run_baselines as b
import grounded_references as g
import source_snapshot_references as src


def received(root):
    rows=[];flow=[]
    for folder in sorted((Path(root)/'calls').iterdir()):
        terminal=json.loads((folder/'terminal.json').read_text(encoding='utf-8'))
        choices=terminal.get('body',{}).get('choices') or []
        text=choices[0].get('message',{}).get('content') if choices else None
        finish=choices[0].get('finish_reason') if choices else None
        if finish!='stop' or not isinstance(text,str):
            flow.append({'stratum':folder.name,'received':0,'requested':2,'http_status':terminal.get('http_status'),'finish_reason':finish});continue
        blocks=re.findall(r'```(?:json)?\s*([\s\S]*?)\s*```',text)
        parsed=json.loads(blocks[0] if len(blocks)==1 else text)
        if not isinstance(parsed,list) or len(parsed)!=2:raise ValueError('Unexpected supplement response')
        rows.extend(parsed);flow.append({'stratum':folder.name,'received':2,'requested':2,'http_status':terminal.get('http_status'),'finish_reason':finish})
    if {x['id'] for x in rows}!=set(g.IDS):raise ValueError('Do not silently omit or invent proposals')
    return rows,flow


def build(a):
    out=Path(a.output)
    if out.exists():raise FileExistsError('Do not overwrite task freeze')
    proposals,flow=received(a.generation)
    oracle=src.SnapshotReference(a.data);binding=g.bindings(oracle)
    answers=g.expected(oracle,binding);queries=g.queries(binding)
    source=Path(a.source_tasks)
    prior=json.loads((source/'manifest.json').read_text(encoding='utf-8'))
    specs=[]
    for row in proposals:
        diagnostic=row['id']==g.IDS[1]
        columns=(['comment_id','event_time','actor_id'] if row['id']==g.IDS[2] else
                 ['push_id','event_time','branch_id'] if row['id']==g.IDS[3] else row['columns'])
        variants=[row['question']] if diagnostic else [row['question'],row['paraphrase']]
        clarification=' Context: query only observed records in the supplied graph. The displayed event_time means canonical edge.source_event_time (normalized UTC), not a commit authored date. '
        if row['id'] in (g.IDS[2],g.IDS[3]):
            clarification+='Return exactly one row per requested entity pair; if several qualifying timed records exist for that pair, return the earliest qualifying time. '
        if row['id']==g.IDS[3]:
            clarification+='The reference direction is Push -> REFERENCE[REFERENCES] -> PullRequest, as in the typed schema. '
        if diagnostic:
            clarification+='This is a literal outgoing-edge query, not an inferred performed-activity metric. Do not reverse the requested direction; empty grouped results have no rows. '
        clarification+='Return the complete result with columns '+','.join(columns)+'.'
        for index,text in enumerate(variants):
            question=text+clarification
            for token,key in [('{{ISSUE_ID}}','issue_id'),('{{PR_ID}}','pr_id'),('{{ACTOR_ID}}','actor_id')]:question=question.replace(token,binding[key])
            specs.append({'id':a.project+'_'+row['id']+'_'+str(index),'project':a.project,'family':row['id'],
                'semantic_goal':'grounded:'+row['id'],'group':'grounded:'+row['id'],'variant_index':index,
                'question':question,'columns':columns,'expected_status':'ANSWER','expected_rows':answers[row['id']],
                'reference_query':queries[row['id']]['query'],'reference_parameters':queries[row['id']]['parameters'],
                'service_expressible':False,'information_expressible':False,
                'stratum':'literal_direction_diagnostic' if diagnostic else 'schema_grounded_synthetic_answerable',
                'raw_generation_id':row['id'],'adaptation':'Normalized qualified column aliases/event-time spelling; explicit earliest-time tie rule for one-row-per-pair; incoming Push-reference orientation resolved from schema. Original source preserved. Literal outgoing-Actor zero probe uses canonical only due paraphrase direction drift.',
                'output_contract_eligible':len(answers[row['id']])<=500,
                'provenance':'One fixed catalog-blind schema-grounded supplement, same provider model, AI-assisted semantic adjudication and independent CSV/Cypher references. Not human requests or exact unedited generated text.'})
    b.save(out/'specifications.json',specs)
    b.save(out/'shared_schema.json',json.loads((source/'shared_schema.json').read_text(encoding='utf-8')))
    b.save(out/'manifest.json',{'mode':'E7_ADAPTED_GROUNDED_SYNTHETIC_WITH_DIRECTION_CONTROL','project':a.project,
        'source_hashes':prior['source_hashes'],'cases':len(specs),'answerable_goal_count':3,'diagnostic_goal_count':1,
        'generation_requested':8,'generation_received':4,'missing_due_infrastructure':4,'flow':flow,
        'bindings':binding,'task_sha256':b.sha(out/'specifications.json'),
        'generation_manifest_sha256':b.sha(Path(a.generation)/'manifest.json'),
        'reference_sha256':b.sha(Path(g.__file__)),'builder_sha256':b.sha(__file__),
        'limits':'All four received proposals retained, all outside fixed catalogs. Three adapted positive goal families, not eight independent natural-language goals. One zero-result direction control separate from main usefulness. Paraphrases share explicit clarification; no human study.'})
    print(json.dumps({'project':a.project,'cases':len(specs),'bindings':binding,'row_counts':{k:len(v) for k,v in answers.items()},'all_output_eligible':all(x['output_contract_eligible'] for x in specs)}),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    for key in ('generation','source-tasks','data','project','output'):parser.add_argument('--'+key,required=True)
    build(parser.parse_args())
