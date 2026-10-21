"""Two synthetic worlds expose information lost by the six-service interface.

Only an Issue OPENED_BY edge timestamp changes. No current service reads that
timestamp on that edge kind. Equal interface observations need not determine
the monthly Issue-opening count. No real event or model prediction is changed.
"""
import argparse
import csv
from itertools import permutations
import json
from pathlib import Path

import run_baselines as b
from mechanism_core import Oracle, compile_call
from run_mechanism_pilot import bag
from e3_scope_pilot import references, query
import strong_baselines as s


def calls():
    result=[]
    def add(op,**args):
        value={'operation':op,'arguments':args}
        compile_call(value)
        result.append(value)
    for issue in ('I_900001#1','I_900001#2','I_900001#3'):
        add('issue_openers',issue_id=issue)
    for prefix in ('I_','I_900001','I_900001#','I_900001#1','I_900001#2','I_999999#'):
        add('issue_count',issue_prefix=prefix)
        for grain in ('paths','distinct_comments','distinct_commits'):
            add('issue_commit_path_count',issue_prefix=prefix,grain=grain)
    for pr in ('PR_900001#1','PR_900001#2'):
        for width in (1,2):
            for fields in permutations(('domain','external_resource_id'),width):
                for distinct in (False,True):
                    add('pr_links',pr_id=pr,fields=list(fields),distinct=distinct)
        for start,end in (('2023-01-01','2024-01-01'),('2023-07-01','2023-08-01'),('2023-08-01','2023-09-01')):
            for grain in ('records','distinct_targets'):
                for op in ('pr_link_count','pr_domain_counts'):
                    add(op,pr_id=pr,start_date=start,end_date=end,grain=grain)
    return result


def write_world(out,changed):
    labels={'i':('Issue','I_900001#1'),'j':('Issue','I_900001#2'),'a':('Actor','A_1'),
            'p':('PullRequest','PR_900001#1'),'x':('ExternalResource','E_1'),
            'c':('IssueComment','IC_900001#1#1'),'k':('Commit','C_900001@'+'a'*40)}
    nodes=[{'node_uid':uid,'label':label,'entity_id':eid,'properties_json':json.dumps({'entity_id':eid})}
           for uid,(label,eid) in labels.items()]
    descriptions=[('i','a','EVENT_ACTION','OPENED_BY','2023-08-04T00:00:00Z' if changed else '2023-07-04T00:00:00Z'),
                  ('j','a','EVENT_ACTION','OPENED_BY','2023-08-04T00:00:00Z'),
                  ('p','x','REFERENCE','LINKS_TO','2023-07-10T00:00:00Z'),
                  ('p','x','REFERENCE','LINKS_TO','2023-07-10T00:00:00Z'),
                  ('c','i','EVENT_ACTION','COMMENTED_ON_ISSUE','2023-07-06T00:00:00Z'),
                  ('c','k','REFERENCE','REFERENCES','2023-07-06T00:00:00Z')]
    edges=[]
    for index,(source,target,physical,service,date) in enumerate(descriptions):
        props={'edge_uid':'W'+str(index),'service_rel_type':service,'source_event_time':date}
        if service=='LINKS_TO':
            props['url_domain_etld1']='example.org'
        edges.append({'source_node_uid':source,'target_node_uid':target,'native_rel_type':physical,
                      'service_rel_type':service,'properties_json':json.dumps(props)})
    out.mkdir(parents=True)
    for name,rows in (('nodes.csv',nodes),('edges.csv',edges)):
        with (out/name).open('x',encoding='utf-8',newline='') as f:
            w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)


def build(output):
    out=Path(output)
    if out.exists():
        raise FileExistsError('Keep original witness')
    observations=[]
    cc=calls()
    for name,changed in [('A',False),('B',True)]:
        folder=out/name
        write_world(folder,changed)
        nodes,edges,_=b.load_graph(folder)
        oracle=Oracle(nodes,edges)
        service_rows=[oracle.expected(c) for c in cc]
        target=references(folder,'900001')['issues_new']
        b.save(folder/'oracle_observations.json',{'calls':cc,'service_rows':service_rows,'target_rows':target})
        observations.append((service_rows,target))
    assert all(bag(a)==bag(z) for a,z in zip(observations[0][0],observations[1][0]))
    assert observations[0][1]==[[1]] and observations[1][1]==[[0]]
    result={'mode':'SYNTHETIC_INFORMATION_BOUNDARY_NOT_MODEL_ACCURACY','service_calls_checked':len(cc),
            'service_outputs_equal':True,'target_A':observations[0][1],'target_B':observations[1][1],
            'changed_property':'source_event_time on Issue -> EVENT_ACTION[OPENED_BY] -> Actor only',
            'assumptions':'Same user context; observations restricted to the six current services, no other graph/API data access.',
            'limits':'Finite argument execution checks are not an exhaustive proof. Compiler dependency analysis explains invariance for arbitrary legal parameters. This is a counterexample to interface sufficiency, not a new general theorem or real-world failure rate.',
            'compiler_sha256':b.sha(Path(__file__).with_name('mechanism_core.py')),
            'world_hashes':{name:{file:b.sha(out/name/file) for file in ('nodes.csv','edges.csv','oracle_observations.json')} for name in ('A','B')}}
    b.save(out/'manifest.json',result)
    print(json.dumps(result,indent=2))


def verify(root,world,receipt,port,output):
    root=Path(root)
    record=json.loads(Path(receipt).read_text(encoding='utf-8'))
    manifest=json.loads((root/'manifest.json').read_text(encoding='utf-8'))
    data=json.loads((root/world/'oracle_observations.json').read_text(encoding='utf-8'))
    if any(b.sha(root/world/k)!=v for k,v in manifest['world_hashes'][world].items()):
        raise ValueError('World source changed')
    if record['source_hashes']!={k:v for k,v in manifest['world_hashes'][world].items() if k.endswith('.csv')}:
        raise ValueError('Import source mismatch')
    if b.sha(Path(__file__).with_name('mechanism_core.py'))!=manifest['compiler_sha256']:
        raise ValueError('Compiler changed')
    b.DB_URL=f'http://127.0.0.1:{port}/db/neo4j/tx/commit'
    if b.db('CALL db.info() YIELD id,name RETURN id,name')['data']!=record['database_identity']:
        raise ValueError('Wrong synthetic database')
    rows=[]
    for call,expected in zip(data['calls'],data['service_rows']):
        c=compile_call(call)
        r=s.evaluate({'status':'ANSWER','cypher':c['query'],'parameters':c['parameters']},'G')['execution']
        assert r['status']=='EXECUTED' and r['columns']==c['columns'] and bag(r['rows'])==bag(expected)
        rows.append(r['rows'])
    r=s.evaluate({'status':'ANSWER','cypher':query('issues_new'),'parameters':{'prefix':'I_900001#','start':'2023-07-01T00:00:00Z','end':'2023-08-01T00:00:00Z'}},'G')['execution']
    assert r['status']=='EXECUTED' and r['rows']==data['target_rows']
    b.save(Path(output),{'mode':'INFORMATION_WITNESS_ACTUAL_CYPHER_EXECUTION','world':world,
            'verified_service_calls':len(rows),'service_rows':rows,'target_rows':r['rows'],
            'database_identity':record['database_identity'],'manifest_sha256':b.sha(root/'manifest.json'),
            'code_sha256':b.sha(__file__)})
    print(json.dumps({'world':world,'verified_service_calls':len(rows),'target_rows':r['rows']}))


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--build')
    p.add_argument('--root');p.add_argument('--world',choices=('A','B'))
    p.add_argument('--receipt');p.add_argument('--port',type=int,choices=(17477,17478));p.add_argument('--output')
    a=p.parse_args()
    if a.build:
        build(a.build)
    else:
        if not all((a.root,a.world,a.receipt,a.port,a.output)):
            p.error('Verify requires root, world, receipt, port, output')
        verify(a.root,a.world,a.receipt,a.port,a.output)
