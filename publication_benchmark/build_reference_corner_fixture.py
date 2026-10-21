"""Two bounded synthetic worlds for reference corner cases; no model outputs.

These are evaluator checks, not new useful tasks or model-effect samples.
"""
import argparse
import csv
from datetime import datetime,timedelta,timezone
import hashlib
import json
from pathlib import Path

import grounded_references as g
import source_snapshot_references as s


def stamp(seconds):
    return (datetime(2023,4,1,tzinfo=timezone.utc)+timedelta(seconds=seconds)).isoformat().replace('+00:00','Z')


def build(output):
    out=Path(output)
    if out.exists():raise FileExistsError('Do not overwrite fixture history')
    out.mkdir(parents=True)
    nodes={};edges=[];worlds=[]
    def node(label,eid):
        uid=label+'|'+eid
        nodes[uid]={'node_uid':uid,'label':label,'entity_id':eid,'properties_json':json.dumps({'entity_id':eid})}
        return uid
    def edge(source,target,physical,service,seconds):
        uid='fixture_edge_'+str(len(edges)+1)
        props={'edge_uid':uid,'service_rel_type':service,'source_event_time':stamp(seconds),
            'raw_event_trigger':'synthetic_reference_diagnostic'}
        edges.append({'source_node_uid':source,'target_node_uid':target,'native_rel_type':physical,
            'service_rel_type':service,'properties_json':json.dumps(props)})
    for w,repo in enumerate(('990001','990002')):
        issue=lambda i:node('Issue',f'I_{repo}#{i}')
        pr=lambda i:node('PullRequest',f'PR_{repo}#{i}')
        owner=node('Actor','A_'+repo+'01');other=node('Actor','A_'+repo+'02')
        main=node('Branch','B_'+repo+':main');temp=node('Branch','B_'+repo+':temp')
        extra=node('Branch','B_'+repo+':extra')
        push=node('Push','P_'+repo+'.1')
        commits=[node('Commit','C_'+repo+str(i)) for i in range(1,4)]
        e=lambda a,b,k,t:edge(a,b,'EVENT_ACTION',k,t)
        ref=lambda a,b,k,t:edge(a,b,'REFERENCE',k,t)
        e(main,owner,'CREATED_BY',0);e(main,owner,'CREATED_BY',1)
        e(main,owner,'DELETED_BY',604800);e(temp,owner,'CREATED_BY',-1);e(temp,other,'DELETED_BY',2)
        if w:e(extra,other,'CREATED_BY',604799);e(main,owner,'DELETED_BY',604799)
        e(push,commits[0],'COMMITTED_AS',3);e(push,commits[0],'COMMITTED_AS',4)
        e(push,commits[1],'COMMITTED_AS',604800)
        if w:e(push,commits[2],'COMMITTED_AS',604799)
        review=node('PullRequestReview','PRR_'+repo+'.1')
        e(review,pr(1),'CREATED_IN',5);e(review,pr(1),'CREATED_IN',6)
        e(node('PullRequestReview','PRR_'+repo+'.2'),pr(2),'CREATED_IN',604800)
        if w:e(node('PullRequestReview','PRR_'+repo+'.3'),pr(2),'CREATED_IN',7)
        e(issue(1),owner,'OPENED_BY',0);e(issue(1),owner,'OPENED_BY',0)
        e(issue(1),other,'CLOSED_BY',60);e(issue(1),other,'CLOSED_BY',90);e(issue(1),owner,'REOPENED_BY',120)
        e(issue(2),owner,'OPENED_BY',86400);e(issue(3),other,'OPENED_BY',604800)
        e(issue(0),owner,'OPENED_BY',-100);e(issue(0),other,'CLOSED_BY',10)
        e(pr(1),owner,'OPENED_BY',600);e(pr(1),other,'MERGED_BY',900);e(pr(1),other,'CLOSED_BY',1200)
        e(pr(2),other,'OPENED_BY',86400)
        if w:
            e(pr(2),other,'CLOSED_BY',86420)
            e(issue(4),other,'OPENED_BY',172800);e(issue(4),other,'CLOSED_BY',172801)
        c1=node('IssueComment','IC_'+repo+'.1');c2=node('IssueComment','IC_'+repo+'.2')
        c3=node('IssueComment','IC_'+repo+'.3');c4=node('IssueComment','IC_'+repo+'.4')
        for comment,actor in [(c1,owner),(c2,other),(c3,owner),(c4,other)]:e(comment,actor,'OPENED_BY',-1)
        e(c1,issue(1),'COMMENTED_ON_ISSUE',10)
        response=45 if w else 30
        e(c2,issue(1),'COMMENTED_ON_ISSUE',response);e(c2,issue(1),'COMMENTED_ON_ISSUE',response+10)
        e(c3,issue(2),'COMMENTED_ON_ISSUE',86410);e(c4,issue(5),'COMMENTED_ON_ISSUE',33)
        ref(issue(1),commits[0],'REFERENCES',20);ref(issue(1),commits[0],'REFERENCES',21)
        ref(issue(1),node('ExternalResource','ER_'+repo),'LINKS_TO',22)
        if w:ref(issue(1),commits[1],'REFERENCES',23)
        ref(push,pr(1),'REFERENCES',200);ref(push,pr(1),'REFERENCES',250)
        e(push,main,'ADDED_TO',-1)
        if w:e(push,temp,'ADDED_TO',-1)
        binding={'repo_id':repo,'issue_id':f'I_{repo}#1','pr_id':f'PR_{repo}#1','actor_id':'A_'+repo+'01'}
        manual_source={
            'branch_events':[[2,2]] if w else [[1,1]],'pushed_commits':[[2]] if w else [[1]],
            'reviewed_prs':[[2]] if w else [[1]],
            'opening_actor_attribution':[['A_'+repo+'01',2,1],['A_'+repo+'02',1 if w else 0,1]],
            'pr_observed_duration':[[f'PR_{repo}#1',300]]+([[f'PR_{repo}#2',20]] if w else []),
            'issue_observed_duration':[[f'I_{repo}#1',60]]+([[f'I_{repo}#4',1]] if w else []),
            'issue_observed_response':[[f'I_{repo}#1',response],[f'I_{repo}#2',None]]+([[f'I_{repo}#4',None]] if w else []),
            'issues_observed_active':[[5 if w else 4]]}
        manual_grounded={g.IDS[0]:[['LINKS_TO',1],['REFERENCES',3 if w else 2]],g.IDS[1]:[],
            g.IDS[2]:[[f'IC_{repo}.1',stamp(10),'A_'+repo+'01'],[f'IC_{repo}.2',stamp(response),'A_'+repo+'02']],
            g.IDS[3]:[[f'P_{repo}.1',stamp(200),'B_'+repo+':main']]+([[f'P_{repo}.1',stamp(200),'B_'+repo+':temp']] if w else [])}
        worlds.append({'repo_id':repo,'binding':binding,'manual_source':manual_source,'manual_grounded':manual_grounded})
    for name,rows,fields in [('nodes.csv',list(nodes.values()),['node_uid','label','entity_id','properties_json']),
        ('edges.csv',edges,['source_node_uid','target_node_uid','native_rel_type','service_rel_type','properties_json'])]:
        with (out/name).open('x',encoding='utf-8',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader();writer.writerows(rows)
    oracle=s.SnapshotReference(out)
    def bag(rows):return sorted(json.dumps(row,sort_keys=True) for row in rows)
    for world in worlds:
        for actual,expected in [(oracle.expected(world['repo_id']),world['manual_source']),
            (g.expected(oracle,world['binding']),world['manual_grounded'])]:
            for goal,rows in expected.items():
                if bag(rows)!=bag(actual[goal]):raise ValueError('Manual/CSV mismatch: '+goal)
    manifest={'mode':'REFERENCE_CORNER_CASE_DIAGNOSTIC_NOT_MODEL_EFFECT','nodes':len(nodes),'edges':len(edges),
        'worlds':worlds,'manual_csv_goal_world_checks':24,
        'builder_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'source_hashes':{n:hashlib.sha256((out/n).read_bytes()).hexdigest() for n in ('nodes.csv','edges.csv')},
        'limits':'Synthetic evaluator validation only. Not independent user goals, random graph sampling, model predictions or a proof of query equivalence.'}
    with (out/'manifest.json').open('x',encoding='utf-8') as f:json.dump(manifest,f,indent=2)
    print(json.dumps({k:manifest[k] for k in ('nodes','edges','manual_csv_goal_world_checks')}))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);build(p.parse_args().output)
