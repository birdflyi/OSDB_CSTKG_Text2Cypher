"""CSV-only grounded-proposal references and separate manual query checks.

Two failed generator calls are missing four proposals. Retain the four full
records: three adapted answerable goals, one literal-direction zero control.
No request is replaced according to tested-system support.
"""
from collections import Counter,defaultdict
import source_snapshot_references as src


IDS=('GROUNDED2_AGG_REF_BY_TYPE','GROUNDED2_AGG_EVENT_BY_ACTOR_TIME','GROUNDED4_001','GROUNDED4_002')


def bindings(oracle):
    repo=oracle.repo_id()
    inside=lambda e:e['time'] and src.START<=e['time']<src.END
    issues=sorted(eid for label,eid in oracle.nodes.values() if label=='Issue' and eid.startswith('I_'+repo+'#'))
    prs=sorted(eid for label,eid in oracle.nodes.values() if label=='PullRequest' and eid.startswith('PR_'+repo+'#'))
    comment_targets={e['target'][1] for e in oracle.edges if e['physical']=='EVENT_ACTION' and e['source'][0]=='IssueComment' and e['target'][0]=='Issue' and e['service']=='COMMENTED_ON_ISSUE' and inside(e)}
    reference_sources={e['source'][1] for e in oracle.edges if e['physical']=='REFERENCE' and e['source'][0]=='Issue'}
    issue=next((x for x in issues if x in comment_targets and x in reference_sources),None)
    if issue is None:issue=next((x for x in issues if x in comment_targets),issues[0])
    push_branches={e['source'][1] for e in oracle.edges if e['physical']=='EVENT_ACTION' and e['source'][0]=='Push' and e['target'][0]=='Branch' and e['service']=='ADDED_TO'}
    matched_prs={e['target'][1] for e in oracle.edges if e['physical']=='REFERENCE' and e['source'][0]=='Push' and e['target'][0]=='PullRequest' and e['service']=='REFERENCES' and inside(e) and e['source'][1] in push_branches}
    pr=next((x for x in prs if x in matched_prs),prs[0])
    actors=sorted({e['target'][1] for e in oracle.edges if e['physical']=='EVENT_ACTION' and e['target'][0]=='Actor' and inside(e)})
    if not actors:raise ValueError('No incoming-Actor direction witness')
    return {'repo_id':repo,'issue_id':issue,'pr_id':pr,'actor_id':actors[0]}


def expected(oracle,binding):
    issue,pr,actor=(binding[x] for x in ('issue_id','pr_id','actor_id'))
    inside=lambda e:e['time'] and src.START<=e['time']<src.END
    refs=Counter(e['service'] for e in oracle.edges if e['source']==('Issue',issue) and e['physical']=='REFERENCE')
    outgoing=Counter(e['service'] for e in oracle.edges if e['source']==('Actor',actor) and e['physical']=='EVENT_ACTION' and inside(e))
    creators=defaultdict(set);branches=defaultdict(set)
    for e in oracle.edges:
        if e['physical']=='EVENT_ACTION' and e['source'][0]=='IssueComment' and e['target'][0]=='Actor' and e['service']=='OPENED_BY':
            creators[e['source'][1]].add(e['target'][1])
        if e['physical']=='EVENT_ACTION' and e['source'][0]=='Push' and e['target'][0]=='Branch' and e['service']=='ADDED_TO':
            branches[e['source'][1]].add(e['target'][1])
    comments=defaultdict(list);pushes=defaultdict(list)
    for e in oracle.edges:
        if not inside(e):continue
        if e['physical']=='EVENT_ACTION' and e['source'][0]=='IssueComment' and e['target']==('Issue',issue) and e['service']=='COMMENTED_ON_ISSUE':
            for author in creators[e['source'][1]]:comments[(e['source'][1],author)].append(e['time'])
        if e['physical']=='REFERENCE' and e['source'][0]=='Push' and e['target']==('PullRequest',pr) and e['service']=='REFERENCES':
            for branch in branches[e['source'][1]]:pushes[(e['source'][1],branch)].append(e['time'])
    return {IDS[0]:[[kind,count] for kind,count in sorted(refs.items())],
        IDS[1]:[[kind,count] for kind,count in sorted(outgoing.items())],
        IDS[2]:[[comment,min(times),author] for (comment,author),times in sorted(comments.items())],
        IDS[3]:[[push,min(times),branch] for (push,branch),times in sorted(pushes.items())]}


def queries(binding):
    params={**binding,'start':src.START,'end':src.END}
    texts={
        IDS[0]:"MATCH (i:Issue {entity_id:$issue_id})-[r:REFERENCE]->() RETURN r.service_rel_type AS service_rel_type,count(r) AS reference_count",
        IDS[1]:"MATCH (a:Actor {entity_id:$actor_id})-[r:EVENT_ACTION]->() WHERE r.source_event_time >= $start AND r.source_event_time < $end RETURN r.service_rel_type AS service_rel_type,count(r) AS event_count",
        IDS[2]:"MATCH (c:IssueComment)-[r:EVENT_ACTION]->(i:Issue {entity_id:$issue_id}),(c)-[o:EVENT_ACTION]->(a:Actor) WHERE r.service_rel_type='COMMENTED_ON_ISSUE' AND o.service_rel_type='OPENED_BY' AND r.source_event_time >= $start AND r.source_event_time < $end WITH c,a,min(r.source_event_time) AS event_time RETURN c.entity_id AS comment_id,event_time,a.entity_id AS actor_id",
        IDS[3]:"MATCH (p:Push)-[r:REFERENCE]->(pr:PullRequest {entity_id:$pr_id}),(p)-[a:EVENT_ACTION]->(br:Branch) WHERE r.service_rel_type='REFERENCES' AND a.service_rel_type='ADDED_TO' AND r.source_event_time >= $start AND r.source_event_time < $end WITH p,br,min(r.source_event_time) AS event_time RETURN p.entity_id AS push_id,event_time,br.entity_id AS branch_id"}
    return {key:{'query':text,'parameters':params} for key,text in texts.items()}
