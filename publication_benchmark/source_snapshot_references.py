"""Eight prelisted new partial source operationalizations, not exact metrics.

Reference computation reads CSV only. Queries are separate handwritten checks;
neither calls the tested service compiler. All eight source candidates retained,
not filtered by tested-service support. Old six E3 goals remain development.
"""
from collections import Counter,defaultdict
import csv
from datetime import datetime
import json
from pathlib import Path


START='2023-04-01T00:00:00Z'
END='2023-04-08T00:00:00Z'
GOALS=[('branch_events','SRC011',['created_branches','deleted_branches']),
       ('pushed_commits','SRC013',['commit_count']),
       ('pr_observed_duration','SRC018',['pr_id','observed_seconds']),
       ('reviewed_prs','SRC019',['reviewed_pr_count']),
       ('opening_actor_attribution','SRC021',['actor_id','issue_count','pr_count']),
       ('issue_observed_duration','SRC028',['issue_id','observed_seconds']),
       ('issue_observed_response','SRC029',['issue_id','observed_response_seconds']),
       ('issues_observed_active','SRC030',['active_issue_count'])]


def seconds(a,b):
    return int((datetime.fromisoformat(b.replace('Z','+00:00'))-
                datetime.fromisoformat(a.replace('Z','+00:00'))).total_seconds())


class SnapshotReference:
    def __init__(self,data):
        root=Path(data)
        with (root/'nodes.csv').open(encoding='utf-8-sig',newline='') as f:
            self.nodes={x['node_uid']:(x['label'],x['entity_id']) for x in csv.DictReader(f)}
        self.edges=[]
        with (root/'edges.csv').open(encoding='utf-8-sig',newline='') as f:
            for row in csv.DictReader(f):
                props=json.loads(row['properties_json'])
                self.edges.append({'source':self.nodes[row['source_node_uid']],
                    'target':self.nodes[row['target_node_uid']],
                    'physical':row['native_rel_type'],'service':row['service_rel_type'],
                    'time':props.get('source_event_time')})

    def repo_id(self):
        counts=Counter(eid.split('#')[0][3:] for label,eid in self.nodes.values() if label=='PullRequest')
        if not counts:
            raise ValueError('No PR identity scope')
        ordered=sorted(counts.items(),key=lambda x:(-x[1],x[0]))
        if len(ordered)>1 and ordered[0][1]==ordered[1][1]:
            raise ValueError('Ambiguous focal repository; do not guess')
        return ordered[0][0]

    def expected(self,repo):
        issue='I_'+repo+'#';pr='PR_'+repo+'#'
        events=[e for e in self.edges if e['physical']=='EVENT_ACTION' and e['time']]
        inside=lambda e:START<=e['time']<END
        direct=[e for e in events if e['target'][0]=='Actor']
        branches={action:{e['source'][1] for e in direct if e['source'][0]=='Branch' and e['source'][1].startswith('B_'+repo+':') and e['service']==action and inside(e)} for action in ('CREATED_BY','DELETED_BY')}
        commits={e['target'][1] for e in events if e['source'][0]=='Push' and e['source'][1].startswith('P_'+repo+'.') and e['target'][0]=='Commit' and e['service']=='COMMITTED_AS' and inside(e)}
        reviews={e['target'][1] for e in events if e['source'][0]=='PullRequestReview' and e['target'][0]=='PullRequest' and e['target'][1].startswith(pr) and e['service']=='CREATED_IN' and inside(e)}
        attribution=defaultdict(lambda:{'Issue':set(),'PullRequest':set()})
        for e in direct:
            label,eid=e['source']
            if label in ('Issue','PullRequest') and eid.startswith(issue if label=='Issue' else pr) and e['service']=='OPENED_BY' and inside(e):
                attribution[e['target'][1]][label].add(eid)
        opened=defaultdict(list);closed=defaultdict(list)
        for e in direct:
            label,eid=e['source']
            if label not in ('Issue','PullRequest') or not eid.startswith(issue if label=='Issue' else pr):
                continue
            if e['service']=='OPENED_BY':opened[(label,eid)].append((e['time'],e['target'][1]))
            if e['service']=='CLOSED_BY' or (label=='PullRequest' and e['service']=='MERGED_BY'):
                closed[(label,eid)].append(e['time'])
        durations={'Issue':[],'PullRequest':[]};cohort={}
        for (label,eid),rows in opened.items():
            first=min(t for t,a in rows)
            if not START<=first<END:continue
            later=[t for t in closed[(label,eid)] if t>=first]
            if later:durations[label].append([eid,seconds(first,min(later))])
            if label=='Issue':cohort[eid]=(first,{a for t,a in rows if t==first})
        creators=defaultdict(set)
        for e in direct:
            if e['source'][0]=='IssueComment' and e['service']=='OPENED_BY':
                creators[e['source'][1]].add(e['target'][1])
        responses=defaultdict(list)
        active=set()
        for e in events:
            if e['source'][0]=='Issue' and e['source'][1].startswith(issue) and e['target'][0]=='Actor' and e['service'] in ('OPENED_BY','CLOSED_BY','REOPENED_BY') and inside(e):
                active.add(e['source'][1])
            if e['source'][0]=='IssueComment' and e['target'][0]=='Issue' and e['target'][1].startswith(issue) and e['service']=='COMMENTED_ON_ISSUE':
                eid=e['target'][1]
                if inside(e):active.add(eid)
                if eid in cohort:
                    first,opening_actors=cohort[eid]
                    if e['time']>=first and creators[e['source'][1]]-opening_actors:
                        responses[eid].append(e['time'])
        return {'branch_events':[[len(branches['CREATED_BY']),len(branches['DELETED_BY'])]],
            'pushed_commits':[[len(commits)]],'reviewed_prs':[[len(reviews)]],
            'opening_actor_attribution':[[a,len(rows['Issue']),len(rows['PullRequest'])] for a,rows in sorted(attribution.items())],
            'pr_observed_duration':sorted(durations['PullRequest']),
            'issue_observed_duration':sorted(durations['Issue']),
            'issue_observed_response':[[eid,seconds(first,min(responses[eid])) if responses[eid] else None] for eid,(first,_) in sorted(cohort.items())],
            'issues_observed_active':[[len(active)]]}


def queries(repo):
    p={'start':START,'end':END,'issue_prefix':'I_'+repo+'#','pr_prefix':'PR_'+repo+'#',
       'branch_prefix':'B_'+repo+':','push_prefix':'P_'+repo+'.'}
    when='r.source_event_time >= $start AND r.source_event_time < $end'
    q={
      'branch_events':"MATCH (n:Branch)-[r:EVENT_ACTION]->(a:Actor) WHERE n.entity_id STARTS WITH $branch_prefix AND "+when+" RETURN count(DISTINCT CASE WHEN r.service_rel_type='CREATED_BY' THEN n.entity_id END) AS created_branches,count(DISTINCT CASE WHEN r.service_rel_type='DELETED_BY' THEN n.entity_id END) AS deleted_branches",
      'pushed_commits':"MATCH (p:Push)-[r:EVENT_ACTION]->(c:Commit) WHERE p.entity_id STARTS WITH $push_prefix AND r.service_rel_type='COMMITTED_AS' AND "+when+" RETURN count(DISTINCT c.entity_id) AS commit_count",
      'reviewed_prs':"MATCH (v:PullRequestReview)-[r:EVENT_ACTION]->(p:PullRequest) WHERE p.entity_id STARTS WITH $pr_prefix AND r.service_rel_type='CREATED_IN' AND "+when+" RETURN count(DISTINCT p.entity_id) AS reviewed_pr_count",
      'opening_actor_attribution':"MATCH (n)-[r:EVENT_ACTION]->(a:Actor) WHERE ((n:Issue AND n.entity_id STARTS WITH $issue_prefix) OR (n:PullRequest AND n.entity_id STARTS WITH $pr_prefix)) AND r.service_rel_type='OPENED_BY' AND "+when+" RETURN a.entity_id AS actor_id,count(DISTINCT CASE WHEN n:Issue THEN n.entity_id END) AS issue_count,count(DISTINCT CASE WHEN n:PullRequest THEN n.entity_id END) AS pr_count",
      'issues_observed_active':"MATCH (i:Issue) WHERE i.entity_id STARTS WITH $issue_prefix OPTIONAL MATCH (i)-[r:EVENT_ACTION]->(a:Actor) WHERE r.service_rel_type IN ['OPENED_BY','CLOSED_BY','REOPENED_BY'] AND "+when+" WITH i,count(r)>0 AS direct OPTIONAL MATCH (c:IssueComment)-[r:EVENT_ACTION]->(i) WHERE r.service_rel_type='COMMENTED_ON_ISSUE' AND "+when+" WITH i,direct,count(r)>0 AS commented RETURN count(CASE WHEN direct OR commented THEN i.entity_id END) AS active_issue_count"}
    for label,name,prefix,col in [('Issue','issue_observed_duration','issue_prefix','issue_id'),('PullRequest','pr_observed_duration','pr_prefix','pr_id')]:
        actions="['CLOSED_BY']" if label=='Issue' else "['CLOSED_BY','MERGED_BY']"
        q[name]=f"MATCH (n:{label})-[o:EVENT_ACTION]->(a:Actor) WHERE n.entity_id STARTS WITH ${prefix} AND o.service_rel_type='OPENED_BY' WITH n,min(o.source_event_time) AS opened WHERE opened >= $start AND opened < $end MATCH (n)-[r:EVENT_ACTION]->(b:Actor) WHERE r.service_rel_type IN {actions} AND r.source_event_time >= opened WITH n,opened,min(r.source_event_time) AS closed RETURN n.entity_id AS {col},toInteger((datetime(closed).epochMillis-datetime(opened).epochMillis)/1000) AS observed_seconds"
    q['issue_observed_response']="MATCH (i:Issue)-[o:EVENT_ACTION]->(a:Actor) WHERE i.entity_id STARTS WITH $issue_prefix AND o.service_rel_type='OPENED_BY' WITH i,min(o.source_event_time) AS opened WHERE opened >= $start AND opened < $end MATCH (i)-[o:EVENT_ACTION]->(a:Actor) WHERE o.service_rel_type='OPENED_BY' AND o.source_event_time=opened WITH i,opened,collect(DISTINCT a.entity_id) AS opening_actors OPTIONAL MATCH (c:IssueComment)-[r:EVENT_ACTION]->(i),(c)-[author:EVENT_ACTION]->(responder:Actor) WHERE r.service_rel_type='COMMENTED_ON_ISSUE' AND author.service_rel_type='OPENED_BY' AND NOT responder.entity_id IN opening_actors AND r.source_event_time >= opened WITH i,opened,min(r.source_event_time) AS response RETURN i.entity_id AS issue_id,CASE WHEN response IS NULL THEN null ELSE toInteger((datetime(response).epochMillis-datetime(opened).epochMillis)/1000) END AS observed_response_seconds"
    return {name:{'query':query,'parameters':p} for name,query in q.items()}


def wordings(repo):
    scope=f'Use only observed facts in the supplied snapshot. UTC window [2023-04-01,2023-04-08). Literal Issue prefix I_{repo}#, PullRequest prefix PR_{repo}#, Branch prefix B_{repo}: and Push prefix P_{repo}. as applicable. '
    details={
      'branch_events':'Count distinct Branch identifiers with observed CREATED_BY events and with observed DELETED_BY events in the window, as separate counts. Count each branch once per action. This is observed event activity, not a description of branch-management policy.',
      'pushed_commits':'Count distinct Commit identifiers reached by Push EVENT_ACTION[COMMITTED_AS] records in the window. Scope the Push identifiers. Do not use commit authored dates or merely referenced commits.',
      'reviewed_prs':'Count distinct PullRequest identifiers with an incoming PullRequestReview EVENT_ACTION[CREATED_IN] record in the window. Scope the PullRequest identifiers. This counts observed formal-review records, not review quality or complete history.',
      'opening_actor_attribution':'For each Actor with an observed initial OPENED_BY event from an Issue or PullRequest in the window, return the Actor identifier, distinct Issue count, and distinct PullRequest count. Include zero in the other count. Scope each source label by its own literal prefix. Do not include reopening or organization affiliation.',
      'issues_observed_active':'Count distinct Issues having either an outgoing OPENED_BY/CLOSED_BY/REOPENED_BY event to an Actor, or an incoming IssueComment COMMENTED_ON_ISSUE event, in the window. Count each Issue once across all these activities. References alone are not activity in this definition.',
      'pr_observed_duration':'For each PullRequest whose earliest observed initial OPENED_BY event is in the window, find its earliest observed CLOSED_BY or MERGED_BY event at or after that opening. Return the PR identifier and elapsed seconds. Omit PRs with no such observed terminal event. Later terminal events may be outside the window. Do not infer current state or complete-history resolution duration.',
      'issue_observed_duration':'For each Issue whose earliest observed initial OPENED_BY event is in the window, find its earliest observed CLOSED_BY event at or after that opening. Return the Issue identifier and elapsed seconds. Omit Issues with no such observed close. Later closing events may be outside the window. Do not infer current state or complete-history resolution duration.',
      'issue_observed_response':'For every Issue whose earliest observed initial OPENED_BY event is in the window, find its earliest later-or-equal IssueComment COMMENTED_ON_ISSUE event whose comment has an OPENED_BY Actor different from all opening Actors at the initial opening time. Return Issue identifier and elapsed seconds, null if no such observed response. Later responses may be outside the window. Do not infer unseen historical comments.'}
    return {name:scope+text+' Return the complete result.' for name,text in details.items()}
