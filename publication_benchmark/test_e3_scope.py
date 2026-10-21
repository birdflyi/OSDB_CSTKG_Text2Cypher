import csv
import json

from e3_scope_pilot import references, query, TASKS
from strong_baselines import read_guard
from collect_demand_sources import question


def test_source_question_parser_handles_bold_and_links():
    assert question('# Title\n**Question:** What changed?\n## Overview')=='What changed?'


def test_csv_reference_deduplicates_and_requires_explicit_decline(tmp_path):
    nodes=[{'node_uid':'i','label':'Issue','entity_id':'I_1#1'},
           {'node_uid':'p','label':'PullRequest','entity_id':'PR_1#2'},
           {'node_uid':'q','label':'PullRequest','entity_id':'PR_1#3'},
           {'node_uid':'a','label':'Actor','entity_id':'A_2'}]
    with (tmp_path/'nodes.csv').open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=list(nodes[0]));w.writeheader();w.writerows(nodes)
    edges=[]
    for source,service,time,trigger in [('i','OPENED_BY','2023-07-01T00:00:00Z',''),('i','OPENED_BY','2023-07-01T00:00:00Z',''),('i','OPENED_BY','2023-08-01T00:00:00Z',''),('p','CLOSED_BY','2023-07-02T00:00:00Z','pull_merged=True'),('q','CLOSED_BY','2023-07-02T00:00:00Z','pull_merged=False')]:
        edges.append({'source_node_uid':source,'target_node_uid':'a','native_rel_type':'EVENT_ACTION','service_rel_type':service,'properties_json':json.dumps({'source_event_time':time,'raw_event_trigger':trigger})})
    with (tmp_path/'edges.csv').open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=list(edges[0]));w.writeheader();w.writerows(edges)
    r=references(tmp_path,'1')
    assert r['issues_new']==[[1]]
    assert r['pr_declined']==[['PR_1#3']]
    assert r['pr_acceptance_ratio']==[[0,1,0.0]]


def test_all_six_reference_queries_are_read_only():
    assert len({source for _,source,_,_ in TASKS})==6
    assert all(read_guard(query(task)) is None for task,_,_,_ in TASKS)
