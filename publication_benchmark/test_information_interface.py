import pytest
import information_interface as i
import mechanism_core as legacy
import csv
import json
import run_information_comparison as runner


def args(**overrides):
    return dict(entity_label='PullRequest',entity_prefix='PR_900001#',start_date='2023-07-01',end_date='2023-08-01',
                events=[{'action':'CLOSED_BY','explicit_decline':True}],mode='entity_count',fields=[],output_columns=['count'],**overrides)


def compile_args(a):
    return i.compile_call({'operation':'observed_events','arguments':a})


def test_time_and_decline_filter_precede_aggregation():
    compiled=compile_args(args())
    q=compiled['query']
    assert q.index('source_event_time')<q.index('count(DISTINCT')
    assert "pull_merged=False" in q
    assert compiled['parameters']['end_date']=='2023-08-01T00:00:00Z'


def test_ratio_uses_separate_predicates_and_zero_denominator_policy():
    a=args()
    a.update(mode='entity_count_ratio',events=[{'action':'MERGED_BY','explicit_decline':False},{'action':'CLOSED_BY','explicit_decline':True}],output_columns=['accepted_count','declined_count','ratio'])
    q=compile_args(a)['query']
    assert 'CASE WHEN (r.service_rel_type=$action0)' in q
    assert "CASE WHEN (r.service_rel_type=$action1 AND r.raw_event_trigger CONTAINS 'pull_merged=False')" in q
    assert 'denominator=0 THEN null' in q


def test_fact_projection_can_retain_evidence_and_date():
    a=args()
    a.update(mode='records',fields=['entity_id','event_time','edge_id'],output_columns=['pr_id','time','evidence_id'])
    q=compile_args(a)['query']
    assert 'r.source_event_time AS `time`' in q
    assert 'r.edge_uid AS `evidence_id`' in q
    assert 'RETURN DISTINCT' not in q


@pytest.mark.parametrize('patch',[
    {'entity_label':'Commit'}, {'entity_prefix':'I_900001#'},
    {'start_date':'2023-08-01'},{'end_date':'2023-02-30'},
    {'events':[{'action':'CLOSED_BY','explicit_decline':'false'}]},
    {'events':[{'action':'OPENED_BY','explicit_decline':True}]},
    {'output_columns':['count` MATCH (n) DELETE n']},
    {'fields':['edge_id']}, {'mode':'free_cypher'},
])
def test_rejects_invalid_or_unbounded_contract(patch):
    a=args()
    a.update(patch)
    with pytest.raises((ValueError,TypeError)):
        compile_args(a)


def test_old_compiler_unchanged():
    call={'operation':'issue_count','arguments':{'issue_prefix':'I_900001#'}}
    assert i.compile_call(call)==legacy.compile_call(call)
    assert len(legacy.CATALOG)==6


def test_independent_oracle_handles_boundary_duplicates_and_explicit_decline(tmp_path):
    with (tmp_path/'nodes.csv').open('w',encoding='utf-8',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=['node_uid','label','entity_id'])
        writer.writeheader()
        for uid,label,eid in [('p1','PullRequest','PR_900001#1'),('p2','PullRequest','PR_900001#2'),('a','Actor','A_1')]:
            writer.writerow(dict(node_uid=uid,label=label,entity_id=eid))
    with (tmp_path/'edges.csv').open('w',encoding='utf-8',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=['source_node_uid','target_node_uid','native_rel_type','service_rel_type','properties_json'])
        writer.writeheader()
        for index,(source,action,time,trigger) in enumerate([
            ('p1','MERGED_BY','2023-07-01T00:00:00Z','pull_merged=True'),
            ('p2','CLOSED_BY','2023-07-31T23:59:59Z','pull_merged=False'),
            ('p2','CLOSED_BY','2023-07-31T23:59:59Z','pull_merged=False'),
            ('p1','CLOSED_BY','2023-07-14T00:00:00Z',''),
            ('p1','CLOSED_BY','2023-08-01T00:00:00Z','pull_merged=False')]):
            writer.writerow(dict(source_node_uid=source,target_node_uid='a',native_rel_type='EVENT_ACTION',service_rel_type=action,
                properties_json=json.dumps({'source_event_time':time,'raw_event_trigger':trigger,'edge_uid':'e'+str(index)})))
    oracle=i.EventOracle(tmp_path)
    a=args()
    assert oracle.expected(a)==[[1]]
    a.update(mode='records',fields=['entity_id','edge_id'],output_columns=['pr_id','evidence_id'])
    assert len(oracle.expected(a))==2
    a.update(mode='entity_count_ratio',fields=[],events=[{'action':'MERGED_BY','explicit_decline':False},{'action':'CLOSED_BY','explicit_decline':True}],output_columns=['accepted','declined','ratio'])
    assert oracle.expected(a)==[[1,1,1.0]]
    a['entity_prefix']='PR_900001#999'
    assert oracle.expected(a)==[[0,0,None]]


def test_public_input_contains_no_gold_or_reference():
    spec={'question':'Count observed PRs.','columns':['count'],'expected_rows':[[900]],
          'reference_query':'PRIVATE_GOLD','reference_call':{'operation':'PRIVATE'}}
    for arm in ('G','P','I'):
        data=runner.public_input(spec,{'node_types':['Issue']},arm)
        assert data['task']=={'question':spec['question'],'columns':spec['columns']}
        assert 'PRIVATE' not in json.dumps(data)
        assert ('observed_events' in {x['operation'] for x in data['contracts']})==(arm!='P')
