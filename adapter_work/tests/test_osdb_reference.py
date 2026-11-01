import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from osdb_reference import solve

def row(s,t,rel,st='IssueComment',tt='Actor',time='2',eid='e'):
    return dict(src_entity_id=s,tar_entity_id=t,relation_label_repr=rel,src_entity_type=st,
                tar_entity_type=tt,event_time=time,event_id=eid,event_type='X')

def test_nested_set_relations_are_not_missing_top_level_relation():
    c=dict(window=dict(start='1',end='3'),set_a=dict(anchor='a',relation='r'),set_b=dict(anchor='b',relation='q'))
    rows=[row('a','x','r'),row('b','x','q'),row('a','y','r')]
    assert solve(rows,c,'set_intersection')==['x']
    assert solve(rows,c,'set_difference')==['y']

def test_two_hop_and_branching_contract_fields():
    c=dict(window=dict(start='1',end='3'),anchor='a',path=['r','q'],intermediate_type='IssueComment',target_type='Actor')
    rows=[row('a','m','r',tt='IssueComment'),row('m','x','q'),row('a','z','q')]
    assert solve(rows,c,'multi_hop_path')==['x']
    assert solve(rows,dict(c,relations=['r','q']),'branching_two_relations')==[[1,1]]

def test_typed_endpoints_and_snapshot_lookup():
    c=dict(window=dict(start='1',end='3'),anchor='a',relation='r',target_type='Actor',snapshot_scoped=True)
    rows=[row('a','x','r',time='0'),row('a','bad','r',tt='Repo')]
    assert solve(rows,c,'single_entity_lookup')==['x']
    assert solve(rows,c,'single_hop_neighbors')==[]

def test_event_identity_includes_both_endpoints_without_double_count():
    c=dict(window=dict(start='1',end='3'),entity_type='Repo')
    rows=[row('a','r','x',tt='Repo',eid='1'),row('a','r','x',tt='Repo',eid='1'),row('r','b','x',st='Repo',eid='2')]
    assert solve(rows,c,'duplicate_shared_identity')==[['r',2]]
