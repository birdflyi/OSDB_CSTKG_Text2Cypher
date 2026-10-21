from io import BytesIO
import pytest

from fetch_candidate_projects import BoundedStream
from profile_candidate_graph import Profile
from select_holdout_projects import js_divergence, distribution


def test_prefix_budget_is_enforced():
    stream=BoundedStream(BytesIO(b'abcdef'),4)
    assert stream.read(3)==b'abc'
    with pytest.raises(ValueError):
        stream.read(2)


def test_parallel_records_and_missing_event_time_remain_visible():
    nodes={'p':{'label':'PullRequest','properties':['entity_id']},'x':{'label':'ExternalResource','properties':['entity_id']}}
    p=Profile(nodes)
    props={'service_rel_type':'LINKS_TO','url_domain_etld1':'example.org','source_event_time':'2023-01-01T00:00:00Z'}
    p.add('p','x','REFERENCE',props)
    p.add('p','x','REFERENCE',{'service_rel_type':'LINKS_TO'})
    r=p.result()
    assert r['edges']==2 and r['repeated_link_pairs']==1 and r['parallel_link_excess_records']==1
    assert r['time_missing']['REFERENCE']==1 and r['missing_domain_link_records']==1


def test_distribution_contrast_is_normalized_and_symmetric():
    p=distribution({'A':10})
    q=distribution({'B':20})
    assert js_divergence(p,p)==0
    assert js_divergence(p,q)==js_divergence(q,p)==1
