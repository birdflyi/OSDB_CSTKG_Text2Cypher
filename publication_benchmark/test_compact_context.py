import compact_context as c
import strong_baselines as s
import information_interface as i
import run_information_comparison as runner


def schema():
    return {'node_property_names':{'Issue':['entity_id','issue_extra'],'PullRequest':['entity_id','pr_extra']},
        'edge_property_names':{'EVENT_ACTION':['service_rel_type','source_event_time']},
        'typed_relationship_signatures':[['Issue','EVENT_ACTION','OPENED_BY','Actor']],
        'raw_event_trigger_values':['pull_merged=False'],'interpretation':'Known semantics.'}


def test_compaction_preserves_all_direct_schema_information():
    original=schema()
    compact=c.compact_schema(original)
    for label,names in original['node_property_names'].items():
        recovered=set(compact['node_properties_on_every_label'])|set(compact['additional_node_properties'].get(label,[]))
        assert recovered==set(names)
    for field in ('edge_property_names','typed_relationship_signatures','raw_event_trigger_values','interpretation'):
        assert compact[field]==original[field]


def test_context_separates_unusable_metadata_not_business_meanings():
    spec={'question':'Count issues.','columns':['count'],'expected_rows':[[999]],'reference_call':{'private':True}}
    direct=c.public_input(spec,schema(),'G')
    service=c.public_input(spec,schema(),'I')
    control=c.public_input(spec,schema(),'P')
    assert 'contracts' not in direct
    assert 'full_graph_metadata' not in service
    assert service['contracts']==i.CATALOG
    assert control['contracts']==s.CATALOG
    assert direct['observed_event_definitions']==service['observed_event_definitions']==control['observed_event_definitions']
    for data in (direct,service,control):
        assert data['task']=={'question':spec['question'],'columns':spec['columns']}


def test_predeclared_graph_information_refusal_classes_exclude_errors():
    spec={'expected_status':'UNSUPPORTED','accepted_nonanswer_statuses':['UNSUPPORTED','INSUFFICIENT_EVIDENCE'],
          'service_expressible':False,'information_expressible':False}
    obs={'execution':{'status':'NOT_EXECUTED'}}
    assert runner.grade(spec,{'prediction':{'status':'INSUFFICIENT_EVIDENCE'}},obs,'G')['correct']
    assert not runner.grade(spec,{'prediction':{'status':'ERROR'}},obs,'G')['correct']
    assert not runner.grade(spec,{'prediction':{'status':'NEEDS_INPUT'}},obs,'G')['correct']
