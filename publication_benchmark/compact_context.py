"""Representation-appropriate context, without dropping direct graph facts.

G retains every schema signature, node/edge property and raw trigger value,
but no unusable service-call examples or catalog. I/P retain their executable
contracts and service examples, without unused Cypher/storage metadata. Common
business meanings and snapshot limits stay available to every arm.
"""
import information_interface as info
import strong_baselines as s
import run_baselines as b


def compact_schema(schema):
    props=schema['node_property_names']
    common=set.intersection(*(set(v) for v in props.values())) if props else set()
    return {'node_labels':sorted(props),'node_properties_on_every_label':sorted(common),
        'additional_node_properties':{label:sorted(set(names)-common) for label,names in props.items() if set(names)-common},
        'edge_property_names':schema['edge_property_names'],
        'typed_relationship_signatures':schema['typed_relationship_signatures'],
        'raw_event_trigger_values':schema['raw_event_trigger_values'],
        'interpretation':schema['interpretation']}


def public_input(spec,schema,arm):
    examples=s.examples()
    common={'task':{k:spec[k] for k in ('question','columns')},
        'observed_event_definitions':info.EVENT_CONTRACT['meaning'],
        'business_limits':'Observed snapshot facts only. Literal ID prefix is not graph repository membership. Event time is not commit authored time. REFERENCES is not fixing/introducing/resolving causality. Do not infer complete history or current lifecycle state.'}
    if arm=='G':
        return {**common,'schema':b.SCHEMA,'full_graph_metadata':compact_schema(schema),
            'schematic_examples':[{k:x[k] for k in ('question','columns','direct')} for x in examples]}
    return {**common,'contracts':info.CATALOG if arm=='I' else s.CATALOG,
        'schematic_examples':[{k:x[k] for k in ('question','columns','service')} for x in examples]}
