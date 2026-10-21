"""Export preselected holdout graphs without reading model predictions."""
import argparse
from collections import Counter
import csv
import json
from pathlib import Path
import sys

import run_baselines as b


def run(a):
    source,out=Path(a.input),Path(a.output)
    if out.exists():
        raise FileExistsError('Never overwrite a holdout projection')
    selection=json.loads(Path(a.selection).read_text(encoding='utf-8'))
    if a.project not in (selection['scale_project'],selection['distribution_contrast_project']):
        raise ValueError('Project was not preselected')
    profile=json.loads(Path(a.profile).read_text(encoding='utf-8'))
    if b.sha(a.profile)!=selection['profile_hashes'][a.project]:
        raise ValueError('Preselection profile changed')
    raw_manifest=json.loads(Path(a.source_manifest).read_text(encoding='utf-8'))
    if raw_manifest['status']!='COMPLETE_REQUESTED_MEMBERS' or b.sha(source)!=raw_manifest['files'][source.name]['sha256']:
        raise ValueError('Raw source changed')
    repo=Path(__file__).resolve().parents[1]
    projection=repo/'graph-migration/projection/d1_typed_projection.py'
    if b.sha(projection)!=profile['provenance']['projection_sha256']:
        raise ValueError('Projection changed since project preselection')
    sys.path.insert(0,str(repo/'graph-migration'))
    from projection.d1_typed_projection import D1TypedProjection,VERSION
    from models import NormalizedGraph
    from exporters.csv_exporter import CsvExporter
    projector=D1TypedProjection()
    with source.open(encoding='utf-8-sig',newline='') as f:
        graph=projector.project(csv.DictReader(f))
    execution=NormalizedGraph()
    for node in graph.nodes.values():
        if node.properties['query_eligible']:
            execution.add_node(node)
    signatures=set()
    types=Counter()
    for edge in graph.edges:
        if edge.properties['query_eligible']:
            if edge.source_node_uid not in execution.nodes or edge.target_node_uid not in execution.nodes:
                raise ValueError('Eligible edge has missing endpoint')
            execution.add_edge(edge)
            signatures.add((execution.nodes[edge.source_node_uid].label,edge.rel_type,
                            edge.properties['service_rel_type'],execution.nodes[edge.target_node_uid].label))
            types[edge.rel_type]+=1
    actual={'nodes':len(execution.nodes),'edges':len(execution.edges),'typed_signatures':len(signatures)}
    if any(actual[k]!=profile['features'][k] for k in actual):
        raise ValueError('Export differs from prediction-free project profile')
    out.mkdir(parents=True)
    CsvExporter().export(execution,out/'execution_csv')
    report={'mode':'E8_HOLDOUT_PROJECTION_NO_MODEL_PREDICTIONS','project':a.project,
        'projection_version':VERSION,'projection_sha256':b.sha(projection),
        'raw_sha256':b.sha(source),'source_manifest_sha256':b.sha(a.source_manifest),
        'selection_sha256':b.sha(a.selection),'profile_sha256':b.sha(a.profile),
        'counts':actual,'physical_counts':dict(types),
        'execution_csv_hashes':{name:b.sha(out/'execution_csv'/name) for name in ('nodes.csv','edges.csv')},
        'holdout_predictions_seen':0,
        'limits':'Same source year and projection/schema. Preselected graph size is not a query-difficulty measure. Whole source tar archive checksum was not verified.'}
    b.save(out/'projection_receipt.json',report)
    print(json.dumps({'project':a.project,**actual}),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    for key in ('input','output','project','selection','profile','source-manifest'):
        parser.add_argument('--'+key,required=True)
    run(parser.parse_args())
