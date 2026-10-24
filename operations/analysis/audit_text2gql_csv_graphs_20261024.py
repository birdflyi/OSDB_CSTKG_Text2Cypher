"""Offline CSV identity/schema audit. No database or query/model execution.

ISO date checks are diagnostics, not a claim about TuGraph's accepted grammar.
Empty fields are counted without choosing a NULL-vs-empty-string import policy.
"""
from collections import Counter
import csv
from datetime import date, datetime, timezone
import hashlib
import json
import math
from pathlib import Path

ROOT=Path(r'D:\ch7\literature\public_candidate_metadata_20261024\Text2GQL-Bench')


def audit_graph(config_path):
    config=json.loads(config_path.read_text(encoding='utf-8'))
    schemas={row['label']:row for row in config['schema']}
    if len(schemas)!=len(config['schema']):raise ValueError('Repeated schema label')
    vertex_labels={label for label,row in schemas.items() if row['type']=='VERTEX'}
    keys={label:set() for label in vertex_labels}
    vertices=Counter();edges=Counter();property_types=Counter();empty_fields=Counter()
    diagnostics=Counter();details=[];file_receipts=[];edge_pairs=Counter()
    def note(kind,file,line):
        diagnostics[kind]+=1
        if len(details)<25:details.append(dict(kind=kind,file=file,line=line))
    for schema in schemas.values():
        for prop in schema.get('properties',[]):property_types[prop['type']]+=1
    for record_kind in ('VERTEX','EDGE'):
        for file in config['files']:
            label=file['label'];schema=schemas[label]
            if schema['type']!=record_kind:continue
            assert file['format']=='CSV'
            relative=Path(file['path'])
            assert not relative.is_absolute() and '..' not in relative.parts
            path=config_path.parent/relative
            raw=path.read_bytes()
            file_receipts.append(dict(file=file['path'],label=label,bytes=len(raw),
                                      sha256=hashlib.sha256(raw).hexdigest()))
            properties={prop['name']:prop for prop in schema.get('properties',[])}
            with path.open(encoding='utf-8-sig',newline='') as stream:
                reader=csv.reader(stream)
                for index,row in enumerate(reader,1):
                    if index<=file.get('header',0):
                        if len(row)!=len(file['columns']):note('header_width_mismatch',file['path'],index)
                        continue
                    if len(row)!=len(file['columns']):
                        note('row_width_mismatch',file['path'],index)
                        continue
                    values=dict(zip(file['columns'],row))
                    for field,definition in properties.items():
                        if field not in values:
                            note('unmapped_property',file['path'],index);continue
                        value=values[field];kind=definition['type']
                        if value=='':
                            empty_fields[label+'.'+field]+=1
                            if not definition.get('optional',False):note('empty_required_property',file['path'],index)
                            continue
                        try:
                            if kind in ('INT8','INT16','INT32','INT64'):int(value)
                            elif kind in ('FLOAT','DOUBLE'):
                                if not math.isfinite(float(value)):raise ValueError('nonfinite')
                            elif kind=='DATE':date.fromisoformat(value)
                            elif kind=='DATETIME':datetime.fromisoformat(value)
                            elif kind=='BOOL':
                                if value.lower() not in ('true','false','0','1'):raise ValueError('bool')
                            elif kind not in ('STRING','BLOB'):note('unchecked_property_type',file['path'],index)
                        except ValueError:
                            note('lexical_diagnostic_'+kind,file['path'],index)
                    if record_kind=='VERTEX':
                        vertices[label]+=1
                        primary=schema['primary']
                        if primary not in values:note('unmapped_primary',file['path'],index);continue
                        key=values[primary]
                        if key in keys[label]:note('duplicate_label_primary',file['path'],index)
                        keys[label].add(key)
                    else:
                        edges[label]+=1
                        src=file['SRC_ID'];dst=file['DST_ID']
                        if [src,dst] not in schema.get('constraints',[]):note('endpoint_schema_constraint_mismatch',file['path'],index)
                        if values['SRC_ID'] not in keys[src]:note('dangling_source',file['path'],index)
                        if values['DST_ID'] not in keys[dst]:note('dangling_target',file['path'],index)
                        edge_pairs[(src,values['SRC_ID'],label,dst,values['DST_ID'])]+=1
    return dict(graph=config_path.parts[-4],vertex_types=len(vertex_labels),
        edge_types=sum(s['type']=='EDGE' for s in schemas.values()),
        vertex_rows=sum(vertices.values()),edge_rows=sum(edges.values()),
        vertices_by_label=dict(vertices),edges_by_label=dict(edges),
        property_declarations_by_type=dict(property_types),
        edge_property_declarations=sum(len(s.get('properties',[])) for s in schemas.values() if s['type']=='EDGE'),
        repeated_typed_endpoint_pairs=sum(n>1 for n in edge_pairs.values()),
        extra_edge_rows_on_repeated_typed_pairs=sum(n-1 for n in edge_pairs.values() if n>1),
        empty_property_fields=sum(empty_fields.values()),empty_fields_by_property=dict(empty_fields),
        diagnostics=dict(diagnostics),first_diagnostics=details,file_receipts=file_receipts,
        config_sha256=hashlib.sha256(config_path.read_bytes()).hexdigest())


if __name__=='__main__':
    root=ROOT/'selected_zip_assets_20261024/dataset/test'
    graphs=[audit_graph(path) for path in sorted(root.glob('*/Cypher/TuGraph-DB_Instance/import_config.json'))]
    assert len(graphs)==6
    result=dict(kind='OFFLINE_NATIVE_CSV_STRUCTURE_AUDIT',timestamp_utc=datetime.now(timezone.utc).isoformat(),
        graphs=graphs,source_executed=False,model_calls=0,database_imported=False,
        full_engine_semantics_certified=False,
        caveats=['lexical ISO date diagnostics do not establish TuGraph rejection',
                 'empty fields counted; NULL versus empty-string policy still needs native engine verification',
                 'repeated edge endpoints are counted, never silently collapsed',
                 'no test questions or reference queries used to tune methods'])
    (ROOT/'offline_csv_graph_audit_20261024.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps([dict(graph=g['graph'],nodes=g['vertex_rows'],edges=g['edge_rows'],
        vertex_types=g['vertex_types'],edge_types=g['edge_types'],edge_properties=g['edge_property_declarations'],
        repeated_pairs=g['repeated_typed_endpoint_pairs'],empty_property_fields=g['empty_property_fields'],
        diagnostics=g['diagnostics']) for g in graphs]))
