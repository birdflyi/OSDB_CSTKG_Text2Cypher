"""Compare preserved first-graph native projections with CSV numeric bits."""
from pathlib import Path
from datetime import datetime,date,timezone
import ast,csv,json,math,struct,re

def native_decimal(text):
    # Exact operation order in pinned fma-common/text_parser.h:521-556.
    m=re.fullmatch(r'([+-]?)([0-9]+)(?:\.([0-9]*))?(?:[eE]([+-]?[0-9]+))?',text)
    assert m
    value=0.0
    for digit in m[2]:value=value*10+int(digit)
    tail=0.0;mult=.1
    for digit in m[3] or '':tail=tail+mult*int(digit);mult*=.1
    value+=tail
    if m[4]:value*=math.pow(10,int(m[4]))
    return -value if m[1]=='-' else value

source=Path('D:/ch7/reports/text2gql_reference_graph_worker_20261026.py')
tree=ast.parse(source.read_text(encoding='utf-8'))
functions=[x for x in tree.body if isinstance(x,ast.FunctionDef) and x.name in ['canonical','native_decimal']]
namespace={'date':date,'datetime':datetime,'timezone':timezone,'struct':struct,'re':re,'math':math}
exec(compile(ast.fix_missing_locations(ast.Module(body=functions,type_ignores=[])),str(source),'exec'),namespace)
canonical=namespace['canonical']
base=Path('D:/ch7/literature/public_candidate_metadata_20261024/Text2GQL-Bench/selected_zip_assets_20261024/dataset/test/Manufacturing_Production_Process/Cypher/TuGraph-DB_Instance')
conf=json.loads((base/'import_config.json').read_text(encoding='utf-8'));schemas={x['label']:x for x in conf['schema']}
responses=Path('D:/ch7/reports/text2gql_native_reference_graph_00_v2_20261026/responses')
all_diff=[]
for label,index in [('MaterialBatch',3),('REQUIRES',10),('CONSUMES',11)]:
    schema=schemas[label];props=schema['properties'];f=next(x for x in conf['files'] if x['label']==label)
    with (base/f['path']).open(encoding='utf-8-sig',newline='') as inp:
        rows=list(csv.reader(inp))[f['header']:]
    expected={tuple(row[:1 if schema['type']=='VERTEX' else 2]):dict(zip(f['columns'],row)) for row in rows}
    actual=json.loads((responses/('identity_'+str(index)+'.json')).read_text(encoding='utf-8'))['result']
    for row in actual:
        keys=1 if schema['type']=='VERTEX' else 2;key=tuple(row[:keys]);src=expected[key]
        for n,prop in enumerate(props):
            offset=n if schema['type']=='VERTEX' else n+2
            raw=src[prop['name']];e=None if raw=='' else raw;a=row[offset]
            if canonical(e,prop['type'],True)!=canonical(a,prop['type'],True):
                d={'label':label,'key':key,'property':prop['name'],'kind':prop['type'],'source':raw,'actual':a}
                if prop['type']=='DOUBLE' and e is not None and a is not None:
                    want=float(e);native=native_decimal(e);d.update({'expected_hex':want.hex(),'actual_hex':float(a).hex(),
                        'absolute_difference':abs(want-float(a)),'ulps':abs(want-float(a))/math.ulp(want)})
                    d['native_parser_hex']=native.hex();d['matches_native_parser_exactly']=native.hex()==float(a).hex()
                all_diff.append(d)
result={'differences':all_diff,'count':len(all_diff),'max_ulps':max(x.get('ulps',0) for x in all_diff),
        'all_differences_explained_by_pinned_parser_exact_bits':all(x.get('matches_native_parser_exactly',False) for x in all_diff),
        'no_scoring_changed':True,'new_model_calls':0,'new_database_queries':0}
out=responses.parent/'numeric_identity_diagnostic.json'
out.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'count':len(all_diff),'first':all_diff[:3],'max_ulps':result['max_ulps'],
                  'all_match_native_parser':result['all_differences_explained_by_pinned_parser_exact_bits']},ensure_ascii=True))
