"""Offline conformance of two pinned author comparator implementations.

Execute only reviewed AST subsets with fabricated rows and a local fake driver.
No author imports, pipeline, database, network, model, or recovery code runs.
"""
import ast
import argparse
from collections import Counter
from decimal import Decimal
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--cache', type=Path,
                    default=Path('D:/ch7/reports/text2gql_driver_contract_20261026'))
BASE = parser.parse_args().cache


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class FakeDriver:
    def __init__(self, answers):
        self.answers = answers
        self.calls = []

    def query(self, query, db_name='default'):
        self.calls.append({'query':query,'db_name':db_name})
        return self.answers[query]


def load_reviewed_subset(path, class_name, methods):
    original = ast.parse(path.read_text(encoding='utf-8'))
    source_class = next(x for x in original.body if isinstance(x,ast.ClassDef) and x.name==class_name)
    selected = [x for x in source_class.body if isinstance(x,ast.FunctionDef) and x.name in methods]
    assert {x.name for x in selected} == set(methods)
    # No author module imports or top-level initialization; no standalone
    # evaluator constructor/queries/docker/recovery code is compiled.
    blocked_names = {'open','eval','exec','compile','__import__','globals','locals'}
    for method in selected:
        for node in ast.walk(method):
            assert not isinstance(node,(ast.Import,ast.ImportFrom,ast.Global,ast.Nonlocal))
            if isinstance(node,ast.Call) and isinstance(node.func,ast.Name):
                assert node.func.id not in blocked_names
    isolated = ast.ClassDef(name=class_name,bases=[],keywords=[],body=selected,decorator_list=[])
    module = ast.fix_missing_locations(ast.Module(body=[isolated],type_ignores=[]))
    namespace = {'math':math,'Decimal':Decimal,'Counter':Counter,'DatabaseDriver':FakeDriver}
    exec(compile(module,str(path),'exec'),namespace)
    return namespace[class_name], [{'method':m.name,'line':m.lineno,'end_line':m.end_lineno} for m in selected]


def main():
    receipts = json.loads((BASE/'source_receipts.json').read_text(encoding='utf-8'))
    for item in receipts:
        path = BASE/'upstream'/item['path']
        assert sha(path)==item['sha256']
        raw=path.read_bytes()
        assert hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()==item['git_blob_sha']
    oop_path = BASE/'upstream/impl/evaluation/metrics.py'
    ref_path = BASE/'upstream/contrib/standalone_text2graph_scripts/reference/ea_cypher.py'
    OOP,oop_methods = load_reviewed_subset(oop_path,'ExecutionAccuracy',
                                        ['__init__','_normalize','_compare_results','compute'])
    Reference,reference_methods = load_reviewed_subset(ref_path,'ExecutionEvaluator',['compare_results'])
    oop = OOP(FakeDriver({})); reference = Reference()
    checks=[]
    cases=[
        ('unordered_row_permutation',[{'x':1},{'x':2}],[{'x':2},{'x':1}],
         'MATCH (n) RETURN n.x',True,True),
        ('duplicate_multiplicity',[{'x':1},{'x':1}],[{'x':1}],
         'MATCH (n) RETURN n.x',False,True),
        ('ordered_row_permutation',[{'x':1},{'x':2}],[{'x':2},{'x':1}],
         'MATCH (n) RETURN n.x ORDER BY n.x',False,True),
        ('ordering_marker_removed',[{'x':1},{'x':2}],[{'x':2},{'x':1}],
         'MATCH (n) RETURN n.x',True,True),
        ('order_by_inside_string',[{'x':1},{'x':2}],[{'x':2},{'x':1}],
         "MATCH (n) WHERE n.label='order by' RETURN n.x",False,True),
        ('nine_decimal_rounding',[{'x':1.00000000001}],[{'x':1.0}],
         'RETURN 1.0',True,True),
        ('boolean_integer_python_equality',[{'x':True}],[{'x':1}],
         'RETURN 1',True,True),
        ('empty_results',[],[],'MATCH (n) RETURN n.x',True,True),
    ]
    for name,gold,pred,query,want_oop,want_ref in cases:
        observed = [oop._compare_results(gold,pred,query),reference.compare_results(gold,pred)]
        assert observed == [want_oop,want_ref], name
        checks.append({'case':name,'oop':observed[0],'standalone_reference':observed[1]})
    driver=FakeDriver({'gold':[{'x':1}],'good':[{'x':1}],'bad':[{'x':2}],'error':None,'empty':[]})
    metric=OOP(driver)
    batch = metric.compute(['good','bad'],['gold','gold'])
    assert batch==0.5
    checks.append({'case':'batch_average','observed':batch})
    driver.calls.clear()
    assert metric.compute(['good'],['gold','gold']) == 1.0
    assert len(driver.calls)==2 and all(x['db_name']=='default' for x in driver.calls)
    checks.append({'case':'unequal_input_lengths_zip_truncates','observed_score':1.0,
                   'paired_examples_evaluated':1,'declared_gold_examples':2,
                   'default_database':'default'})
    driver.calls.clear()
    assert metric.compute(['error','empty'],['gold','empty'],db_id='explicit_graph')==0.5
    assert all(x['db_name']=='explicit_graph' for x in driver.calls)
    checks.append({'case':'error_vs_valid_empty_and_explicit_database','observed_score':0.5})
    pipeline_path=BASE/'upstream/run_pipeline.py'
    pipeline=ast.parse(pipeline_path.read_text(encoding='utf-8'))
    runner=next(x for x in pipeline.body if isinstance(x,ast.ClassDef) and x.name=='PipelineRunner')
    details=next(x for x in runner.body if isinstance(x,ast.FunctionDef) and x.name=='_save_detailed_results')
    assignments=[]
    for node in ast.walk(details):
        if isinstance(node,ast.Dict):
            for key,value in zip(node.keys,node.values):
                if isinstance(key,ast.Constant) and key.value=='ea' and isinstance(value,ast.Name):
                    assignments.append({'line':key.lineno,'value_variable':value.id})
    assert assignments and all(x['value_variable']=='ea' for x in assignments)
    result={'kind':'PINNED_AUTHOR_SCORING_CONFORMANCE_NOT_BENCHMARK_RESULTS',
            'time_local':datetime.now().astimezone().isoformat(),
            'repository':json.loads((BASE/'manifest.json').read_text(encoding='utf-8'))['canonical_repository'],
            'commit':json.loads((BASE/'manifest.json').read_text(encoding='utf-8'))['commit'],
            'executed_author_subset':True,'full_author_pipeline_executed':False,
            'author_imports_or_recovery_executed':False,'new_model_calls':0,'new_database_queries':0,
            'source_methods':{'oop':oop_methods,'standalone_reference':reference_methods},
            'source_sha256':{'oop':sha(oop_path),'standalone_reference':sha(ref_path),'pipeline':sha(pipeline_path)},
            'synthetic_checks':checks,'checks_passed':len(checks),
            'static_per_record_ea_assignments':assignments,
            'formal_scoring_contract_frozen':False,'actual_reference_answers_verified':False,
            'limits':['Fabricated rows and stub query responses only, no actual database semantics verified.',
                      'Current pinned driver implementations differ; paper-reported version not established.',
                      'Nine-decimal normalization is a contract, not automatically a defect.',
                      'Ordering detection is a query-text heuristic; dataset impact remains unmeasured.',
                      'Do not change the already completed CypherBench metric or use these checks as EX gains.']}
    (BASE/'comparator_conformance.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result,ensure_ascii=True))


if __name__=='__main__':
    main()
