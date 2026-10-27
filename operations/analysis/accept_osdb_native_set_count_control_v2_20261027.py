"""Finite real Neo4j acceptance of the new same-plan native count control.

One NEW loopback-only synthetic store; no model/SSH, no old-store mutation,
no method tuning, no automatic statement retries and no performance claim.
Fixtures and 36 plans x three strategies are sealed before the first boot.
"""
from copy import deepcopy
from pathlib import Path
import hashlib, json, os, shutil, socket, subprocess, sys, threading, time
sys.path.insert(0, 'D:/ch7/tools/python_deps/neo4j_5_15')
import requests
from neo4j import GraphDatabase
from accept_osdb_actor_graph_references_20261026 import stop_owned, live, budget, runtime, now
from observed_profile_semantic_cases import PEOPLE, EDGES, SCHEMA, call
from osdb_model_read_executor_dev_v2 import ModelReadExecutor
from osdb_shared_read_executor_dev import digest
from osdb_native_set_count_control_dev import compile_native_set_count
from osdb_independent_path_cost_dev import PathCosts
import observed_neo4j_services_dev_v2 as compiler

ROOT = Path('D:/ch7')
REPO = ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
CANON = ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
HOME = Path('D:/Program Files (x86)/neo4j-community-5.15.0')
OUT = ROOT/'reports/osdb_native_set_count_engine_v2_20261027'
HTTP, BOLT = 18521, 18522


def read(p): return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def write(p, value):
    p = Path(p); p.parent.mkdir(parents=True, exist_ok=True)
    with p.open('x', encoding='utf-8', newline='\n') as f:
        json.dump(value, f, ensure_ascii=False, indent=2, allow_nan=False); f.write('\n')


def members(keys, label='Person', identity='key'):
    c = call(); c['nodes'][0]['label'] = label
    c['where'] = [{'field': 'n.'+identity, 'op': 'in', 'value': keys}]
    c['returns'] = [{'field': 'n.'+identity, 'alias': 'id'}]
    return c


def fixed_cases():
    # Independent expected entity sets, not outputs of the tested compiler.
    families = []
    def add(name, left, right, a, b): families.append((name, left, right, set(a), set(b)))
    for name, a, b in [
        ('empty_both', [], []), ('empty_left', [], ['a']), ('empty_right', ['b'], []),
        ('disjoint', ['a', 'b'], ['c', 'd']), ('overlap', ['a', 'b', 'c'], ['b', 'd']),
        ('identical', ['a', 'b'], ['a', 'b'])]:
        add(name, members(a), members(b), a, b)
    duplicate = call(); duplicate['nodes'].append({'id': 'm', 'label': 'Person'})
    duplicate['edges'] = [{'id': 'r', 'source': 'n', 'target': 'm', 'label': 'LINK'}]
    duplicate['where'] = [{'field': 'n.key', 'op': 'eq', 'value': 'a'}]
    duplicate['returns'] = [{'field': 'm.key', 'alias': 'id'}]
    add('duplicate_physical_edges', duplicate, members(['b', 'c']), ['b'], ['b', 'c'])
    negated = call(); negated['where'] = [{'field': 'n.eligible', 'op': 'eq', 'value': False, 'negate': True}]
    add('null_negation', negated, members(['a', 'b']), ['a', 'e', 'f'], ['a', 'b'])
    a, b = call(), call()
    a['where'] = [{'field': 'n.clock', 'op': 'eq', 'value': '2023-07-01T00:00:00Z'}]
    b['where'] = [{'field': 'n.clock', 'op': 'eq', 'value': '2023-07-01T01:00:00+01:00'}]
    add('same_instant_different_spelling', a, b, ['a', 'b'], ['a', 'b'])
    a, b = call(), call()
    a['where'] = [{'field': 'n.clock', 'op': 'ge', 'value': '2023-07-01T00:00:00Z'},
                  {'field': 'n.clock', 'op': 'lt', 'value': '2023-07-01T00:00:00.000000001Z'}]
    b['where'] = [{'field': 'n.clock', 'op': 'ge', 'value': '2023-07-01T00:00:00.000000001Z'},
                  {'field': 'n.clock', 'op': 'lt', 'value': '2023-07-01T00:00:00.1Z'}]
    add('nanosecond_half_open', a, b, ['a', 'b'], ['e'])
    a, b = call(), call()
    a['where'] = [{'field': 'n.display', 'op': 'eq', 'value': 'same'}]
    b['where'] = [{'field': 'n.display', 'op': 'eq', 'value': 'other'}]
    add('certifiable_scalar_partition', a, b, ['a', 'b'], ['c'])
    add('integer_identity', members([1, 2], 'NumberPerson', 'number'),
        members([2, 3], 'NumberPerson', 'number'), [1, 2], [2, 3])
    tasks = []
    for family, left, right, a, b in families:
        counts = {'union': len(a | b), 'intersection': len(a & b), 'difference': len(a - b)}
        for op in ('union', 'intersection', 'difference'):
            plan = {'status': 'ANSWER', 'calls': [deepcopy(left), deepcopy(right)],
                    'combine': {'op': op, 'key': 'id', 'output': 'count', 'alias': 'answer'}}
            tasks.append({'name': family+'_'+op, 'family': family, 'plan': plan,
                          'expected_rows': [[counts[op]]], 'expected_columns': ['answer']})
    assert len(families) == 12 and len(tasks) == 36
    return tasks


def main():
    assert not OUT.exists(), 'Preserve all previous partial evidence'
    assert not live(HTTP) and not live(BOLT)
    assert shutil.disk_usage('D:/').free > 1024**3
    before_budget, before_runtime = budget(), runtime()
    assert not before_budget['pending']
    freeze = read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
    assert all(sha(CANON/n) == h for n, h in freeze.items())
    # Generic compiler schema includes integer identity; do not widen OSDB public input.
    schema = deepcopy(SCHEMA); tasks = fixed_cases()
    prepared = [dict(t, native_program=compile_native_set_count(t['plan'], schema)) for t in tasks]
    sources = [Path(__file__)] + [Path(__file__).with_name(n) for n in (
        'osdb_native_set_count_control_dev.py', 'observed_neo4j_services_dev_v2.py',
        'osdb_model_read_executor_dev_v2.py', 'osdb_shared_read_executor_dev.py',
        'observed_profile_semantic_cases.py', 'osdb_independent_path_cost_dev.py',
        'osdb_common_input.py', 'osdb_common_input_v2.py', 'osdb_response_boundary_dev.py',
        'osdb_budgeted_model_client_dev.py', 'osdb_scoped_attempts_dev.py',
        'accept_osdb_actor_graph_references_20261026.py')]
    OUT.mkdir(); (OUT/'executed_sources').mkdir()
    for p in sources: shutil.copyfile(p, OUT/'executed_sources'/p.name)
    strategies = ('contract', 'full_detail', 'native')
    write(OUT/'protocol.json', {'sealed_local': now(), 'kind': 'SYNTHETIC_ENGINE_ACCEPTANCE_NOT_FORMAL_EX',
        'source_sha256': {p.name: sha(p) for p in sources}, 'schema': schema,
        'fixture': {'people': PEOPLE, 'integer_people': [1, 2, 3], 'edges': EDGES},
        'cases': prepared, 'plans': 36, 'expected_strategy_checks': 108,
        'strategy_order': [list(strategies[i % 3:]+strategies[:i % 3]) for i in range(36)],
        'query_limits': {'explain_seconds': 30, 'read_seconds': 60, 'rows': 1000000, 'bytes': 64*1024*1024},
        'readiness': {'attempts_max': 3, 'interval_seconds_min': 10},
        'model_requests': 0, 'query_retry': False, 'training': 0, 'SSH': 0,
        'budget_before': before_budget, 'adapter_before': before_runtime, 'frozen': freeze,
        'performance_comparison': False, 'existing_store_mutation': False})
    for folder in ('conf', 'data', 'logs', 'run', 'plugins'): (OUT/folder).mkdir()
    config = ['server.default_listen_address=127.0.0.1', f'server.http.listen_address=127.0.0.1:{HTTP}',
        'server.http.enabled=true', 'server.https.enabled=false', 'server.bolt.enabled=true',
        f'server.bolt.listen_address=127.0.0.1:{BOLT}', f'server.bolt.advertised_address=127.0.0.1:{BOLT}',
        'dbms.security.auth_enabled=false', 'dbms.default_database=neo4j',
        'server.memory.heap.initial_size=256m', 'server.memory.heap.max_size=512m',
        'server.memory.pagecache.size=128m', 'dbms.security.allow_csv_import_from_file_urls=false',
        'server.jvm.additional=-XX:ActiveProcessorCount=2']
    for folder in ('data', 'logs', 'run', 'plugins'):
        config.append('server.directories.'+folder+'='+str(OUT/folder).replace('\\', '/'))
    (OUT/'conf/neo4j.conf').write_text('\n'.join(config)+'\n', encoding='utf-8')
    launcher = OUT/'launch_owned.ps1'
    launcher.write_text("& '"+str(HOME/'bin/neo4j.bat')+"' console\n", encoding='utf-8')
    session = requests.Session(); session.trust_env = False
    http_count, get_count, results, executors = 0, 0, [], []
    failure, driver, executor = None, None, None
    def setup(query, params):
        nonlocal http_count
        http_count += 1; folder = OUT/'trusted_setup'/f'{http_count:04d}'
        payload = {'statements': [{'statement': query, 'parameters': params, 'resultDataContents': ['row']}]}
        write(folder/'request.json', payload); started = time.monotonic(); terminal = {}
        try:
            response = session.post(f'http://127.0.0.1:{HTTP}/db/neo4j/tx/commit', json=payload, timeout=(3, 30))
            terminal.update(http_status=response.status_code, body=response.json())
            assert response.status_code == 200 and not terminal['body']['errors']
            return [x['row'] for x in terminal['body']['results'][0]['data']]
        except Exception as exc:
            terminal.update(error_type=type(exc).__name__); raise
        finally:
            terminal['client_seconds'] = time.monotonic()-started; write(folder/'terminal.json', terminal)
    stdout = (OUT/'console_stdout.log').open('wb'); stderr = (OUT/'console_stderr.log').open('wb')
    process = subprocess.Popen(['powershell.exe', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', str(launcher)],
        env=dict(os.environ, NEO4J_CONF=str(OUT/'conf'), NEO4J_HOME=str(HOME)), stdout=stdout, stderr=stderr,
        creationflags=subprocess.CREATE_NO_WINDOW)
    write(OUT/'execution_owner.json', {'hostname': socket.gethostname(), 'wrapper_pid': process.pid,
          'config': str(OUT/'conf'), 'ports': [HTTP, BOLT], 'scope': 'NEW local native-control synthetic acceptance only'})
    try:
        ready = False
        for index in range(3):
            time.sleep(12 if index == 0 else 10); get_count += 1; receipt = {'attempt': index+1}
            try:
                response = session.get(f'http://127.0.0.1:{HTTP}/', timeout=(2, 3))
                receipt['http_status'] = response.status_code; ready = response.status_code == 200
            except requests.RequestException as exc: receipt['error_type'] = type(exc).__name__
            write(OUT/f'readiness_{index+1}.json', receipt)
            if ready or process.poll() is not None: break
        assert ready, 'Readiness failed; no blind restart'
        assert setup('MATCH(n) RETURN count(n)', {}) == [[0]]
        setup('UNWIND $rows AS p CREATE(n:Person) SET n=p', {'rows': PEOPLE})
        setup('UNWIND $rows AS p CREATE(n:NumberPerson {number:p})', {'rows': [1, 2, 3]})
        setup('UNWIND $rows AS e MATCH(a:Person {key:e.a}),(b:Person {key:e.b}) '
              'CREATE(a)-[r:LINK]->(b) SET r.physical_key=e.key,r.clock=e.clock', {'rows': EDGES})
        driver = GraphDatabase.driver(f'bolt://127.0.0.1:{BOLT}', auth=None, connection_timeout=3,
                                     max_transaction_retry_time=0, max_connection_pool_size=1)
        executor = ModelReadExecutor(driver, schema, OUT/'reads'); executors.append(executor)
        receipt = executor.admit()
        hashes = [digest(['node', 'Person', p]) for p in PEOPLE]
        hashes += [digest(['node', 'NumberPerson', {'number': n}]) for n in (1, 2, 3)]
        hashes += [digest(['edge', 'LINK', ('Person', e['a']), ('Person', e['b']),
                          {'physical_key': e['key'], 'clock': e['clock']}]) for e in EDGES]
        assert receipt['semantic_snapshot_sha256'] == hashlib.sha256(''.join(sorted(hashes)).encode()).hexdigest()
        lock = threading.RLock()
        for index, task in enumerate(prepared):
            order = strategies[index % 3:]+strategies[:index % 3]
            for strategy in order:
                costs = PathCosts(task['name']+'__'+strategy)
                def execute(query, params):
                    return costs.read(executor.execute, query, params, lock, lambda: executor.number)
                with costs.span('fixed_correct_plan_execution', strategy):
                    if strategy == 'native':
                        program = task['native_program']
                        result = costs.measure('answer_execution', execute, program['query'], program['parameters'])
                    else:
                        result = costs.measure('answer_execution', compiler.execute_plan,
                                               task['plan'], schema, execute, strategy=strategy)
                complete = result.get('complete') if 'complete' in result else (
                    bool(result.get('observations')) and all(x.get('complete') is True for x in result['observations']))
                passed = (result['status'] == 'EXECUTED' and complete is True and
                          result['columns'] == task['expected_columns'] and result['rows'] == task['expected_rows'] and
                          type(result['rows'][0][0]) is int)
                record = {'name': task['name'], 'strategy': strategy, 'passed': passed, 'result': result,
                          'expected_rows': task['expected_rows'], 'costs': costs.snapshot(),
                          'formal_EX': False, 'performance_result': False}
                write(OUT/'checks'/(task['name']+'__'+strategy+'.json'), record)
                results.append({'name': task['name'], 'strategy': strategy, 'passed': passed,
                                'representation': result.get('representation')})
                assert passed, (task['name'], strategy)
    except Exception as exc:
        failure = {'type': type(exc).__name__, 'message': str(exc)}; write(OUT/'failure.json', failure)
    finally:
        if driver: driver.close()
        stopped = stop_owned(process, OUT/'conf', HTTP)
        stopped['bolt_port_closed'] = not live(BOLT); assert stopped['bolt_port_closed']
        write(OUT/'shutdown_receipt.json', stopped); session.close(); stdout.close(); stderr.close()
    after_budget = budget()
    assert before_budget == after_budget and before_runtime == runtime()
    assert all(sha(CANON/n) == h for n, h in freeze.items())
    terminals = [read(p) for executor in executors for p in executor.folder.glob('*/terminal.json')]
    summary = {'kind': 'EXPOSED_SYNTHETIC_NATIVE_CONTROL_ENGINE_ACCEPTANCE_NOT_MODEL_RESULT',
        'planned_plans': 36, 'planned_strategy_checks': 108, 'completed_strategy_checks': len(results),
        'passed_strategy_checks': sum(x['passed'] for x in results),
        'failed_strategy_checks': sum(not x['passed'] for x in results), 'undispatched_strategy_checks': 108-len(results),
        'results': results, 'failure': failure, 'physical_Bolt_statements': len(terminals),
        'Bolt_client_seconds': sum(x['client_seconds'] for x in terminals),
        'readiness_HTTP': get_count, 'trusted_setup_HTTP_POST': http_count,
        'model_calls': 0, 'SSH': 0, 'training': 0, 'automatic_statement_retry': False,
        'budget_before': before_budget, 'budget_after': after_budget, 'source_files_unchanged': all(sha(p) == read(OUT/'protocol.json')['source_sha256'][p.name] for p in sources),
        'new_owned_JVM_stopped': True, 'canonical32_adapter46_unchanged': True,
        'native_control_entire_IR_accepted': False, 'true_four_graph_factory_accepted': False,
        'performance_comparison_accepted': False, 'formal_admission': False, 'T1_complete': False}
    write(OUT/'summary.json', summary)
    print(json.dumps({k:v for k,v in summary.items() if k!='results'}, ensure_ascii=True), flush=True)
    if failure: raise RuntimeError('Acceptance stopped; preserve failed evidence before any new version')


if __name__ == '__main__': main()
