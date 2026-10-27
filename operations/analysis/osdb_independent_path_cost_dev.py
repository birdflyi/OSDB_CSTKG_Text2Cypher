"""Independent DEVELOPMENT method/control timing; no probes or launch CLI.

Times are client walls under observed contention, not server compute or
standalone latency. No per-token/per-row sampling and no display callbacks.
The old path files, scheduling module and approved messages are unchanged.
"""
from contextlib import contextmanager
from copy import deepcopy
import threading
import time
from osdb_budgeted_model_client_dev import outcome
from osdb_response_boundary_dev import feedback_messages, execute_response
from osdb_scoped_attempts_dev import immutable

VERSION = 'osdb-independent-path-cost-dev-1'


class PathCosts:
    def __init__(self, path_key, clock=time.monotonic):
        self.path_key = path_key
        self.clock = clock
        self.started = clock()
        self.entries = []
        self.local = threading.local()
        self.mutex = threading.Lock()

    @contextmanager
    def span(self, component, group=None):
        stack = getattr(self.local, 'stack', [])
        self.local.stack = stack
        with self.mutex:
            parent = stack[-1] if stack else None
            if group is None:
                if parent is None:
                    raise ValueError('A root cost span requires an explicit group')
                group = self.entries[parent]['group']
            elif parent is not None and group != self.entries[parent]['group']:
                raise ValueError('Cost groups must use separate root spans')
            index = len(self.entries)
            self.entries.append({'id': index, 'parent_id': parent, 'component': component,
                                 'group': group, 'started': self.clock(), 'ended': None,
                                 'status': 'OPEN'})
        stack.append(index)
        try:
            yield index
        except BaseException as exc:
            with self.mutex:
                self.entries[index].update(status='ERROR', error_type=type(exc).__name__)
            raise
        else:
            with self.mutex:
                self.entries[index]['status'] = 'FINISHED'
        finally:
            with self.mutex:
                self.entries[index]['ended'] = self.clock()
            assert stack.pop() == index

    def measure(self, component, function, *args, **kwargs):
        with self.span(component):
            return function(*args, **kwargs)

    def read(self, execute, query, params, lock, statement_count=None):
        """Own the one shared read lock; never wrap an already locked callback.

        The factory may supply a read-only counter of physical statements.
        Without it counts are unknown; one callback is not one DB statement.
        """
        with self.span('read_lock_wait'):
            acquired = lock.acquire()
        if acquired is not True:
            raise RuntimeError('Shared graph lock acquisition failed')
        try:
            def sample_count():
                # A factory-local counter only: never perform I/O to measure I/O.
                try:
                    value = statement_count() if statement_count else None
                except Exception:
                    return None
                return value if type(value) is int and value >= 0 else None
            before = sample_count()
            with self.span('db_executor_client_wall') as index:
                try:
                    result = execute(query, params)
                finally:
                    after = sample_count()
                    count = (after - before if type(before) is int and type(after) is int
                             and after >= before else None)
                    with self.mutex:
                        self.entries[index]['physical_statements'] = count
                if type(result) is dict:
                    with self.mutex:
                        self.entries[index].update(
                            returned_rows=len(result['rows']) if type(result.get('rows')) is list else None,
                            result_bytes=result.get('result_bytes'),
                            returned_complete=result.get('complete'))
                return result
        finally:
            lock.release()

    def snapshot(self):
        with self.mutex:
            entries = deepcopy(self.entries)
        for item in entries:
            end = item.pop('ended')
            start = item.pop('started')
            item['started_offset_seconds'] = start - self.started
            item['inclusive_seconds'] = None if end is None else end - start
        for item in entries:
            children = [r['inclusive_seconds'] for r in entries if r['parent_id'] == item['id']]
            item['exclusive_seconds'] = (None if item['inclusive_seconds'] is None or None in children
                                         else max(0.0, item['inclusive_seconds'] - sum(children)))
        def total(rows, field):
            values = [r.get(field) for r in rows]
            return None if None in values else sum(values)
        groups = {}
        for group in sorted({r['group'] for r in entries}):
            rows = [r for r in entries if r['group'] == group]
            reads = [r for r in rows if r['component'] == 'db_executor_client_wall']
            select = lambda name: [r for r in rows if r['component'] == name]
            groups[group] = {
                'root_wall_seconds': total([r for r in rows if r['parent_id'] is None], 'inclusive_seconds'),
                'model_client_generate_wall_seconds': total(select('model_client_generate'), 'inclusive_seconds'),
                'read_lock_wait_seconds': total(select('read_lock_wait'), 'inclusive_seconds'),
                'db_executor_client_wall_seconds': total(reads, 'inclusive_seconds'),
                'answer_processing_excluding_read_seconds': total(select('answer_execution'), 'exclusive_seconds'),
                'read_callbacks': len(reads),
                'physical_statements': total(reads, 'physical_statements'),
                'returned_rows': total(reads, 'returned_rows'),
                'result_bytes': total(reads, 'result_bytes'),
                'exclusive_accounted_seconds': total(rows, 'exclusive_seconds'),
            }
        return {'version': VERSION, 'path_key': self.path_key, 'groups': groups, 'spans': entries,
                'method_path_wall_seconds': groups.get('method', {}).get('root_wall_seconds'),
                'server_compute_seconds': None, 'time_to_first_token_seconds': None,
                'pure_decode_tokens_per_second': None, 'cache_state': None,
                'independent_deployment_costs_established': False,
                'clock_kind': 'client_monotonic', 'formal_admission': False}


def timed_scheduled_path(client, key, arm, schema, execute, costs):
    """Existing one-repair policy; stop method clock BEFORE control replay.

    execute must acquire the shared read lock through this same PathCosts.
    Cost records concern observed deployment, including contention. Shared
    controls still need planning for a separately deployed logical strategy.
    """
    if arm not in ('direct', 'ours') or not key.endswith('__' + arm) or costs.path_key != key:
        raise ValueError('Method/cost identity mismatch')
    start = time.monotonic()
    try:
        with costs.span('method_path', 'method'):
            first = costs.measure('model_client_generate', client.generate, key, 'initial')
            records = [first]
            first_result = costs.measure('answer_execution', outcome, arm, first, schema, execute)
            result, final, withheld = first_result, first, False
            if not first.get('transport_error') and result['status'] == 'INFRASTRUCTURE_ERROR':
                client.stop_for_unknown_read(key)
            if feedback_messages(client.initial[key], first.get('content') or '', result) is not None:
                if client.lease.stopped:
                    withheld = True
                else:
                    try:
                        final = costs.measure('model_client_generate', client.generate, key, 'feedback', result)
                    except RuntimeError:
                        if client.lease.stopped and key + '__feedback' not in client.lease.reserved:
                            withheld = True
                        else:
                            raise
                    else:
                        records.append(final)
                        result = costs.measure('answer_execution', outcome, arm, final, schema, execute)
                        if not final.get('transport_error') and result['status'] == 'INFRASTRUCTURE_ERROR':
                            client.stop_for_unknown_read(key)
        method_ended_epoch = time.time()
        # A later control failure must not erase the completed method outcome.
        immutable(client.lease.directory / 'method_finals' / (key + '.json'), {
            'version': VERSION, 'path_key': key, 'arm': arm, 'first_result': first_result,
            'final': result, 'records': records, 'method_ended_epoch': method_ended_epoch,
            'feedback_withheld_due_to_batch_stop': withheld,
            'costs': costs.snapshot(), 'formal_prediction': False,
            'method_outcome_saved_before_controls': True})
        controls = {}
        if arm == 'ours' and result['status'] == 'EXECUTED' and result.get('complete') is True:
            with costs.span('fixed_plan_replay', 'full_detail_control'):
                detailed = costs.measure('answer_execution', execute_response,
                                         'ours', final['content'], schema, execute, strategy='full_detail')
                if detailed['status'] == 'INFRASTRUCTURE_ERROR':
                    client.stop_for_unknown_read(key)
            controls = {
                'full_detail': {'result': detailed, 'shared_planner': True, 'extra_model_calls': 0},
                'simple_correct': {'same_result_as': 'ours_final', 'shared_planner': True,
                                   'extra_model_calls': 0, 'extra_database_execution': False,
                                   'reason': 'Current static safe-count/full-identity rule equals contract policy'},
            }
        saved = {'version': VERSION, 'path_key': key, 'arm': arm, 'first_result': first_result,
                 'final': result, 'records': records, 'shared_plan_controls': controls,
                 'feedback_withheld_due_to_batch_stop': withheld,
                 'attached_ledger_attempts': len(records), 'formal_prediction': False,
                 'method_ended_epoch': method_ended_epoch,
                 'evaluator_reference_read_by_this_function': False,
                 'path_wall_seconds': time.monotonic() - start,
                 'path_wall_scope': 'orchestration_including_control_not_independent_method_latency',
                 'costs': costs.snapshot()}
        immutable(client.lease.directory / 'paths' / (key + '.json'), saved)
        return saved
    finally:
        # Unknown/failed paths retain cost error spans; no retry or fake zero.
        immutable(client.lease.directory / 'costs' / (key + '.json'), costs.snapshot())
