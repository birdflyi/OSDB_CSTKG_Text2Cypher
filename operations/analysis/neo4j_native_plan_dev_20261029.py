"""Native entity-set execution component; preparation, not benchmark admission.

Consumes an ANSWER IR and an already owned NativeReadCapture. All model I/O,
graph lifecycle, immutable-store proof and primary scoring remain external.
Controls share the same plan; simple_correct deliberately uses full_detail.
"""

from copy import deepcopy
from dataclasses import asdict
import hashlib
import os
from pathlib import Path
import time

from neo4j_native_answer_contract_dev_20261029 import (
    PROFILE as ANSWER_PROFILE, IdentityScopeConflict, UnsupportedNativeValue,
    encoded, freeze_answer,
)
from neo4j_native_selection_dev_20261029 import (
    NativeSchema, UnsupportedSelection, compile_selection, fields, name, quote,
)
from neo4j_read_capture_dev_20261029 import NativeReadCapture, CaptureStateError


class EvidenceConflict(RuntimeError):
    pass


class PlanEvidence:
    """One new directory of exclusive, flushed/fsynced JSON evidence files.

    No existing-directory resume or overwrite. A partial write poisons this
    writer and remains evidence. Data fsync is used, but this component does
    not certify directory-entry durability under machine/power loss.
    """

    def __init__(self, directory, *, max_record_bytes):
        if type(max_record_bytes) is not int or max_record_bytes <= 0:
            raise ValueError('Explicit positive audit record byte bound required')
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=False)
        self.max_bytes = max_record_bytes
        self.sequence, self.previous, self.binding, self.failed = 0, None, None, False

    def __call__(self, event):
        if self.failed:
            raise EvidenceConflict('Failed audit writer cannot resume automatically')
        try:
            binding = tuple(event[key] for key in ('capture_id','database','snapshot_sha256'))
            if self.binding is None:
                if event['kind'] != 'capture_open_intent':
                    raise EvidenceConflict('Audit must begin before capture opens')
                self.binding = binding
            if binding != self.binding:
                raise IdentityScopeConflict('Audit capture/database/snapshot binding changed')
            envelope = dict(evidence_sequence=self.sequence+1, previous_sha256=self.previous,
                            event=deepcopy(event))
            raw = encoded(envelope)
            if len(raw) > self.max_bytes:
                raise EvidenceConflict('Audit record exceeds its evidence byte bound')
            filename = '%08d.json' % (self.sequence+1)
            with (self.directory/filename).open('xb') as stream:
                if stream.write(raw) != len(raw):
                    raise OSError('Short evidence write')
                stream.flush()
                os.fsync(stream.fileno())
            self.sequence += 1
            self.previous = hashlib.sha256(raw).hexdigest()
            return dict(file=filename, sha256=self.previous)
        except BaseException:
            self.failed = True
            raise

    def verify(self, receipt):
        try:
            filename = receipt['file']
            if type(filename) is not str or Path(filename).name != filename:
                raise EvidenceConflict('Invalid local evidence receipt')
            if hashlib.sha256((self.directory/filename).read_bytes()).hexdigest() != receipt['sha256']:
                raise EvidenceConflict('Sealed method evidence changed')
        except BaseException:
            self.failed = True
            raise


class PreparedPlan:
    def __init__(self, plan, schema):
        started = time.monotonic()
        fields(plan, ['status','calls','combine'])
        if plan['status'] != 'ANSWER' or type(plan['calls']) is not list or not 1 <= len(plan['calls']) <= 2:
            raise UnsupportedSelection('This executor accepts bounded ANSWER plans only')
        if not isinstance(schema, NativeSchema):
            raise UnsupportedSelection('Prepared native schema required')
        self.plan, self.schema = deepcopy(plan), schema
        self.calls = [compile_selection(call,schema) for call in self.plan['calls']]
        combine = self.plan['combine']
        fields(combine, ['op'], ['key','output','columns','alias'])
        self.operation = combine['op']
        self.key, self.output, self.columns = None, None, None
        self.identities = []
        self.disjoint_certificate = None
        if self.operation == 'single':
            if len(self.calls) != 1 or set(combine) != {'op'}:
                raise UnsupportedSelection('Single plan needs exactly one input')
        else:
            if self.operation not in ('union','intersection','difference') or len(self.calls) != 2:
                raise UnsupportedSelection('Expected a supported two-input entity-set operation')
            self.key, self.output = combine.get('key'), combine.get('output')
            if self.calls[0]['columns'] != self.calls[1]['columns'] or self.key not in self.calls[0]['columns']:
                raise UnsupportedSelection('Set input columns/key must align')
            for call, compiled in zip(self.plan['calls'],self.calls):
                contract = compiled['contract']
                if contract['aggregates'] or contract['explicit_limit'] is not None or call.get('order_by'):
                    raise UnsupportedSelection('Set inputs must be unaggregated, unordered and untruncated')
                projected = contract['projected']
                source = projected[self.key]
                variable = source['field']
                if source['kind'] not in ('node','relationship') or variable not in contract['bindings']:
                    raise UnsupportedSelection('Composition key must be a native entity, not a property/string ID')
                binding = contract['bindings'][variable]
                if binding['nullable']:
                    raise UnsupportedSelection('Entity key must be bound by the required MATCH')
                if any(item['field'] != variable and not item['field'].startswith(variable+'.') for item in projected.values()):
                    raise UnsupportedSelection('All set payload must belong to its key entity')
                self.identities.append(dict(variable=variable, kind=binding['kind'], label=binding['label']))
            if any(self.identities[0][key] != self.identities[1][key] for key in ('kind','label')):
                raise UnsupportedSelection('This bounded profile requires the same key entity kind/label')
            if self.output == 'count':
                if set(combine) != {'op','key','output','alias'}:
                    raise UnsupportedSelection('Count composition needs one explicit output alias')
                self.columns = [name(combine['alias'])]
            elif self.output == 'rows':
                if set(combine) != {'op','key','output','columns'}:
                    raise UnsupportedSelection('Row composition needs an explicit final projection')
                self.columns = combine['columns']
                if (type(self.columns) is not list or not self.columns
                        or any(type(c) is not str or c not in self.calls[0]['columns'] for c in self.columns)
                        or len(set(self.columns)) != len(self.columns)):
                    raise UnsupportedSelection('Invalid final set projection')
            else:
                raise UnsupportedSelection('Set output must be rows or count')
            self.disjoint_certificate = self._disjoint()
        self.sha256 = hashlib.sha256(encoded(self.plan)).hexdigest()
        self.compile_seconds = time.monotonic()-started

    def _disjoint(self):
        # Unequal strings/booleans cannot both equal one native property value.
        # Do not extend this certificate to numeric coercion, time, NULL, IN,
        # negation or property-uniqueness assumptions. Immutable store required.
        for a in self.plan['calls'][0]['where']:
            for b in self.plan['calls'][1]['where']:
                roots = [identity['variable'] for identity in self.identities]
                property_name = a['field'].partition('.')[2]
                kinds = [compiled['contract']['bindings'][root]['properties'].get(property_name)
                         for compiled,root in zip(self.calls,roots)]
                if (a.get('op') == b.get('op') == 'eq' and 'value' in a and 'value' in b
                        and not a.get('negate',False) and not b.get('negate',False)
                        and a['field'].startswith(roots[0]+'.') and b['field'].startswith(roots[1]+'.')
                        and a['field'].partition('.')[2] == b['field'].partition('.')[2]
                        and kinds[0] == kinds[1] and kinds[0] in ('STRING','BOOLEAN')
                        and type(a['value']) is type(b['value']) and type(a['value']) in (str,bool)
                        and a['value'] != b['value']):
                    return dict(kind='unequal_native_string_or_boolean_equalities',
                                predicates=[deepcopy(a),deepcopy(b)], requires_immutable_store=True)
        return None


def _entities(value):
    if type(value) is dict:
        if value.get('t') in ('node','relationship','path'):
            return True
        return any(_entities(child) for child in value.values())
    return type(value) is list and any(_entities(child) for child in value)


def _check_answer(answer, capture, columns):
    if (answer.get('profile') != ANSWER_PROFILE or answer.get('status') != 'EXECUTED'
            or answer.get('complete') is not True or answer.get('columns') != columns):
        raise CaptureStateError('Missing/incomplete/foreign native answer or mismatched columns')
    if answer.get('scope') != asdict(capture.scope):
        raise IdentityScopeConflict('Read is not bound to this explicit transaction')
    if type(answer.get('rows')) is not list or any(type(row) is not list or len(row) != len(columns) for row in answer['rows']):
        raise CaptureStateError('Malformed native row shape')
    return answer


def _compose(prepared, answers, capture):
    index = prepared.calls[0]['columns'].index(prepared.key)
    maps = []
    for identity, answer in zip(prepared.identities,answers):
        mapping = {}
        for row in answer['rows']:
            key = row[index]
            if type(key) is not dict or key.get('t') != identity['kind'] or type(key.get('element_id')) is not str or not key['element_id']:
                raise CaptureStateError('NULL, scalar or wrong-kind entity key cannot compose')
            if (identity['kind'] == 'node' and (type(key.get('labels')) is not list or identity['label'] not in key['labels'])
                    or identity['kind'] == 'relationship' and identity['label'] != key.get('type')):
                raise CaptureStateError('Native key does not match its declared label/type')
            token = key['t'],key['element_id']
            if token in mapping and mapping[token] != row:
                raise CaptureStateError('One entity key has conflicting payload in a supposedly immutable capture')
            mapping[token] = row
        maps.append(mapping)
    common = maps[0].keys() & maps[1].keys()
    if any(maps[0][key] != maps[1][key] for key in common):
        raise CaptureStateError('Entity payload changed between inputs; no last-write-wins merge')
    selected = {'union':maps[0].keys()|maps[1].keys(), 'intersection':common,
                'difference':maps[0].keys()-maps[1].keys()}[prepared.operation]
    if prepared.output == 'count':
        return freeze_answer(prepared.columns,[[len(selected)]],capture.scope,
                             complete=True,max_bytes=capture.max_bytes)
    merged = dict(maps[1]); merged.update(maps[0])
    indices = [prepared.calls[0]['columns'].index(column) for column in prepared.columns]
    rows = [[deepcopy(merged[key][i]) for i in indices] for key in sorted(selected)]
    answer = deepcopy(answers[0]);answer.update(columns=list(prepared.columns),rows=rows,
        contains_graph_entities=_entities(rows))
    if len(encoded(answer)) > capture.max_bytes:
        raise UnsupportedNativeValue('Composed answer exceeds evidence bound; no truncation')
    return answer


def native_set_count_query(prepared):
    if prepared.operation == 'single' or prepared.output != 'count':
        raise UnsupportedSelection('Native set-count applies only to two-input entity-set count plans')
    branches = [compile_selection(call,prepared.schema,parameter_prefix=prefix)
                for call,prefix in zip(prepared.plan['calls'],('left_','right_'))]
    left,right = (call['query'] for call in branches)
    parameters = dict(branches[0]['parameters']);parameters.update(branches[1]['parameters'])
    key,alias = quote(prepared.key),quote(prepared.columns[0])
    if prepared.operation == 'union':
        return dict(query='CALL { '+left+' UNION ALL '+right+' } RETURN count(DISTINCT '+key+') AS '+alias,
                    parameters=parameters)
    # Each nested aggregate is inside its own CALL, so an empty input produces
    # [] instead of deleting the outer row. No properties/IDs leave the DB.
    lists = ('CALL { CALL { '+left+' } RETURN collect(DISTINCT '+key+') AS `__left` } '
             'CALL { CALL { '+right+' } RETURN collect(DISTINCT '+key+') AS `__right` } ')
    predicate = '`__item` IN `__right`'
    if prepared.operation == 'difference':
        predicate = 'NOT ('+predicate+')'
    return dict(query=lists+'RETURN size([`__item` IN `__left` WHERE '+predicate+']) AS '+alias,
                parameters=parameters)


class NativePlanChain:
    """Save method final first, then execute explicitly shared-plan controls.

    A control's startup/graph costs are not measured independently by this
    component. Costs and representation are kept per role; unknown fees remain
    None. Actual DB acceptance and immutable graph proof are still required.
    """

    def __init__(self, prepared, capture, evidence):
        if not isinstance(prepared,PreparedPlan) or not isinstance(capture,NativeReadCapture):
            raise ValueError('Prepared plan and owned native read capture required')
        if capture.state != 'open' or capture.emit is not evidence or not isinstance(evidence,PlanEvidence):
            raise CaptureStateError('One live capture and its exact durable evidence writer required')
        self.prepared,self.capture,self.evidence = prepared,capture,evidence
        self.method_receipt,self.controls_started,self.failed = None,False,False

    def _save(self, kind, **fields):
        scope = self.capture.scope
        return self.evidence(dict(kind=kind,capture_id=scope.capture_id,database=scope.database,
            snapshot_sha256=scope.snapshot_sha256,plan_sha256=self.prepared.sha256,epoch=time.time(),**fields))

    def _run(self, strategy, purpose):
        if strategy not in ('contract','full_detail','native_set_count'):
            raise ValueError('Unknown prepared execution strategy')
        started = time.monotonic(); completed_reads = 0
        def read(query, parameters, columns):
            nonlocal completed_reads
            self._save('plan_read_prepared',purpose=purpose,strategy=strategy,query=query,
                       parameters=parameters,expected_columns=columns,read_ordinal=completed_reads+1)
            answer = self.capture.read(query,parameters,purpose=purpose)
            _check_answer(answer,self.capture,columns)
            completed_reads += 1
            return answer
        prepared = self.prepared
        certificate = None
        if strategy == 'native_set_count':
            if prepared.operation == 'single' or prepared.output != 'count':
                return dict(answer=dict(status='NOT_APPLICABLE',complete=False,columns=None,rows=None),
                    representation='two_input_count_only',completed_read_operations=0,new_model_calls=0,
                    monetary_cost=None,formal_admission=False)
            compiled = native_set_count_query(prepared)
            answer = read(compiled['query'],compiled['parameters'],prepared.columns)
            representation = 'native_entity_set_count'
            self._count(answer)
        elif prepared.operation == 'single':
            compiled = prepared.calls[0]
            answer = read(compiled['query'],compiled['parameters'],compiled['columns'])
            representation = 'native_selection'
        elif (strategy == 'contract' and prepared.operation == 'union' and prepared.output == 'count'
                and prepared.disjoint_certificate is not None):
            counts = []
            for raw,identity in zip(prepared.plan['calls'],prepared.identities):
                reduced = deepcopy(raw)
                reduced['returns'] = [dict(field=identity['variable'],agg='count',distinct=True,alias=prepared.columns[0])]
                reduced['distinct'] = False
                compiled = compile_selection(reduced,prepared.schema)
                counts.append(self._count(read(compiled['query'],compiled['parameters'],compiled['columns'])))
            answer = freeze_answer(prepared.columns,[[sum(counts)]],self.capture.scope,
                                   complete=True,max_bytes=self.capture.max_bytes)
            certificate = prepared.disjoint_certificate
            representation = 'certified_disjoint_entity_counts'
        else:
            answers = []
            for raw in prepared.plan['calls']:
                distinct = deepcopy(raw);distinct['distinct'] = True
                compiled = compile_selection(distinct,prepared.schema)
                answers.append(read(compiled['query'],compiled['parameters'],compiled['columns']))
            answer = _compose(prepared,answers,self.capture)
            representation = 'native_entity_records'
        return dict(answer=answer,representation=representation,certificate=certificate,
            completed_read_operations=completed_reads,completed_explain_statements=completed_reads,
            completed_query_statements=completed_reads,plan_local_seconds=time.monotonic()-started,
            shared_plan_preparation_compile_seconds=prepared.compile_seconds,
            shared_plan_compile_charge_id=prepared.sha256,new_model_calls=0,monetary_cost=None,
            independent_startup_cost_measured=False,requires_owned_immutable_graph=True,
            graph_immutability_certified_by_this_component=False,formal_admission=False)

    @staticmethod
    def _count(answer):
        rows = answer['rows']
        if (len(rows) != 1 or len(rows[0]) != 1 or type(rows[0][0]) is not dict
                or rows[0][0].get('t') != 'int' or type(rows[0][0].get('v')) is not str
                or not rows[0][0]['v'].isdigit()):
            raise CaptureStateError('Expected one nonnegative native integer count')
        return int(rows[0][0]['v'])

    def run_method(self, *, strategy='contract'):
        if self.method_receipt is not None or self.failed or self.capture.state != 'open':
            raise CaptureStateError('Method chain already completed or failed')
        if strategy not in ('contract','full_detail'):
            raise ValueError('Method strategy must be contract or full_detail; native count is a control')
        try:
            self._save('plan_execution_intent',strategy=strategy,plan=self.prepared.plan,
                       compiled=self.prepared.calls,purpose='method')
            result = self._run(strategy,'method')
            self.method_receipt = self._save('method_final_saved',result=result,strategy=strategy)
            return deepcopy(result)
        except BaseException as error:
            self.failed = True
            self._save('plan_failed',purpose='method',error_type=type(error).__name__)
            raise

    def run_controls(self):
        if self.method_receipt is None or self.controls_started or self.failed or self.capture.state != 'open':
            raise CaptureStateError('Controls require a sealed successful method stage and run only once')
        results = {}
        try:
            self.evidence.verify(self.method_receipt)
            self.controls_started = True
            for control,strategy in (('correct_detail','full_detail'),('simple_correct','full_detail'),
                                     ('native_set_count','native_set_count')):
                self._save('control_execution_intent',control=control,strategy=strategy,
                           source_method_receipt=self.method_receipt,shared_planner=True,
                           independent_model_method=False,simple_correct_equals_full_detail=True)
                result = self._run(strategy,'control')
                self._save('control_final_saved',control=control,result=result,shared_planner=True,
                           independent_model_method=False)
                results[control] = result
            return results
        except BaseException as error:
            self.failed = True
            self._save('plan_failed',purpose='control',error_type=type(error).__name__)
            raise
