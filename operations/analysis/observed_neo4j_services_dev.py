# -*- coding: utf-8 -*-
"""Isolated observed-schema Neo4j DEVELOPMENT profile, not formal admission.

This is a DEVELOPMENT candidate, not the v16 method or a general Cypher IR.
No query text is accepted from the service planner. Labels/properties/edges
are checked against the supplied graph schema; literal values are parameters.
Specialized from BASE_SOURCE_SHA256 without changing the frozen source. No
injected metadata, query-specific rules, or extra calls. Temporal annotations
are trusted schema policy: storage stays string, predicate operands use
datetime conversion, and raw timestamp projections stay string.
"""
from copy import deepcopy
from datetime import date, datetime
import json
import math
import re
import time
VERSION = 'observed-neo4j-profile-dev-1'
BASE_SOURCE_SHA256 = '98ac435c8ce509c3810c7be73086072b082718e0fcfff5ff7d9056236dc5bc2b'

class UnsupportedPlan(ValueError):
    pass

class IncompleteEvidence(RuntimeError):
    pass

class ServiceExecutionError(RuntimeError):
    pass

def ident(value):
    return '`' + value.replace('`', '``') + '`'

def object_fields(value, required, optional=()):
    if not isinstance(value, dict):
        raise UnsupportedPlan('Expected an object with required fields: ' + ','.join(required))
    missing = set(required) - value.keys()
    extra = value.keys() - set(required) - set(optional)
    if missing or extra:
        raise UnsupportedPlan('Missing required fields: ' + ','.join(sorted(missing)) + '; unsupported fields: ' + ','.join(sorted(extra)))

def name(value):
    if not isinstance(value, str) or not re.fullmatch('[a-zA-Z][a-zA-Z0-9_]{0,31}', value):
        raise UnsupportedPlan('Invalid variable or output alias')
    return value

def literal_matches(value, kind):
    if kind == 'bool':
        return type(value) is bool
    if kind == 'datetime':
        if type(value) is not str or not re.fullmatch(
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)',value):
            return False
        try:
            # Validate only. Keep original offset/fraction bytes for Neo4j;
            # Python's microsecond parsing is never used to normalize or score.
            zone=re.search(r'(Z|[+-]\d{2}:\d{2})$',value).group(1)
            datetime.fromisoformat(value[:19]+zone.replace('Z','+00:00'))
            return True
        except ValueError:
            return False
    if kind == 'str':
        return type(value) is str
    if kind == 'int':
        return type(value) is int
    if kind == 'float':
        return type(value) in (int, float) and math.isfinite(value)
    if kind == 'list[str]':
        return type(value) is list and all((type(item) is str for item in value))
    if kind == 'date':
        if type(value) is not str:
            return False
        try:
            date.fromisoformat(value)
            return True
        except ValueError:
            return False
    return False

def compile_selection(call, schema, *, backend='neo4j-observed-v1'):
    if backend != 'neo4j-observed-v1':
        raise UnsupportedPlan('Unknown backend profile')

    def ident(value):
        return globals()['ident'](value)
    object_fields(call, ['nodes', 'edges', 'where', 'returns'], ['distinct', 'order_by', 'limit', 'group_by'])
    entities = {row['label']: row for row in schema['entities']}
    identity_fields = {}
    relations = {(row['label'], row['subj_label'], row['obj_label']): row for row in schema['relations']}
    if len(entities)!=len(schema['entities']) or len(relations)!=len(schema['relations']):
        raise UnsupportedPlan('Duplicate observed schema declaration')
    for declaration in list(entities.values())+list(relations.values()):
        temporal=declaration.get('temporal_properties',[])
        if type(temporal) is not list or any(declaration['properties'].get(p)!='str' for p in temporal):
            raise UnsupportedPlan('Temporal annotation requires an observed string property')
    temporal_fields=set()
    if not isinstance(call['nodes'], list) or not 1 <= len(call['nodes']) <= 4:
        raise UnsupportedPlan('One to four typed nodes required')
    if not isinstance(call['edges'], list) or len(call['edges']) > 3:
        raise UnsupportedPlan('At most three typed edges')
    (variables, types, node_labels) = ({}, {}, {})
    for row in call['nodes']:
        object_fields(row, ['id', 'label'])
        variable = name(row['id'])
        if row['label'] not in entities or variable in variables:
            raise UnsupportedPlan('Unknown label or duplicate variable')
        node_labels[variable] = row['label']
        variables[variable] = dict(entities[row['label']]['properties'])
        types[variable] = 'node'
        primary=entities[row['label']].get('identity_property')
        if primary not in variables[variable] or variables[variable][primary] not in ('str','int'):
            raise UnsupportedPlan('Entity identity needs a declared observed string/integer property')
        identity_fields[variable]={'field':variable+'.'+primary,'type':variables[variable][primary]}
        temporal_fields.update(variable+'.'+p for p in entities[row['label']].get('temporal_properties',[]))
    adjacency = {v: set() for v in node_labels}
    required_adjacency = {v: set() for v in node_labels}
    required_nodes = {next(iter(node_labels))}
    optional_edges = []
    required_edges = []
    for row in call['edges']:
        object_fields(row, ['id', 'source', 'target', 'label'], ['optional'])
        if type(row.get('optional', False)) is not bool:
            raise UnsupportedPlan('Optional edge flag must be boolean')
        (v, a, b) = (name(row['id']), row['source'], row['target'])
        if v in variables or a not in node_labels or b not in node_labels:
            raise UnsupportedPlan('Unknown endpoint or duplicate edge variable')
        signature = (row['label'], node_labels[a], node_labels[b])
        if signature not in relations:
            raise UnsupportedPlan('Edge type/direction violates schema')
        variables[v] = dict(relations[signature]['properties'])
        types[v] = 'edge'
        primary=relations[signature].get('identity_property')
        if primary is not None:
            if variables[v].get(primary) not in ('str','int'):
                raise UnsupportedPlan('Relationship identity must be a declared observed property')
            identity_fields[v]={'field':v+'.'+primary,'type':variables[v][primary]}
        temporal_fields.update(v+'.'+p for p in relations[signature].get('temporal_properties',[]))
        adjacency[a].add(b)
        adjacency[b].add(a)
        if row.get('optional', False):
            optional_edges.append(row)
        else:
            required_edges.append(row)
            required_nodes.update([a, b])
            required_adjacency[a].add(b)
            required_adjacency[b].add(a)
    reached = {next(iter(node_labels))}
    while True:
        grown = reached | {b for a in reached for b in adjacency[a]}
        if grown == reached:
            break
        reached = grown
    if reached != set(node_labels):
        raise UnsupportedPlan('Disconnected Cartesian patterns are not supported')
    if len(optional_edges) > 1:
        raise UnsupportedPlan('v0.2 admits at most one optional edge')
    reached_required = {next(iter(node_labels))}
    while True:
        grown = reached_required | {b for a in reached_required for b in required_adjacency[a]}
        if grown == reached_required:
            break
        reached_required = grown
    if reached_required != required_nodes:
        raise UnsupportedPlan('Required pattern must be connected to the first/root node')
    nullable_nodes = set(node_labels) - required_nodes
    matches = ['MATCH ' + ','.join(('(' + v + ':' + ident(label) + ')' for (v, label) in node_labels.items() if v in required_nodes))]
    for row in required_edges:
        matches[0] += f",({row['source']})-[{row['id']}:{ident(row['label'])}]->({row['target']})"
    for row in optional_edges:
        if not {row['source'], row['target']} & required_nodes:
            raise UnsupportedPlan('Optional edge needs an already bound endpoint')

        def endpoint(v):
            return v + (':' + ident(node_labels[v]) if v in nullable_nodes else '')
        matches.append(f"OPTIONAL MATCH ({endpoint(row['source'])})-[{row['id']}:{ident(row['label'])}]->({endpoint(row['target'])})")
    def field(value, allow_node=False, compare=False):
        if allow_node and value in node_labels:
            return (value, 'node')
        if not isinstance(value, str) or value.count('.') != 1:
            raise UnsupportedPlan('Expected variable.property')
        (variable, prop) = value.split('.')
        if variable not in variables or prop not in variables[variable]:
            raise UnsupportedPlan('Unknown property')
        expression=variable+'.'+ident(prop)
        return ('datetime('+expression+')','datetime') if compare and value in temporal_fields else (expression,variables[variable][prop])
    (params, predicates) = ({}, [])
    if not isinstance(call['where'], list) or len(call['where']) > 16:
        raise UnsupportedPlan('Invalid AND predicate list')
    comparisons = {'eq': '=', 'ne': '<>', 'lt': '<', 'le': '<=', 'gt': '>', 'ge': '>='}
    for (index, predicate) in enumerate(call['where']):
        object_fields(predicate, ['field', 'op'], ['value', 'other_field', 'negate'])
        if type(predicate.get('negate', False)) is not bool:
            raise UnsupportedPlan('Predicate negate must be boolean')
        (expression, kind) = field(predicate['field'],compare=True)
        op = predicate['op']
        if kind=='bool' and op not in ('eq','ne','in','is_null','not_null'):
            raise UnsupportedPlan('Boolean predicates admit equality or membership only')
        if kind=='datetime' and op not in (*comparisons,'is_null','not_null'):
            raise UnsupportedPlan('Instant predicates require scalar comparisons')

        def emit_predicate(value):
            predicates.append('NOT (' + value + ')' if predicate.get('negate') else value)
            return
        if op in ('is_null', 'not_null'):
            if 'value' in predicate or 'other_field' in predicate:
                raise UnsupportedPlan('Null predicate has no argument')
            emit_predicate(expression + (' IS NULL' if op == 'is_null' else ' IS NOT NULL'))
            continue
        if ('value' in predicate) == ('other_field' in predicate):
            raise UnsupportedPlan('One predicate argument required')
        if 'other_field' in predicate:
            (right, right_kind) = field(predicate['other_field'],compare=True)
            if op not in comparisons or kind != right_kind:
                raise UnsupportedPlan('Incompatible property comparison')
        else:
            value = predicate['value']
            parameter = 'p' + str(index)
            if value is None or not isinstance(value, (str, int, float, bool, list)):
                raise UnsupportedPlan('Use explicit null predicates')
            if isinstance(value, list) and any((type(v) not in (str, int, float, bool) for v in value)):
                raise UnsupportedPlan('Nested predicate values unsupported')
            if op == 'in':
                typed = type(value) is list and all((literal_matches(v, kind) for v in value))
            elif op == 'contains' and kind == 'list[str]':
                typed = type(value) is str
            else:
                typed = literal_matches(value, kind)
            if not typed:
                raise UnsupportedPlan('Literal type does not match schema property')
            if kind == 'date':
                if not isinstance(value, str) or op not in comparisons:
                    raise UnsupportedPlan('Date predicates require scalar ISO date')
                date.fromisoformat(value)
            params[parameter] = value
            right = 'date($' + parameter + ')' if kind == 'date' else 'datetime($'+parameter+')' if kind=='datetime' else '$' + parameter
        if op in comparisons:
            emit_predicate(expression + comparisons[op] + right)
        elif op == 'in' and isinstance(predicate.get('value'), list):
            emit_predicate(expression + ' IN ' + right)
        elif op == 'contains' and kind.startswith('list['):
            emit_predicate(right + ' IN ' + expression)
        elif op in ('contains', 'starts_with', 'ends_with') and kind == 'str':
            if not isinstance(predicate.get('value'), str):
                raise UnsupportedPlan('String predicate requires string literal')
            emit_predicate(expression + ' ' + {'contains': 'CONTAINS', 'starts_with': 'STARTS WITH', 'ends_with': 'ENDS WITH'}[op] + ' ' + right)
        else:
            raise UnsupportedPlan('Unsupported typed predicate')
    if predicates:
        if optional_edges:
            matches.append('WITH *')
        matches.append('WHERE ' + ' AND '.join(('(' + item + ')' for item in predicates)))
    if not isinstance(call['returns'], list) or not 1 <= len(call['returns']) <= 8:
        raise UnsupportedPlan('One to eight return items required')
    (outputs, columns, projected, aggregates) = ([], [], {}, [])
    for item in call['returns']:
        object_fields(item, ['field', 'alias'], ['agg', 'distinct'])
        alias = name(item['alias'])
        if alias in columns:
            raise UnsupportedPlan('Duplicate output alias')
        aggregate = item.get('agg')
        if item['field'] == '*' and aggregate == 'count':
            (expression, kind) = ('*', 'record')
        else:
            (expression, kind) = field(item['field'], allow_node=aggregate == 'count')
        if aggregate:
            if aggregate not in ('count', 'sum', 'avg', 'min', 'max', 'collect'):
                raise UnsupportedPlan('Unsupported aggregation')
            if type(item.get('distinct', False)) is not bool:
                raise UnsupportedPlan('distinct must be boolean')
            if expression == '*' and item.get('distinct'):
                raise UnsupportedPlan('COUNT DISTINCT * unsupported')
            if aggregate in ('sum', 'avg') and kind not in ('int', 'float'):
                raise UnsupportedPlan('Numeric aggregation requires numeric property')
            expression = aggregate + '(' + ('DISTINCT ' if item.get('distinct') else '') + expression + ')'
            aggregates.append(alias)
        else:
            if 'distinct' in item:
                raise UnsupportedPlan('Per-item DISTINCT needs aggregation')
            projected[alias] = item['field']
        columns.append(alias)
        outputs.append(expression + ' AS ' + ident(alias))
    if type(call.get('distinct', False)) is not bool:
        raise UnsupportedPlan('Row DISTINCT must be boolean')
    if 'group_by' in call:
        keys = call['group_by']
        if not aggregates or not isinstance(keys, list) or (not keys) or (len(keys) > 4) or (len(set(keys)) != len(keys)):
            raise UnsupportedPlan('Explicit grouping needs aggregation and one to four unique property keys')
        grouped = [field(value)[0] for value in keys]
        for value in projected.values():
            variable = value.split('.')[0]
            identity = identity_fields.get(variable,{}).get('field')
            if value not in keys and identity not in keys:
                raise UnsupportedPlan('Return property is not determined by declared grouping keys')
        hidden = [expression + ' AS ' + ident('__group_' + str(index)) for (index, expression) in enumerate(grouped)]
        matches.append('WITH ' + ','.join(hidden + outputs))
        matches.append('RETURN ' + ('DISTINCT ' if call.get('distinct') else '') + ','.join((ident(alias) for alias in columns)))
    else:
        matches.append('RETURN ' + ('DISTINCT ' if call.get('distinct') else '') + ','.join(outputs))
    ordering = call.get('order_by', [])
    if not isinstance(ordering, list):
        raise UnsupportedPlan('Invalid ordering')
    order_clauses = []
    for item in ordering:
        object_fields(item, ['column', 'direction'])
        if item['column'] not in columns or item['direction'] not in ('ASC', 'DESC'):
            raise UnsupportedPlan('Invalid order alias/direction')
        order_clauses.append(ident(item['column']) + ' ' + item['direction'])
    if order_clauses:
        matches.append('ORDER BY ' + ','.join(order_clauses))
    if 'limit' in call:
        if type(call['limit']) is not int or not 1 <= call['limit'] <= 10000:
            raise UnsupportedPlan('Invalid explicit limit')
        params['requested_limit'] = call['limit']
        matches.append('LIMIT $requested_limit')
    result = {'query': ' '.join(matches), 'parameters': params, 'columns': columns, 'contract': {'node_labels': node_labels, 'identity_fields':identity_fields,'projected': projected, 'nullable_nodes': sorted(nullable_nodes), 'group_by': call.get('group_by'), 'aggregates': aggregates, 'explicit_limit': call.get('limit'), 'set_rows': call.get('distinct', False),'temporal_fields':sorted(temporal_fields)}}
    return result

def _set_contract(call, compiled, key):
    contract = compiled['contract']
    if contract['aggregates'] or contract['explicit_limit'] is not None or call.get('order_by'):
        raise UnsupportedPlan('Set inputs must be unaggregated and untruncated')
    reference = contract['projected'].get(key, '')
    node = reference.split('.')[0]
    identity = contract['identity_fields'].get(node, {})
    if identity.get('field') != reference:
        raise UnsupportedPlan('Composition key must project the declared observed entity identity')
    if node in contract.get('nullable_nodes', []):
        raise UnsupportedPlan('Composition entity key must be bound in the required pattern')
    if any((not field.startswith(node + '.') for field in contract['projected'].values())):
        raise UnsupportedPlan('Set payload must be functionally attached to its key entity')
    return (node, contract['node_labels'][node])

def _disjoint_equality(left, right, compiled_left, compiled_right, key, schema):
    (a, label) = _set_contract(left, compiled_left, key)
    (b, other_label) = _set_contract(right, compiled_right, key)
    if label != other_label:
        raise UnsupportedPlan('Mixed entity types require a separate identity contract')
    props = next((row['properties'] for row in schema['entities'] if row['label'] == label))
    for x in left['where']:
        for y in right['where']:
            # Unequal RFC3339 spellings may identify the same instant. The
            # lexical str disjointness certificate does not apply to them.
            if x.get('field') in compiled_left['contract']['temporal_fields'] or y.get('field') in compiled_right['contract']['temporal_fields']:
                continue
            if x.get('op') == y.get('op') == 'eq' and 'value' in x and ('value' in y) and (not x.get('negate', False)) and (not y.get('negate', False)) and x['field'].startswith(a + '.') and y['field'].startswith(b + '.') and (x['field'].split('.')[1] == y['field'].split('.')[1]):
                kind = props.get(x['field'].split('.')[1])
                if kind in ('str', 'int', 'float', 'date') and type(x['value']) == type(y['value']) and (x['value'] != y['value']):
                    return True
    return False

def execute_plan(plan, schema, execute, *, strategy='contract', backend='neo4j-observed-v1'):
    """execute(query, params) -> {rows,columns,complete,... measured data}.

    contract: certified-disjoint union counts may use count summaries.
    full_detail: always retrieve sufficient entity rows for set composition.
    Both share the same logical plan. Neither uses gold or reference outputs.
    """
    total_started = time.monotonic()
    compile_seconds = 0.0
    db_wall_seconds = 0.0
    compose_seconds = 0.0
    if strategy not in ('contract', 'full_detail'):
        raise UnsupportedPlan('Unknown execution strategy')
    object_fields(plan, ['status'], ['calls', 'combine', 'reason'])
    if plan['status'] == 'UNSUPPORTED':
        return {'status': 'UNSUPPORTED', 'rows': None, 'observations': []}
    if plan['status'] != 'ANSWER' or not isinstance(plan.get('calls'), list) or (not 1 <= len(plan['calls']) <= 2):
        raise UnsupportedPlan('One or two calls required')
    combine = plan.get('combine')
    object_fields(combine, ['op'], ['key', 'output', 'columns', 'alias'])
    calls = plan['calls']

    def compile_call(call):
        nonlocal compile_seconds
        start = time.monotonic()
        value = compile_selection(call, schema, backend=backend)
        compile_seconds += time.monotonic() - start
        return value
    compiled = [compile_call(call) for call in calls]
    operation = combine['op']
    if operation == 'single':
        if len(calls) != 1 or set(combine) != {'op'}:
            raise UnsupportedPlan('Single needs exactly one input')
    elif operation not in ('union', 'intersection', 'difference') or len(calls) != 2:
        raise UnsupportedPlan('Unsupported two-input composition')
    (observations, started) = ([], time.monotonic())

    def run(item):
        nonlocal db_wall_seconds
        call_started = time.monotonic()
        observation = execute(item['query'], item['parameters'])
        db_wall_seconds += time.monotonic() - call_started
        observations.append(observation)
        if observation.get('status') == 'EXECUTION_ERROR':
            raise ServiceExecutionError(observation.get('error', 'Database execution failed'))
        if observation.get('complete') is not True:
            raise IncompleteEvidence('Executor did not certify complete result')
        if observation.get('columns') != item['columns']:
            raise UnsupportedPlan('Executor output columns violate contract')
        return observation['rows']
    if operation == 'single':
        rows = run(compiled[0])
        columns = compiled[0]['columns']
        representation = 'native_selection'
    else:
        key = combine.get('key')
        if key not in compiled[0]['columns'] or compiled[0]['columns'] != compiled[1]['columns']:
            raise UnsupportedPlan('Set input keys/columns must align')
        identities = [_set_contract(call, query, key) for (call, query) in zip(calls, compiled)]
        if identities[0][1] != identities[1][1]:
            raise UnsupportedPlan('Incompatible key entity types')
        output = combine.get('output')
        if output not in ('rows', 'count'):
            raise UnsupportedPlan('Set output must be rows or count')
        if output == 'count':
            columns = [name(combine.get('alias', 'count'))]
        else:
            columns = combine.get('columns')
            if not isinstance(columns, list) or not columns or len(set(columns)) != len(columns) or any((c not in compiled[0]['columns'] for c in columns)):
                raise UnsupportedPlan('Invalid final projection')
        if strategy == 'contract' and operation == 'union' and (output == 'count') and _disjoint_equality(*calls, *compiled, key, schema):
            counts = []
            for (call, query) in zip(calls, compiled):
                reduced = deepcopy(call)
                reduced['returns'] = [{'field': query['contract']['projected'][key], 'agg': 'count', 'distinct': True, 'alias': columns[0]}]
                reduced['distinct'] = False
                summary = run(compile_call(reduced))
                if len(summary) != 1 or len(summary[0]) != 1 or type(summary[0][0]) is not int:
                    raise UnsupportedPlan('Invalid count summary')
                counts.append(summary[0][0])
            compose_started = time.monotonic()
            rows = [[sum(counts)]]
            compose_seconds += time.monotonic() - compose_started
            representation = 'certified_disjoint_counts'
        else:
            detailed = []
            for call in calls:
                distinct_call = deepcopy(call)
                distinct_call['distinct'] = True
                detailed.append(compile_call(distinct_call))
            records = [run(query) for query in detailed]
            compose_started = time.monotonic()
            indices = {column: index for (index, column) in enumerate(compiled[0]['columns'])}
            maps = []
            for (input_index, result) in enumerate(records):
                mapping = {}
                for row in result:
                    if len(row) != len(indices):
                        raise UnsupportedPlan('Malformed executor row')
                    identity = row[indices[key]]
                    node = identities[input_index][0]
                    kind = compiled[input_index]['contract']['identity_fields'][node]['type']
                    valid_identity = type(identity) is {'str': str, 'int': int}[kind]
                    if not valid_identity:
                        raise UnsupportedPlan('Non-string or missing entity identity')
                    if identity in mapping and mapping[identity] != row:
                        raise UnsupportedPlan('Payload not uniquely determined by entity key')
                    mapping[identity] = row
                maps.append(mapping)
            shared = maps[0].keys() & maps[1].keys()
            if any((maps[0][key] != maps[1][key] for key in shared)):
                raise UnsupportedPlan('Input snapshots/payload contracts disagree')
            selected = {'union': maps[0].keys() | maps[1].keys(), 'intersection': shared, 'difference': maps[0].keys() - maps[1].keys()}[operation]
            merged = {**maps[1], **maps[0]}
            rows = [[len(selected)]] if output == 'count' else [[merged[k][indices[column]] for column in columns] for k in sorted(selected)]
            representation = 'entity_records'
            compose_seconds += time.monotonic() - compose_started
    total_seconds = time.monotonic() - total_started
    return {'status': 'EXECUTED', 'rows': rows, 'columns': columns, 'representation': representation, 'strategy': strategy, 'observations': observations, 'execution_wall_seconds': time.monotonic() - started, 'total_plan_seconds': total_seconds, 'compile_seconds': compile_seconds, 'db_roundtrip_seconds': db_wall_seconds, 'compose_and_payload_check_seconds': compose_seconds, 'validation_and_dispatch_seconds': max(0.0, total_seconds - compile_seconds - db_wall_seconds - compose_seconds)}
