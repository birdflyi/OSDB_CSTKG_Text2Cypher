"""Version 0.1: typed finite graph selection and two-input set composition.

This is a DEVELOPMENT candidate, not the v16 method or a general Cypher IR.
No query text is accepted from the service planner. Labels/properties/edges
are checked against the supplied graph schema; literal values are parameters.
"""
from copy import deepcopy
from datetime import date
import json
import math
import re
import time


VERSION = 'typed-services-dev-0.1.1'


class UnsupportedPlan(ValueError):
    pass


class IncompleteEvidence(RuntimeError):
    pass


class ServiceExecutionError(RuntimeError):
    pass


def ident(value):
    return '`' + value.replace('`', '``') + '`'


def object_fields(value, required, optional=()):
    if not isinstance(value, dict) or not set(required) <= value.keys() or value.keys() - set(required) - set(optional):
        raise UnsupportedPlan('Invalid or extra plan fields')


def name(value):
    if not isinstance(value, str) or not re.fullmatch(r'[a-zA-Z][a-zA-Z0-9_]{0,31}', value):
        raise UnsupportedPlan('Invalid variable or output alias')
    return value


def literal_matches(value, kind):
    if kind == 'str':
        return type(value) is str
    if kind == 'int':
        return type(value) is int
    if kind == 'float':
        return type(value) in (int, float) and math.isfinite(value)
    if kind == 'list[str]':
        return type(value) is list and all(type(item) is str for item in value)
    if kind == 'date':
        if type(value) is not str:
            return False
        try:
            date.fromisoformat(value)
            return True
        except ValueError:
            return False
    return False


def compile_selection(call, schema):
    object_fields(call, ['nodes', 'edges', 'where', 'returns'],
                  ['distinct', 'order_by', 'limit'])
    entities = {row['label']: row for row in schema['entities']}
    relations = {(row['label'], row['subj_label'], row['obj_label']): row
                 for row in schema['relations']}
    if not isinstance(call['nodes'], list) or not 1 <= len(call['nodes']) <= 4:
        raise UnsupportedPlan('One to four typed nodes required')
    if not isinstance(call['edges'], list) or len(call['edges']) > 3:
        raise UnsupportedPlan('At most three typed edges')
    variables, types, node_labels = {}, {}, {}
    for row in call['nodes']:
        object_fields(row, ['id', 'label'])
        variable = name(row['id'])
        if row['label'] not in entities or variable in variables:
            raise UnsupportedPlan('Unknown label or duplicate variable')
        node_labels[variable] = row['label']
        variables[variable] = dict(entities[row['label']]['properties'])
        variables[variable].update({'eid': 'str', 'name': 'str', 'aliases': 'list[str]',
                                    'description': 'str'})
        types[variable] = 'node'
    matches = ['MATCH ' + ','.join('(' + v + ':' + ident(label) + ')'
                                   for v, label in node_labels.items())]
    adjacency = {v: set() for v in node_labels}
    for row in call['edges']:
        object_fields(row, ['id', 'source', 'target', 'label'])
        v, a, b = name(row['id']), row['source'], row['target']
        if v in variables or a not in node_labels or b not in node_labels:
            raise UnsupportedPlan('Unknown endpoint or duplicate edge variable')
        signature = row['label'], node_labels[a], node_labels[b]
        if signature not in relations:
            raise UnsupportedPlan('Edge type/direction violates schema')
        variables[v] = {**relations[signature]['properties'], 'rid': 'str'}
        types[v] = 'edge'
        adjacency[a].add(b)
        adjacency[b].add(a)
        # Separate MATCH clauses specify conjunction with possible edge reuse.
        matches.append(f'MATCH ({a})-[{v}:{ident(row["label"])}]->({b})')
    reached = {next(iter(node_labels))}
    while True:
        grown = reached | {b for a in reached for b in adjacency[a]}
        if grown == reached:
            break
        reached = grown
    if reached != set(node_labels):
        raise UnsupportedPlan('Disconnected Cartesian patterns are not in v0.1')

    def field(value, allow_node=False):
        if allow_node and value in node_labels:
            return value, 'node'
        if not isinstance(value, str) or value.count('.') != 1:
            raise UnsupportedPlan('Expected variable.property')
        variable, prop = value.split('.')
        if variable not in variables or prop not in variables[variable]:
            raise UnsupportedPlan('Unknown property')
        return variable + '.' + ident(prop), variables[variable][prop]

    params, predicates = {}, []
    if not isinstance(call['where'], list) or len(call['where']) > 16:
        raise UnsupportedPlan('Invalid AND predicate list')
    comparisons = {'eq': '=', 'ne': '<>', 'lt': '<', 'le': '<=', 'gt': '>', 'ge': '>='}
    for index, predicate in enumerate(call['where']):
        object_fields(predicate, ['field', 'op'], ['value', 'other_field'])
        expression, kind = field(predicate['field'])
        op = predicate['op']
        if op in ('is_null', 'not_null'):
            if 'value' in predicate or 'other_field' in predicate:
                raise UnsupportedPlan('Null predicate has no argument')
            predicates.append(expression + (' IS NULL' if op == 'is_null' else ' IS NOT NULL'))
            continue
        if ('value' in predicate) == ('other_field' in predicate):
            raise UnsupportedPlan('One predicate argument required')
        if 'other_field' in predicate:
            right, right_kind = field(predicate['other_field'])
            if op not in comparisons or kind != right_kind:
                raise UnsupportedPlan('Incompatible property comparison')
        else:
            value = predicate['value']
            parameter = 'p' + str(index)
            if value is None or not isinstance(value, (str, int, float, bool, list)):
                raise UnsupportedPlan('Use explicit null predicates')
            if isinstance(value, list) and any(type(v) not in (str, int, float, bool) for v in value):
                raise UnsupportedPlan('Nested predicate values unsupported')
            if op == 'in':
                typed = type(value) is list and all(literal_matches(v, kind) for v in value)
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
            right = ('date($' + parameter + ')' if kind == 'date' else '$' + parameter)
        if op in comparisons:
            predicates.append(expression + comparisons[op] + right)
        elif op == 'in' and isinstance(predicate.get('value'), list):
            predicates.append(expression + ' IN ' + right)
        elif op == 'contains' and kind.startswith('list['):
            predicates.append(right + ' IN ' + expression)
        elif op in ('contains', 'starts_with', 'ends_with') and kind == 'str':
            if not isinstance(predicate.get('value'), str):
                raise UnsupportedPlan('String predicate requires string literal')
            predicates.append(expression + ' ' + {'contains': 'CONTAINS',
                              'starts_with': 'STARTS WITH', 'ends_with': 'ENDS WITH'}[op] + ' ' + right)
        else:
            raise UnsupportedPlan('Unsupported typed predicate')
    if predicates:
        matches.append('WHERE ' + ' AND '.join('(' + item + ')' for item in predicates))
    if not isinstance(call['returns'], list) or not 1 <= len(call['returns']) <= 8:
        raise UnsupportedPlan('One to eight return items required')
    outputs, columns, projected, aggregates = [], [], {}, []
    for item in call['returns']:
        object_fields(item, ['field', 'alias'], ['agg', 'distinct'])
        alias = name(item['alias'])
        if alias in columns:
            raise UnsupportedPlan('Duplicate output alias')
        aggregate = item.get('agg')
        if item['field'] == '*' and aggregate == 'count':
            expression, kind = '*', 'record'
        else:
            expression, kind = field(item['field'], allow_node=aggregate == 'count')
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
    return {'query': ' '.join(matches), 'parameters': params, 'columns': columns,
            'contract': {'node_labels': node_labels, 'projected': projected,
                         'aggregates': aggregates, 'explicit_limit': call.get('limit'),
                         'set_rows': call.get('distinct', False)}}


def _set_contract(call, compiled, key):
    contract = compiled['contract']
    if contract['aggregates'] or contract['explicit_limit'] is not None or call.get('order_by'):
        raise UnsupportedPlan('Set inputs must be unaggregated and untruncated')
    reference = contract['projected'].get(key, '')
    if not reference.endswith('.eid'):
        raise UnsupportedPlan('Composition key must be a projected entity eid')
    node = reference.split('.')[0]
    if any(not field.startswith(node + '.') for field in contract['projected'].values()):
        raise UnsupportedPlan('Set payload must be functionally attached to its key entity')
    return node, contract['node_labels'][node]


def _disjoint_equality(left, right, compiled_left, compiled_right, key, schema):
    a, label = _set_contract(left, compiled_left, key)
    b, other_label = _set_contract(right, compiled_right, key)
    if label != other_label:
        raise UnsupportedPlan('Mixed entity types require a separate identity contract')
    props = next(row['properties'] for row in schema['entities'] if row['label'] == label)
    props = {**props, 'eid': 'str', 'name': 'str'}
    for x in left['where']:
        for y in right['where']:
            if (x.get('op') == y.get('op') == 'eq' and 'value' in x and 'value' in y
                and x['field'].startswith(a + '.') and y['field'].startswith(b + '.')
                and x['field'].split('.')[1] == y['field'].split('.')[1]):
                kind = props.get(x['field'].split('.')[1])
                if (kind in ('str', 'int', 'float', 'date')
                    and type(x['value']) == type(y['value'])
                    and x['value'] != y['value']):
                    return True
    return False


def execute_plan(plan, schema, execute, *, strategy='contract'):
    """execute(query, params) -> {rows,columns,complete,... measured data}.

    contract: certified-disjoint union counts may use count summaries.
    full_detail: always retrieve sufficient entity rows for set composition.
    Both share the same logical plan. Neither uses gold or reference outputs.
    """
    if strategy not in ('contract', 'full_detail'):
        raise UnsupportedPlan('Unknown execution strategy')
    object_fields(plan, ['status'], ['calls', 'combine', 'reason'])
    if plan['status'] == 'UNSUPPORTED':
        return {'status': 'UNSUPPORTED', 'rows': None, 'observations': []}
    if plan['status'] != 'ANSWER' or not isinstance(plan.get('calls'), list) or not 1 <= len(plan['calls']) <= 2:
        raise UnsupportedPlan('One or two calls required')
    combine = plan.get('combine')
    object_fields(combine, ['op'], ['key', 'output', 'columns', 'alias'])
    calls = plan['calls']
    compiled = [compile_selection(call, schema) for call in calls]
    operation = combine['op']
    if operation == 'single':
        if len(calls) != 1 or set(combine) != {'op'}:
            raise UnsupportedPlan('Single needs exactly one input')
    elif operation not in ('union', 'intersection', 'difference') or len(calls) != 2:
        raise UnsupportedPlan('Unsupported two-input composition')
    observations, started = [], time.monotonic()

    def run(item):
        observation = execute(item['query'], item['parameters'])
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
        identities = [_set_contract(call, query, key) for call, query in zip(calls, compiled)]
        if identities[0][1] != identities[1][1]:
            raise UnsupportedPlan('Incompatible key entity types')
        output = combine.get('output')
        if output not in ('rows', 'count'):
            raise UnsupportedPlan('Set output must be rows or count')
        if output == 'count':
            columns = [name(combine.get('alias', 'count'))]
        else:
            columns = combine.get('columns')
            if not isinstance(columns, list) or not columns or len(set(columns)) != len(columns) or any(c not in compiled[0]['columns'] for c in columns):
                raise UnsupportedPlan('Invalid final projection')
        if (strategy == 'contract' and operation == 'union' and output == 'count'
            and _disjoint_equality(*calls, *compiled, key, schema)):
            counts = []
            for call, query in zip(calls, compiled):
                reduced = deepcopy(call)
                reduced['returns'] = [{'field': query['contract']['projected'][key],
                                       'agg': 'count', 'distinct': True, 'alias': columns[0]}]
                reduced['distinct'] = False
                summary = run(compile_selection(reduced, schema))
                if len(summary) != 1 or len(summary[0]) != 1 or type(summary[0][0]) is not int:
                    raise UnsupportedPlan('Invalid count summary')
                counts.append(summary[0][0])
            rows = [[sum(counts)]]
            representation = 'certified_disjoint_counts'
        else:
            records = [run(query) for query in compiled]
            indices = {column: index for index, column in enumerate(compiled[0]['columns'])}
            maps = []
            for result in records:
                mapping = {}
                for row in result:
                    if len(row) != len(indices):
                        raise UnsupportedPlan('Malformed executor row')
                    identity = row[indices[key]]
                    if not isinstance(identity, str):
                        raise UnsupportedPlan('Non-string or missing entity identity')
                    if identity in mapping and mapping[identity] != row:
                        raise UnsupportedPlan('Payload not uniquely determined by entity key')
                    mapping[identity] = row
                maps.append(mapping)
            shared = maps[0].keys() & maps[1].keys()
            if any(maps[0][key] != maps[1][key] for key in shared):
                raise UnsupportedPlan('Input snapshots/payload contracts disagree')
            selected = ({'union': maps[0].keys() | maps[1].keys(),
                         'intersection': shared,
                         'difference': maps[0].keys() - maps[1].keys()})[operation]
            merged = {**maps[1], **maps[0]}
            rows = ([[len(selected)]] if output == 'count' else
                    [[merged[k][indices[column]] for column in columns] for k in sorted(selected)])
            representation = 'entity_records'
    return {'status': 'EXECUTED', 'rows': rows, 'columns': columns,
            'representation': representation, 'strategy': strategy,
            'observations': observations, 'execution_wall_seconds': time.monotonic() - started}


PLAN_INSTRUCTIONS = """Return one JSON object only, without Markdown.
Use {"status":"UNSUPPORTED","reason":"..."} if outside the finite language.
Otherwise use {"status":"ANSWER","calls":[selection],"combine":{"op":"single"}}.
A selection is {"nodes":[{"id":"n","label":"SCHEMA_LABEL"}],"edges":[],
"where":[],"returns":[{"field":"n.name","alias":"name"}]}.
Up to4 nodes and3 connected directed edges; edges have id,source,target,label.
All labels/properties and edge directions must match schema. Available node
metadata properties: eid,name,aliases,description. Edge metadata: rid.
where is an AND list of {field:"n.property",op:"eq",value:literal}; supported
ops eq,ne,lt,le,gt,ge,in,contains,starts_with,ends_with,is_null,not_null.
Use other_field instead of value for same-type property comparisons.
returns items have field and alias; optional agg=count|sum|avg|min|max|collect,
optional distinct boolean for aggregation. count may use field "*" or node ID.
Selection optional distinct boolean, order_by:[{column:alias,direction:"ASC"}],
limit:positive integer ONLY when explicitly requested; never add a default LIMIT.
For union/intersection/difference of ENTITY SETS use exactly2 selections. Each
must return the same columns, include projected n.eid as alias "id", and only
properties of that same answer entity. No input aggregation, order or limit.
combine is {op:"union"|"intersection"|"difference",key:"id",output:"count",
alias:"count"}, or output:"rows",columns:[requested output aliases].
This finite version does not support OPTIONAL MATCH, subqueries, arbitrary
expressions, regex, variable-length paths, arithmetic, HAVING, multi-stage
aggregation, joins of service summaries, or semantic relations absent in schema.
Do not replace an unsupported condition with an easier question.
"""
