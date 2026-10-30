"""Same-plan native set-count DEVELOPMENT control; pure compilation only.

Not a new model/method, not an optimal-query claim, and not DB acceptance.
Requires one immutable, fully admitted observed-schema Neo4j graph. Does not
alter the frozen planner, API policy, campaign, or approved model messages.
Only complete same-type primary-key sets with count output are supported.
"""
from copy import deepcopy
import re
from observed_neo4j_services_dev_v2 import (
    UnsupportedPlan, compile_selection, object_fields, name, ident, _set_contract,
)

VERSION = 'native-set-count-control-dev-1'


def namespace_parameters(query, parameters, prefix):
    """Rename parameter tokens, never quoted identifiers or string contents.

    Input is emitted by the pinned selection compiler, not raw model Cypher.
    Reject comments/unbound/missing tokens rather than silently rewriting.
    """
    name(prefix)
    mapping = {key: prefix + key for key in parameters}
    for key in mapping:
        if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]*', key):
            raise UnsupportedPlan('Invalid compiled parameter name')
    out, seen, i = [], set(), 0
    while i < len(query):
        char = query[i]
        if char in ('`', "'", '"'):
            quote, start = char, i
            i += 1
            while i < len(query):
                if query[i] == '\\' and quote != '`':
                    i += 2
                elif query[i] == quote:
                    if i + 1 < len(query) and query[i + 1] == quote:
                        i += 2
                    else:
                        i += 1
                        break
                else:
                    i += 1
            else:
                raise UnsupportedPlan('Unterminated compiled quotation')
            out.append(query[start:i])
        elif query.startswith('//', i) or query.startswith('/*', i):
            raise UnsupportedPlan('Unexpected comment in compiled selection')
        elif char == '$':
            match = re.match(r'\$([A-Za-z][A-Za-z0-9_]*)', query[i:])
            if not match or match.group(1) not in mapping:
                raise UnsupportedPlan('Unbound compiled parameter')
            key = match.group(1)
            out.append('$' + mapping[key])
            seen.add(key)
            i += len(match.group(0))
        else:
            out.append(char)
            i += 1
    if seen != set(parameters):
        raise UnsupportedPlan('Unused compiled parameter')
    return ''.join(out), {mapping[k]: deepcopy(v) for k, v in parameters.items()}


def compile_native_set_count(plan, schema, *, selection_compiler=compile_selection,
                             set_contract=_set_contract, identity_kind=None):
    """Use source tags + DB grouping for UNION/INTERSECTION/LEFT DIFFERENCE.

    Both branches remain parameterized and DISTINCT. Grouping source tags
    avoids client ID transfer and does not require a disjointness certificate.
    Excluded shapes are CONTROL_NOT_APPLICABLE, never Ours/model failures.
    The caller must validate actual engine semantics/resources before use.
    """
    object_fields(plan, ['status'], ['calls', 'combine', 'reason'])
    if plan['status'] != 'ANSWER' or not isinstance(plan.get('calls'), list) or len(plan['calls']) != 2:
        raise UnsupportedPlan('Control requires an ANSWER with exactly two sets')
    combine = plan.get('combine')
    object_fields(combine, ['op', 'key', 'output'], ['alias'])
    operation = combine['op']
    if operation not in ('union', 'intersection', 'difference') or combine['output'] != 'count':
        raise UnsupportedPlan('Control supports set count only')
    key = name(combine['key'])
    alias = name(combine.get('alias', 'count'))
    queries, params, identities = [], {}, []
    for index, source in enumerate(plan['calls']):
        compiled = selection_compiler(source, schema)
        if compiled['columns'] != [key]:
            raise UnsupportedPlan('Control requires identity-only set inputs')
        node, label = set_contract(source, compiled, key)
        kind = compiled['contract'].get('identity_fields', {}).get(node, {}).get('type', identity_kind)
        if kind is None:
            raise UnsupportedPlan('Explicit identity kind required for this compiler')
        identities.append((label, kind))
        distinct = deepcopy(source)
        distinct['distinct'] = True
        compiled = selection_compiler(distinct, schema)
        query, branch_params = namespace_parameters(
            compiled['query'], compiled['parameters'], 't1Branch%d_' % index)
        if set(params) & set(branch_params):
            raise UnsupportedPlan('Control parameter namespaces collided')
        params.update(branch_params)
        queries.append('CALL { ' + query + ' } RETURN ' + ident(key) +
                       ' AS t1ControlIdentity, ' + str(index + 1) + ' AS t1ControlSource')
    if identities[0] != identities[1]:
        raise UnsupportedPlan('Control requires the same entity type and identity kind')
    query = ('CALL { ' + ' UNION ALL '.join(queries) + ' } '
             'WITH t1ControlIdentity, count(DISTINCT t1ControlSource) AS t1SourceCount, '
             'min(t1ControlSource) AS t1FirstSource ')
    if operation == 'intersection':
        query += 'WHERE t1SourceCount = 2 '
    elif operation == 'difference':
        query += 'WHERE t1SourceCount = 1 AND t1FirstSource = 1 '
    query += 'RETURN count(*) AS ' + ident(alias)
    return {'query': query, 'parameters': params, 'columns': [alias],
            'control_version': VERSION, 'operation': operation,
            'shared_plan': True, 'extra_model_calls': 0,
            'identity_contract': {'entity_type': identities[0][0], 'kind': identities[0][1]},
            'required_physical_queries': 1, 'engine_acceptance': False,
            'not_applicable_is_not_model_failure': True}
