"""Explicit OSDB scalar-output contract. No query execution or gold in prompts."""
from collections import Counter
import json
import math


def validate_contract(contract):
    if contract.get('kind') not in ('list', 'rows', 'count'):
        raise ValueError('Unknown OSDB output kind')
    columns, types = contract.get('columns'), contract.get('types')
    if not columns or len(set(columns)) != len(columns) or len(columns) != len(types or []):
        raise ValueError('Explicit unique columns and types required')
    if any(t not in ('string', 'integer', 'number', 'boolean') for t in types):
        raise ValueError('Unsupported output type')
    if contract.get('order') not in ('ordered', 'bag') or contract.get('duplicates') not in ('retain', 'forbid'):
        raise ValueError('Explicit ordering and duplicate policy required')
    if contract['kind'] in ('count', 'list') and len(columns) != 1:
        raise ValueError('Count/list must have one column')
    if contract['kind'] == 'count' and types != ['integer']:
        raise ValueError('Count must be integer')


def normalized(answer, contract):
    validate_contract(contract)
    if answer.get('status') != 'EXECUTED' or answer.get('complete') is not True:
        raise ValueError('Incomplete execution is not an empty answer')
    if answer.get('columns') != contract['columns'] or not isinstance(answer.get('rows'), list):
        raise ValueError('Columns/rows differ from contract')
    result = []
    for row in answer['rows']:
        if not isinstance(row, list) or len(row) != len(contract['types']):
            raise ValueError('Invalid row width')
        for value, kind in zip(row, contract['types']):
            allowed = {'string': (str,), 'integer': (int,), 'number': (int, float), 'boolean': (bool,)}[kind]
            if type(value) not in allowed or (type(value) is float and not math.isfinite(value)):
                raise ValueError('Output value has wrong type')
        result.append(json.dumps(row, ensure_ascii=False, separators=(',', ':'), allow_nan=False))
    if contract['duplicates'] == 'forbid' and len(set(result)) != len(result):
        raise ValueError('Unexpected duplicate rows')
    if contract['kind'] == 'count' and (len(answer['rows']) != 1 or answer['rows'][0][0] < 0):
        raise ValueError('Count must be one nonnegative integer, including zero')
    return result


def compare(reference, prediction, contract):
    left = normalized(reference, contract)
    try:
        right = normalized(prediction, contract)
    except ValueError:
        return False
    return left == right if contract['order'] == 'ordered' else Counter(left) == Counter(right)
