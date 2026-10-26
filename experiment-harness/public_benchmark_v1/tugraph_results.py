"""Native JSON result validation and a strict DIAGNOSTIC comparator.

This is not the author's EA implementation, not a primary metric freeze, and
does not establish REST/Bolt equivalence. Never infer types from header type=0
or coerce DATE/DATETIME strings into invented typed database objects.
"""
from collections import Counter
from copy import deepcopy
import json
import math

from schema_services import IncompleteEvidence


MAX_ROWS = 1000000
MAX_BYTES = 64*1024**2


class MalformedNativeResult(ValueError):
    pass


def typed_value(value, depth=0):
    if depth > 32:
        raise MalformedNativeResult('Nested result exceeds the declared depth')
    if value is None:return ('null',)
    if type(value) is bool:return ('bool',value)
    if type(value) is int:return ('int',str(value))
    if type(value) is float:
        if not math.isfinite(value):raise MalformedNativeResult('Non-finite JSON number')
        return ('float',value.hex())
    if type(value) is str:return ('str',value)
    if type(value) is list:return ('list',tuple(typed_value(v,depth+1) for v in value))
    if type(value) is dict and all(type(k) is str for k in value):
        return ('map',tuple((k,typed_value(value[k],depth+1)) for k in sorted(value)))
    raise MalformedNativeResult('Non-JSON result type')


def validate_rows(columns, rows):
    if type(columns) is not list or any(type(c) is not str for c in columns):
        raise MalformedNativeResult('Invalid result columns')
    if type(rows) is not list or any(type(r) is not list or len(r)!=len(columns) for r in rows):
        raise MalformedNativeResult('Result row width does not match columns')
    if len(rows) >= MAX_ROWS:raise IncompleteEvidence('Result reaches common row cap')
    for row in rows:
        for value in row:typed_value(value)


def parse_rest_result(raw, *, transport_complete, execution_complete=False):
    if transport_complete is not True:raise IncompleteEvidence('HTTP response completeness unknown')
    if execution_complete is not True:raise IncompleteEvidence('Database execution completeness unknown')
    if type(raw) is not bytes:raise MalformedNativeResult('Expected original response bytes')
    if len(raw)>=MAX_BYTES:raise IncompleteEvidence('Result reaches common byte cap')
    def invalid_constant(value):raise MalformedNativeResult('Non-JSON constant: '+value)
    try:
        body=json.loads(raw.decode('utf-8'),parse_constant=invalid_constant)
    except (UnicodeDecodeError,json.JSONDecodeError) as exc:
        raise MalformedNativeResult('Invalid UTF-8 JSON response') from exc
    if type(body) is not dict or 'error_message' in body:
        raise MalformedNativeResult('Response is not a successful native result')
    if type(body.get('header')) is not list or any(type(h) is not dict or type(h.get('name')) is not str for h in body['header']):
        raise MalformedNativeResult('Invalid native header')
    columns=[h['name'] for h in body['header']]
    rows=body.get('result')
    validate_rows(columns,rows)
    if type(body.get('size')) is not int or body['size']!=len(rows):
        raise MalformedNativeResult('Native reported size disagrees with rows')
    return {'status':'EXECUTED','complete':True,'columns':columns,'rows':deepcopy(rows),
            'response_bytes':len(raw),'representation':'native-json-uncoerced',
            'author_bolt_equivalence_established':False,'formal_metric_frozen':False}


def strict_auxiliary_equal(reference, prediction, *, ordered):
    """Type-sensitive sensitivity check; order is explicit, never chosen by output."""
    if type(ordered) is not bool:raise MalformedNativeResult('Explicit task-order flag required')
    if any(r.get('status')!='EXECUTED' or r.get('complete') is not True for r in (reference,prediction)):
        return False
    for result in (reference,prediction):validate_rows(result['columns'],result['rows'])
    if len(reference['columns'])!=len(prediction['columns']):return False
    a=[tuple(typed_value(v) for v in row) for row in reference['rows']]
    b=[tuple(typed_value(v) for v in row) for row in prediction['rows']]
    return a==b if ordered else Counter(a)==Counter(b)
