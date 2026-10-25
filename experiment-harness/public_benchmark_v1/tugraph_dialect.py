"""Development REST renderer for compiler-owned, parameterized selections.

Not a raw Cypher sanitizer, benchmark runner, or formal admission. The official
legacy REST contract exposes script+graph, without a parameter map. Preserve
typed compiler values and encode each literal exactly once; never interpolate
arbitrary query text or perform schema-dependent coercion of reference queries.
"""
import json
import math
import re

from schema_services import compile_selection, UnsupportedPlan


PROFILE = 'tugraph-4.5.2'


def literal(value):
    if type(value) is str:
        return json.dumps(value, ensure_ascii=False)
    if type(value) is bool:
        return 'true' if value else 'false'
    if type(value) is int:
        if not -(2**63) <= value < 2**63:
            raise UnsupportedPlan('Native integer outside signed INT64')
        return str(value)
    if type(value) is float and math.isfinite(value):
        return repr(value)
    if type(value) is list:
        if any(type(item) not in (str, int, float, bool) for item in value):
            raise UnsupportedPlan('Only flat typed lists can be rendered')
        return '[' + ','.join(literal(item) for item in value) + ']'
    raise UnsupportedPlan('Unsupported native literal')


def compile_rest_selection(call, schema):
    compiled = compile_selection(call, schema, backend=PROFILE)
    query = compiled['query']
    # This renderer only accepts output of the compiler above, which has plain
    # identifiers and no string literals. Literal text is never scanned again.
    if any(mark in query for mark in ('"', "'", '`', '//', '/*', ';')):
        raise UnsupportedPlan('Unexpected token in compiler-owned query')
    seen = set()
    def replace(match):
        key = match.group(1)
        if key not in compiled['parameters']:
            raise UnsupportedPlan('Missing native parameter')
        seen.add(key)
        return literal(compiled['parameters'][key])
    script = re.sub(r'\$([A-Za-z_][A-Za-z0-9_]*)', replace, query)
    if seen != set(compiled['parameters']):
        raise UnsupportedPlan('Unused native parameter')
    return {**compiled, 'script': script, 'backend_profile': PROFILE,
            'formal_admission': False}
