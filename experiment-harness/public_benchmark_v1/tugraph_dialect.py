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


def render_compiler_query(query, parameters):
    """Internal compiler callback; never an entrypoint for raw model queries."""
    # Selection and set-count compilers emit plain or backtick-quoted ASCII
    # identifiers, and no string literals. Validate quotations before replacing
    # parameter tokens; user literal values are encoded once and never rescanned.
    unquoted = re.sub(r"`[A-Za-z_][A-Za-z0-9_]*`", "identifier", query)
    if "``" in query or any(mark in unquoted for mark in ('"', "'", '`', '//', '/*', ';')):
        raise UnsupportedPlan('Unexpected token in compiler-owned query')
    seen = set()
    def replace(match):
        key = match.group(1)
        if key not in parameters:
            raise UnsupportedPlan('Missing native parameter')
        seen.add(key)
        return literal(parameters[key])
    script = re.sub(r'\$([A-Za-z_][A-Za-z0-9_]*)', replace, query)
    if seen != set(parameters):
        raise UnsupportedPlan('Unused native parameter')
    return script


def compile_rest_selection(call, schema):
    compiled = compile_selection(call, schema, backend=PROFILE)
    script = render_compiler_query(compiled['query'], compiled['parameters'])
    return {**compiled, 'script': script, 'backend_profile': PROFILE,
            'formal_admission': False}
