"""Shared-information G/S development experiment. No gold-aware repair."""
import json
import re
import time

import run_baselines as b
from mechanism_core import CATALOG, compile_call

COMMON = """Retrieve the requested OSS graph facts. Preserve scope, fields, duplicates,
time bounds and aggregation grain. Return ONLY one JSON object. Do not repeat
the request ID or add metadata/reason/columns. The runner owns that metadata.
Status is ANSWER, NEEDS_INPUT, UNSUPPORTED, INSUFFICIENT_EVIDENCE, or ERROR.
Never guess a missing anchor or silently omit an unsupported condition.
For a requested complete list or count, do not add a sampling LIMIT.
Business notation such as EVENT_ACTION[OPENED_BY] is explanatory notation,
NOT Cypher syntax. The meaning is encoded in r.service_rel_type.
Use node.entity_id, edge.url_domain_etld1 and edge.source_event_time.
Dates on the graph are ISO UTC strings, with inclusive start/exclusive end.
"""
PROMPTS = {
    'G': COMMON + """Exact keys: status,cypher,parameters. For ANSWER provide one
read-only MATCH query and an object of parameters. Return the public columns
in their requested order. Otherwise cypher and parameters must both be null.
""",
    'S': COMMON + """Exact keys: status,operation,arguments. For ANSWER select one
listed service and give every required argument, with no additional arguments.
Otherwise operation and arguments must both be null. A valid call does not
prove that all conditions in the original request have been retained.
""",
}


def examples():
    """Fictional, fixed schematic examples; not selected from evaluation rows."""
    calls = [
        ('Opening actors of Issue I_900001#2; keep records.',
         {'operation': 'issue_openers', 'arguments': {'issue_id': 'I_900001#2'}}),
        ('Domains then target IDs of links from PR_900001#3; keep records.',
         {'operation': 'pr_links', 'arguments': {'pr_id': 'PR_900001#3', 'fields': ['domain', 'external_resource_id'], 'distinct': False}}),
        ('Count link records per domain for PR_900001#3 from 2021-01-01 inclusive to 2022-01-01 exclusive.',
         {'operation': 'pr_domain_counts', 'arguments': {'pr_id': 'PR_900001#3', 'start_date': '2021-01-01', 'end_date': '2022-01-01', 'grain': 'records'}}),
        ('Count Issues with literal identifier prefix I_900001#.',
         {'operation': 'issue_count', 'arguments': {'issue_prefix': 'I_900001#'}}),
        ('Count comment-to-Issue and same-comment-to-Commit path records for Issue prefix I_900001#.',
         {'operation': 'issue_commit_path_count', 'arguments': {'issue_prefix': 'I_900001#', 'grain': 'paths'}}),
        ('Count link records from PR_900001#3 from 2021-01-01 inclusive to 2022-01-01 exclusive.',
         {'operation': 'pr_link_count', 'arguments': {'pr_id': 'PR_900001#3', 'start_date': '2021-01-01', 'end_date': '2022-01-01', 'grain': 'records'}}),
    ]
    result = []
    for question, call in calls:
        c = compile_call(call)
        result.append({'question': question, 'columns': c['columns'],
                       'direct': {'status': 'ANSWER', 'cypher': c['query'], 'parameters': c['parameters']},
                       'service': {'status': 'ANSWER', **call}})
    return result


def public_input(spec):
    return {'schema': b.SCHEMA, 'contracts': CATALOG, 'paired_schematic_examples': examples(),
            'task': {k: spec[k] for k in ('question', 'columns')}}


def parse(text, representation):
    text = text.strip()
    if '```' in text:
        blocks = re.findall(r'```(?:json)?\s*([\s\S]*?)\s*```', text)
        if len(blocks) != 1:
            raise ValueError('Expected one JSON block')
        text = blocks[0]
    p = json.loads(text)
    if isinstance(p, list) and len(p) == 1:
        p = p[0]
    keys = {'status', 'cypher', 'parameters'} if representation == 'G' else {'status', 'operation', 'arguments'}
    if not isinstance(p, dict) or set(p) != keys or p['status'] not in b.STATUSES:
        raise ValueError('Invalid keys or status; expected ' + ','.join(sorted(keys)))
    payload_keys = keys - {'status'}
    if p['status'] != 'ANSWER' and any(p[k] is not None for k in payload_keys):
        raise ValueError('Nonanswer payload must be null')
    if p['status'] == 'ANSWER':
        string_key, dict_key = ('cypher', 'parameters') if representation == 'G' else ('operation', 'arguments')
        if not isinstance(p[string_key], str) or not isinstance(p[dict_key], dict):
            raise ValueError('Answer requires a string and a parameter object')
    return p


def read_guard(query):
    if not isinstance(query, str):
        return 'Query must be a string'
    query = query.strip().removesuffix(';').strip()
    # Mask literals before identifying write keywords; preserve identifier syntax.
    masked = re.sub(r"'(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\"", "''", query)
    if not re.match(r'^MATCH\b', masked, re.I) or re.search(
        r';|//|/\*|\b(CREATE|MERGE|DELETE|DETACH|SET|REMOVE|DROP|CALL|LOAD|FOREACH|UNION|USE)\b', masked, re.I):
        return 'Only one read-only MATCH query is allowed'
    return None


def transaction(query, parameters, timeout=45):
    started = time.monotonic()
    r = b.LOCAL.post(b.DB_URL, json={'statements': [{'statement': query, 'parameters': parameters,
                            'resultDataContents': ['row']}]}, timeout=(3, timeout))
    r.raise_for_status()
    body = r.json()
    return body, time.monotonic() - started, len(r.content)


def validate(prediction, representation):
    """Structural feedback only. No spec, expected answer, or reference call."""
    if prediction['status'] != 'ANSWER':
        return {'status': 'NONANSWER', 'repairable': False}
    try:
        if representation == 'S':
            c = compile_call({k: prediction[k] for k in ('operation', 'arguments')})
            query, parameters = c['query'], c['parameters']
        else:
            query, parameters = prediction['cypher'], prediction['parameters']
        error = read_guard(query)
        if error:
            return {'status': 'READ_GUARD_ERROR', 'repairable': True, 'feedback': error}
        query = query.strip().removesuffix(';').strip()
        body, seconds, nbytes = transaction('EXPLAIN ' + query, parameters)
        if body.get('errors'):
            return {'status': 'SCHEMA_SYNTAX_ERROR', 'repairable': True, 'feedback': body['errors']}
        missing = [n for n in body.get('notifications', []) if any(
            k in n.get('code', '') for k in ('UnknownPropertyKeyWarning', 'UnknownLabelWarning', 'UnknownRelationshipTypeWarning'))]
        if missing:
            return {'status': 'SCHEMA_WARNING', 'repairable': True, 'feedback': missing}
        return {'status': 'VALID', 'repairable': False, 'query': query, 'parameters': parameters,
                'explain_seconds': seconds, 'explain_bytes': nbytes,
                'notifications': body.get('notifications', [])}
    except (ValueError, TypeError, KeyError) as e:
        return {'status': 'CONTRACT_ERROR', 'repairable': True, 'feedback': str(e)}


def evaluate(prediction, representation):
    check = validate(prediction, representation)
    if check['status'] != 'VALID':
        return {'check': check, 'execution': {'status': 'NOT_EXECUTED'}}
    body, seconds, nbytes = transaction(check['query'], check['parameters'])
    if body.get('errors'):
        return {'check': {'status': 'EXECUTION_ERROR', 'repairable': True, 'feedback': body['errors']},
                'execution': {'status': 'EXECUTION_ERROR', 'seconds': seconds, 'bytes': nbytes}}
    r = body['results'][0]
    over = len(r['data']) > 500
    return {'check': check, 'execution': {'status': 'OUTPUT_BUDGET_ERROR' if over else 'EXECUTED',
            'columns': r['columns'], 'rows': [] if over else [x['row'] for x in r['data']],
            'returned_rows': len(r['data']), 'seconds': seconds, 'bytes': nbytes}}
