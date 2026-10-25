"""Pinned Cypher task lexical profile, not an AST/support/correctness oracle.

Ignore comments and string contents. Preserve quoted identifier names.
Never execute queries, author code or models; never modify source records.
"""
from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import zlib

BASE = Path('D:/ch7/literature/public_candidate_metadata_20261024/Text2GQL-Bench')
OUT = Path('D:/ch7/reports/text2gql_query_feature_profile_20261025.json')


def tokens(text):
    out = []
    i = 0
    while i < len(text):
        c = text[i]
        if c.isspace():
            i += 1
        elif text.startswith('//', i):
            end = text.find('\n', i + 2)
            i = len(text) if end < 0 else end
        elif text.startswith('/*', i):
            end = text.find('*/', i + 2)
            if end < 0:
                raise ValueError('Unclosed comment')
            i = end + 2
        elif c in "'\"`":
            quote = c
            i += 1
            value = []
            closed = False
            while i < len(text):
                if text[i] == '\\' and quote != '`':
                    i += 2
                elif text[i] == quote:
                    i += 1
                    if i < len(text) and text[i] == quote:
                        value.append(quote)
                        i += 1
                    else:
                        closed = True
                        break
                else:
                    value.append(text[i])
                    i += 1
            if not closed:
                raise ValueError('Unclosed literal or identifier')
            out.append(('quoted_identifier', ''.join(value)) if quote == '`' else ('literal', ''))
        elif c.isalpha() or c == '_':
            j = i + 1
            while j < len(text) and (text[j].isalnum() or text[j] == '_'):
                j += 1
            out.append(('word', text[i:j]))
            i = j
        else:
            out.append(('punctuation', c))
            i += 1
    return out


def profile(query, temporal_names, edge_only_names):
    ts = tokens(query)
    def word(i, value):
        return i < len(ts) and ts[i][0] == 'word' and ts[i][1].upper() == value
    def keyword(i, value):
        return word(i, value) and (i == 0 or ts[i - 1][1] not in ('.', ':', '$'))
    functions = {t[1].lower() for i, t in enumerate(ts[:-1])
                 if t[0] == 'word' and ts[i + 1][1] == '(' and
                 (i == 0 or ts[i - 1][1] not in (':', '$'))}
    properties = {t[1] for i, t in enumerate(ts) if i > 0 and ts[i - 1][1] == '.'
                  and t[0] in ('word', 'quoted_identifier')}
    aggregate = functions & {'count', 'sum', 'avg', 'min', 'max', 'collect'}
    flags = {name: any(keyword(i, name) for i in range(len(ts)))
             for name in ['DISTINCT', 'WITH', 'UNWIND', 'UNION', 'OR', 'NOT', 'LIMIT']}
    flags['OPTIONAL_MATCH'] = any(keyword(i, 'OPTIONAL') and word(i + 1, 'MATCH') for i in range(len(ts)))
    flags['ORDER_BY'] = any(keyword(i, 'ORDER') and word(i + 1, 'BY') for i in range(len(ts)))
    flags['CALL_BRACE'] = any(keyword(i, 'CALL') and i + 1 < len(ts) and ts[i + 1][1] == '{' for i in range(len(ts)))
    flags['EXISTS_BRACE'] = any(keyword(i, 'EXISTS') and i + 1 < len(ts) and ts[i + 1][1] == '{' for i in range(len(ts)))
    flags['aggregate_function_token'] = bool(aggregate)
    flags['count_function_token'] = 'count' in functions
    flags['temporal_function_token'] = bool(functions & {'date', 'datetime', 'localdatetime', 'time', 'localtime', 'duration', 'timestamp'})
    flags['declared_temporal_property_name_access'] = bool(properties & temporal_names)
    flags['edge_only_declared_property_name_access'] = bool(properties & edge_only_names)
    return {'flags': flags, 'aggregate_function_names': sorted(aggregate),
            'temporal_property_names_hit': sorted(properties & temporal_names),
            'edge_only_property_names_hit': sorted(properties & edge_only_names)}


def read_asset(member, inventory):
    entry = inventory[member]
    raw = (BASE / 'selected_zip_assets_20261024' / member).read_bytes()
    assert len(raw) == entry['bytes']
    assert hashlib.sha256(raw).hexdigest() == entry['sha256']
    assert f'{zlib.crc32(raw) & 0xffffffff:08x}' == entry['crc32']
    return json.loads(raw)


def run():
    # Measurement checks: literal/comment words must not inflate counts; quoted
    # property identifiers remain visible; property names are case-sensitive.
    p = profile("RETURN 'UNION COUNT(n)' // WITH\n", {'time'}, {'cost'})
    assert not any(p['flags'].values())
    p = profile('/* OPTIONAL MATCH */ RETURN n.`time`, n.cost', {'time'}, {'cost'})
    assert not p['flags']['OPTIONAL_MATCH'] and p['flags']['declared_temporal_property_name_access']
    assert p['flags']['edge_only_declared_property_name_access']
    assert not profile('RETURN n.Time', {'time'}, set())['flags']['declared_temporal_property_name_access']
    snap = json.loads((BASE / 'cypher_test_snapshot_inventory_20261024.json').read_bytes())
    inventory = {x['member']: x for x in snap['assets']}
    files = [m for m in inventory if m.endswith('.json') and m.count('/') == 4]
    assert len(files) == 6
    graphs = {}
    identities = set()
    task_receipts = []
    for member in sorted(files):
        graph = member.split('/')[2]
        rows = read_asset(member, inventory)
        configs = [m for m in inventory if m.startswith(str(Path(member).parent).replace('\\', '/') + '/')
                   and m.endswith('/import_config.json')]
        assert len(configs) == 1
        schema = read_asset(configs[0], inventory)['schema']
        temporal = {p['name'] for label in schema for p in label.get('properties', [])
                    if p['type'] in ('DATE', 'DATETIME')}
        vertex_props = {p['name'] for label in schema if label['type'] == 'VERTEX' for p in label.get('properties', [])}
        edge_props = {p['name'] for label in schema if label['type'] == 'EDGE' for p in label.get('properties', [])}
        edge_only = edge_props - vertex_props
        counts = Counter()
        parse_errors = []
        for row in rows:
            qid = str(row['id'])
            assert (graph, qid) not in identities
            identities.add((graph, qid))
            query = row['initial_gql']
            try:
                result = profile(query, temporal, edge_only)
                counts.update(k for k, v in result['flags'].items() if v)
                task_receipts.append({'graph': graph, 'id': qid, 'query_sha256': hashlib.sha256(query.encode()).hexdigest(), **result})
            except ValueError as exc:
                parse_errors.append({'id': qid, 'error': str(exc)})
        graphs[graph] = {'n': len(rows), 'lexically_profiled': len(rows) - len(parse_errors),
                         'lexical_errors': parse_errors, 'query_file_sha256': inventory[member]['sha256'],
                         'schema_file_sha256': inventory[configs[0]]['sha256'],
                         'declared_temporal_property_names': sorted(temporal),
                         'declared_edge_only_property_names': sorted(edge_only), 'flag_counts': dict(counts)}
    assert len(identities) == 3826 and len(task_receipts) + sum(len(g['lexical_errors']) for g in graphs.values()) == 3826
    out = {'kind': 'OFFLINE_REFERENCE_QUERY_LEXICAL_PROFILE_NOT_ADMISSION',
           'time_local': datetime.now(timezone(timedelta(hours=8))).isoformat(), 'graphs': graphs,
           'total_records': 3826, 'task_receipts': task_receipts,
           'source_object_etag': snap['object_etag'], 'whole_zip_sha256_known': False,
           'matched_asset_hashes_and_crc': True, 'new_model_calls': 0, 'new_database_queries': 0,
           'author_code_executed': False, 'runtime_method_changed': False,
           'formal_task_feature_labels': False, 'formal_admission_granted': False,
           'limits': ['Not a Cypher parser, variable binder, type checker or EX scorer.',
                      'Property-name hits do not prove variable binding, valid queries or use in temporal filters.',
                      'COUNT/DISTINCT/OR tokens do not prove a summary-selection mechanism is applicable.',
                      'Zero hits do not prove an absence of semantic temporal intent.',
                      'No task deleted, label imputed, outcome read or existing frozen method modified.']}
    OUT.write_text(json.dumps(out, indent=2), encoding='utf-8')
    print(json.dumps({'total_records': 3826, 'graphs': {k: {'n': v['n'], 'errors': len(v['lexical_errors']), 'counts': v['flag_counts']} for k, v in graphs.items()}}))


if __name__ == '__main__':
    run()
