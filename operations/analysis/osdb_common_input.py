"""Pure, versioned OSDB input preparation; no model, database or reference access.

The schema is an observed semantic projection, not a database catalog. Storage
tags, retrieval, runtime enforcement and method adaptation require separate gates.
"""
from copy import deepcopy
import hashlib
import json
import math

VERSION = 'osdb-observed-common-input-dev-1'
NOTICE = (
    'Neo4j 5.15.0 semantic projection. Entity identity is (label,entity_id), '
    'an account/artifact key, not a resolved person. Relationships are directed '
    'physical records; service_rel_type is a property, not a relationship label. '
    'source_event_time is stored as STRING. Use explicit datetime conversion for '
    'instant comparisons; do not substitute event_time or lexical ordering. '
    'Preserve NULL, multiplicity and requested half-open bounds. No implicit LIMIT. '
    'The schema lists observed semantic labels/properties, not population counts '
    'or inferred world knowledge. PilotNode/__pilot_uid are importer storage '
    'metadata; their access policy and full runtime enforcement are not yet admitted. '
    'Empty entity suggestions mean no retrieval has been supplied to either method. '
    'Do not infer complete history, people identity, employment or bot exclusion.')


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False)


def observed_type(value):
    if value is None:
        return 'null'
    if type(value) is bool:
        return 'bool'
    if type(value) is int:
        return 'int'
    if type(value) is float and math.isfinite(value):
        return 'float'
    if type(value) is str:
        return 'str'
    if type(value) is list:
        kinds = sorted({observed_type(v) for v in value})
        return 'list[' + '|'.join(kinds) + ']'
    raise ValueError('Unexpected property representation; do not coerce it')


def property_kind(types):
    nonnull = sorted(set(types) - {'null'})
    return (nonnull[0] if len(nonnull) == 1 else
            'mixed[' + '|'.join(nonnull) + ']' if nonnull else 'null_only')


def _public_declaration(row, relation=False):
    fields = ('label', 'subj_label', 'obj_label', 'properties') if relation else (
        'label', 'identity_property', 'properties')
    # An explicit allowlist strips census counts, source records and audit labels.
    out = {k: deepcopy(row[k]) for k in fields}
    names = ('label', 'subj_label', 'obj_label') if relation else ('label', 'identity_property')
    if any(type(out[k]) is not str or not out[k] for k in names):
        raise ValueError('Invalid schema identifier')
    if type(out['properties']) is not dict or any(
            type(k) is not str or not k or type(v) is not str or not v
            for k, v in out['properties'].items()):
        raise ValueError('Invalid schema properties')
    if not relation and out['properties'].get(out['identity_property']) != 'str':
        raise ValueError('OSDB identity must be a declared string property')
    return out


def common_input(*, task_id, graph, question, schema):
    if any(type(v) is not str or not v.strip() for v in (task_id, graph, question)):
        raise ValueError('Explicit nonempty task identity and original question required')
    entities = [_public_declaration(r) for r in schema['entities']]
    relations = [_public_declaration(r, True) for r in schema['relations']]
    labels = [r['label'] for r in entities]
    signatures = [(r['label'], r['subj_label'], r['obj_label']) for r in relations]
    if len(set(labels)) != len(labels) or len(set(signatures)) != len(signatures):
        raise ValueError('Duplicate schema declaration')
    if any(a not in labels or b not in labels for _, a, b in signatures):
        raise ValueError('Unknown relation endpoint')
    payload = {'task_id': task_id, 'graph': graph, 'question': question,
               'schema': {'entities': entities, 'relations': relations},
               'entity_candidates': [], 'database_notes': NOTICE}
    serialized = canonical(payload)
    return {'payload': payload, 'serialized': serialized,
            'sha256': hashlib.sha256(serialized.encode('utf-8')).hexdigest(),
            'formal_admission': False, 'version': VERSION}
