"""Import-config schema bridge. No questions, graph rows, answers or model input.

Expose the actual primary property as the identity; never add a phantom eid or
coerce a string-valued native property to a numeric type from its CSV contents.
"""
from schema_services import UnsupportedPlan


KINDS = {'STRING': 'str', 'BOOL': 'bool', 'INT8': 'int', 'INT16': 'int',
         'INT32': 'int', 'INT64': 'int', 'FLOAT': 'float', 'DOUBLE': 'float',
         'DATE': 'date', 'DATETIME': 'datetime'}


def schema_from_import_config(config):
    if not isinstance(config, dict) or not isinstance(config.get('schema'), list):
        raise UnsupportedPlan('Native import config needs a schema list')
    entities, relations, declarations = [], [], {}
    def properties(row):
        props, native, nullable = {}, {}, {}
        for prop in row.get('properties', []):
            key = prop['name']
            if not isinstance(key, str) or not key or key in props:
                raise UnsupportedPlan('Invalid or duplicate native property')
            if type(prop.get('optional', False)) is not bool:
                raise UnsupportedPlan('Native optional metadata must be boolean')
            native[key] = prop['type']
            props[key] = KINDS.get(prop['type'], 'native:' + prop['type'])
            nullable[key] = prop.get('optional', False)
        return {'properties': props, 'native_types': native, 'nullable': nullable}
    for row in config['schema']:
        if row['type'] != 'VERTEX':
            continue
        label = row['label']
        if label in declarations:
            raise UnsupportedPlan('Duplicate native vertex label')
        values = properties(row)
        primary = row.get('primary')
        if primary not in values['properties'] or values['nullable'][primary]:
            raise UnsupportedPlan('Native identity must be a declared non-null primary property')
        if values['properties'][primary] not in ('str', 'int'):
            raise UnsupportedPlan('Native primary identity type is not admitted')
        entity = {'label': label, 'identity_property': primary, **values}
        declarations[label] = entity
        entities.append(entity)
    if not entities:
        raise UnsupportedPlan('No native vertex declarations')
    seen = set()
    for row in config['schema']:
        if row['type'] == 'VERTEX':
            continue
        if row['type'] != 'EDGE' or not row.get('constraints'):
            raise UnsupportedPlan('Native edge endpoint constraints must be explicit')
        values = properties(row)
        for pair in row['constraints']:
            if not isinstance(pair, list) or len(pair) != 2 or any(x not in declarations for x in pair):
                raise UnsupportedPlan('Native edge constraint refers to unknown endpoints')
            signature = row['label'], pair[0], pair[1]
            if signature in seen:
                raise UnsupportedPlan('Duplicate native edge signature')
            seen.add(signature)
            relations.append({'label': row['label'], 'subj_label': pair[0],
                              'obj_label': pair[1], **values})
    return {'entities': sorted(entities, key=lambda e: e['label']),
            'relations': sorted(relations, key=lambda r: (r['label'], r['subj_label'], r['obj_label'])),
            'backend_profile': 'tugraph-4.5.2', 'identity_source': 'declared_native_primary',
            'formal_admission': False}
