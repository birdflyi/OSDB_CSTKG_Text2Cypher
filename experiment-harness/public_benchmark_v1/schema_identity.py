"""Compare public schemas without depending on declaration list order."""


def canonical_schema(schema):
    result = dict(schema)
    for section in ['entities', 'relations']:
        rows = schema[section]
        def signature(row):
            return ((row['label'], row['subj_label'], row['obj_label']) if section == 'relations'
                    else (row['label'],))
        keys = [signature(row) for row in rows]
        if len(set(keys)) != len(keys):
            raise ValueError('Duplicate schema declaration signature')
        result[section] = sorted(rows, key=signature)
    return result
