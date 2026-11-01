"""Lossless CSV record layer, separate from validated entity identities.

One node represents one source CSV row, not one distinct event or entity.
Duplicate rows remain distinct. No raw identifier becomes a validated entity.
"""
import csv
import hashlib
import json
from pathlib import Path

VERSION = 'osdb_raw_record_v1'
FIELDS = ('src_entity_id', 'src_entity_type', 'tar_entity_id', 'tar_entity_type',
          'relation_label_repr', 'relation_type', 'event_id', 'event_type', 'event_time')


def records(source, expected_sha256):
    source = Path(source)
    if hashlib.sha256(source.read_bytes()).hexdigest() != expected_sha256:
        raise ValueError('Raw source SHA mismatch')
    with source.open(encoding='utf-8-sig', newline='') as stream:
        for index, row in enumerate(csv.DictReader(stream)):
            if None in row or any(value is None for value in row.values()):
                raise ValueError('Malformed CSV row at index '+str(index))
            missing = set(FIELDS)-set(row)
            if missing:
                raise ValueError('Missing source fields: '+str(sorted(missing)))
            raw = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
            uid = f'{VERSION}:{expected_sha256}:{index}'
            properties = {key: row[key] for key in FIELDS}
            properties.update(record_uid=uid, source_sha256=expected_sha256, record_index=index,
                              raw_record_json=raw, raw_record_sha256=hashlib.sha256(raw.encode('utf8')).hexdigest(),
                              record_layer_version=VERSION)
            yield dict(node_uid=uid, label='OSDBRawRecord', properties_json=json.dumps(properties, ensure_ascii=False))


def extend_nodes(source, expected_sha256, entity_nodes, output):
    """Write a new combined node CSV; original entity projection stays unchanged."""
    count = 0
    with Path(entity_nodes).open(encoding='utf-8-sig', newline='') as original, Path(output).open('x', encoding='utf8', newline='') as target:
        reader = csv.DictReader(original)
        if not {'node_uid', 'label', 'properties_json'} <= set(reader.fieldnames or []):
            raise ValueError('Unsupported entity-node CSV columns')
        writer = csv.DictWriter(target, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            if row['label']=='OSDBRawRecord':
                raise ValueError('Record layer already present')
            writer.writerow(row)
        for row in records(source, expected_sha256):
            writer.writerow(row)
            count += 1
    return count
