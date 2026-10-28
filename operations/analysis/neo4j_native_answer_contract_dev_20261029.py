"""Typed capture of materialized Neo4j answers; no database or model I/O.

This is a preparation/auxiliary contract, not either benchmark's official
scorer. Native element IDs are scoped to a capture, never presumed permanent.
The caller supplies a frozen graph snapshot and an actual capture identity.
"""

from collections import Counter
from dataclasses import asdict, dataclass
import datetime
import json
import re
import struct

import neo4j
from neo4j import Record
from neo4j.graph import Node, Relationship, Path
from neo4j.spatial import Point
from neo4j.time import Date, Time, DateTime, Duration


PROFILE = 'neo4j-5.28.1-typed-answer-preparation-v1'


class UnsupportedNativeValue(ValueError):
    pass


class IdentityScopeConflict(ValueError):
    pass


@dataclass(frozen=True)
class CaptureScope:
    """capture_id names one audited read transaction, not a graph/batch/worker.

    Driver records do not themselves prove transaction ownership. The future
    executor must supply and preserve that binding before this is admitted.
    """

    snapshot_sha256: str
    database: str
    capture_id: str

    def __post_init__(self):
        if type(self.snapshot_sha256) is not str or not re.fullmatch('[0-9a-f]{64}', self.snapshot_sha256):
            raise ValueError('An explicit frozen snapshot digest is required')
        for field in (self.database, self.capture_id):
            if type(field) is not str or not field:
                raise ValueError('Database and capture identity must be explicit')


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def _clock(value):
    native = isinstance(value, (Time, DateTime))
    offset = value.utcoffset()
    zone = getattr(value.tzinfo, 'key', None) or getattr(value.tzinfo, 'zone', None)
    return {'hour': value.hour, 'minute': value.minute, 'second': value.second,
            'nanosecond': value.nanosecond if native else value.microsecond * 1000,
            'offset': None if offset is None else [offset.days, offset.seconds, offset.microseconds],
            'zone': zone}


class _Encoder:
    def __init__(self):
        self.contains_entities = False

    def reference(self, entity):
        if not isinstance(entity, (Node, Relationship)):
            raise UnsupportedNativeValue('Missing native graph endpoint')
        if type(entity.element_id) is not str or not entity.element_id:
            raise UnsupportedNativeValue('Missing native element identity; no numeric/property fallback')
        self.contains_entities = True
        return entity.element_id

    def value(self, value, depth=0):
        if depth > 64:
            raise UnsupportedNativeValue('Nested native value exceeds the declared structural bound')
        child = lambda v: self.value(v, depth + 1)
        if value is None:
            return {'t': 'null'}
        if type(value) is bool:
            return {'t': 'bool', 'v': value}
        if type(value) is int:
            return {'t': 'int', 'v': str(value)}
        if type(value) is float:
            # IEEE754 bytes preserve signed zero and nonfinite payloads without
            # invalid JSON numbers or rounding a value to a decimal display.
            return {'t': 'float64', 'v': struct.pack('!d', value).hex()}
        if type(value) is str:
            return {'t': 'str', 'v': value}
        if isinstance(value, (bytes, bytearray, memoryview)):
            return {'t': 'bytes', 'v': bytes(value).hex()}
        if isinstance(value, Node):
            return {'t': 'node', 'element_id': self.reference(value),
                    'labels': sorted(value.labels), 'properties': child(dict(value))}
        if isinstance(value, Relationship):
            return {'t': 'relationship', 'element_id': self.reference(value),
                    'type': value.type, 'start': self.reference(value.start_node),
                    'end': self.reference(value.end_node), 'properties': child(dict(value))}
        if isinstance(value, Path):
            nodes, relationships = value.nodes, value.relationships
            if len(nodes) != len(relationships) + 1:
                raise UnsupportedNativeValue('Malformed native path length')
            directions = []
            for node, relationship in zip(nodes[:-1], relationships):
                if self.reference(node) == self.reference(relationship.start_node):
                    directions.append(1)
                elif self.reference(node) == self.reference(relationship.end_node):
                    directions.append(-1)
                else:
                    raise UnsupportedNativeValue('Disconnected native path')
            return {'t': 'path', 'nodes': [child(n) for n in nodes],
                    'relationships': [child(r) for r in relationships], 'directions': directions}
        if isinstance(value, Point):
            return {'t': 'point', 'srid': value.srid, 'coordinates': [child(v) for v in value]}
        if isinstance(value, (DateTime, datetime.datetime)):
            return {'t': 'datetime', 'year': value.year, 'month': value.month,
                    'day': value.day, **_clock(value)}
        if isinstance(value, (Time, datetime.time)):
            return {'t': 'time', **_clock(value)}
        if isinstance(value, (Date, datetime.date)):
            return {'t': 'date', 'year': value.year, 'month': value.month, 'day': value.day}
        if isinstance(value, Duration):
            return {'t': 'duration', 'months': value.months, 'days': value.days,
                    'seconds': value.seconds, 'nanoseconds': value.nanoseconds}
        if isinstance(value, datetime.timedelta):
            # Python's fixed timedelta is not silently equated with Neo4j's
            # calendar duration (months/days can depend on date/time context).
            return {'t': 'python_timedelta', 'days': value.days,
                    'seconds': value.seconds, 'microseconds': value.microseconds}
        if isinstance(value, dict):
            if any(type(k) is not str for k in value):
                raise UnsupportedNativeValue('Native map keys must be strings')
            return {'t': 'map', 'v': [[k, child(value[k])] for k in sorted(value)]}
        if type(value) in (list, tuple):
            return {'t': 'list', 'v': [child(v) for v in value]}
        raise UnsupportedNativeValue('Unsupported native value type: ' + type(value).__name__)


def freeze_answer(columns, rows, scope, *, complete, max_bytes):
    """Capture already materialized rows positionally; Record.data loses types.

    No streaming Result or query executor is accepted. Complete must come from
    audited execution evidence. Output overflow raises, never slices the rows.
    The byte cap bounds serialized evidence, not server or Python input memory.
    """
    if neo4j.__version__ != '5.28.1':
        raise ValueError('This native profile requires the pinned Neo4j5.28.1 driver')
    if not isinstance(scope, CaptureScope) or type(complete) is not bool:
        raise ValueError('Explicit capture scope and execution completeness required')
    if type(max_bytes) is not int or max_bytes <= 0:
        raise ValueError('An explicit positive evidence byte cap is required')
    if type(columns) is not list or any(type(c) is not str for c in columns):
        raise ValueError('Ordered raw column headers are required')
    if type(rows) not in (list, tuple):
        raise ValueError('Rows must already be materialized; no live Result iterator')
    encoder, result_rows = _Encoder(), []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) != len(columns):
            raise ValueError('Malformed row width or representation')
        if isinstance(row, Record) and list(row.keys()) != columns:
            raise ValueError('Record headers do not match their raw positional contract')
        result_rows.append([encoder.value(row[i]) for i in range(len(columns))])
    answer = {'profile': PROFILE, 'scope': asdict(scope), 'columns': list(columns),
              'rows': result_rows, 'complete': complete,
              'status': 'EXECUTED' if complete else 'INCOMPLETE_ANSWER',
              'contains_graph_entities': encoder.contains_entities,
              'cross_capture_entity_identity_certified': False,
              'formal_metric_frozen': False}
    if len(encoded(answer)) > max_bytes:
        raise UnsupportedNativeValue('Serialized answer exceeds the evidence byte cap; no truncation')
    return answer


def compare_complete_auxiliary(reference, prediction, *, ordered):
    """Exact typed auxiliary comparison of this encoder's verified records.

    Raw aliases remain in evidence; row values compare by column position/width.
    Row order is an explicit caller contract, not inferred from model output.
    Unknown/incomplete answers return None, never equal empty results.
    This function does not convert failures into a benchmark primary score.
    """
    if type(ordered) is not bool:
        raise ValueError('Ordering must be explicitly fixed outside predictions')
    if reference is None or prediction is None:
        return None
    for answer in (reference, prediction):
        if answer.get('status') != 'EXECUTED' or answer.get('complete') is not True:
            return None
        if answer.get('profile') != PROFILE:
            raise ValueError('Foreign typed answer profile')
    left, right = reference['scope'], prediction['scope']
    if any(left[field] != right[field] for field in ('snapshot_sha256', 'database')):
        raise IdentityScopeConflict('Answers come from different graph snapshots/databases')
    if (reference['contains_graph_entities'] or prediction['contains_graph_entities']) and left['capture_id'] != right['capture_id']:
        raise IdentityScopeConflict('Cross-capture element identity has not been certified')
    if len(reference['columns']) != len(prediction['columns']):
        return False
    a, b = reference['rows'], prediction['rows']
    return a == b if ordered else Counter(map(encoded, a)) == Counter(map(encoded, b))
