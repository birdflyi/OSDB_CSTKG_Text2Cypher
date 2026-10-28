"""Native driver-object fixtures only; no driver connection or graph queries."""

import argparse
from dataclasses import replace
import datetime
from decimal import Decimal
import json
from pathlib import Path as FilePath
import socket
import struct
import unittest
from unittest.mock import patch

from neo4j import Record
from neo4j.graph import Graph, Node, Path
from neo4j.spatial import CartesianPoint, WGS84Point
from neo4j.time import Date, Time, DateTime, Duration

from neo4j_native_answer_contract_dev_20261029 import (
    CaptureScope, IdentityScopeConflict, UnsupportedNativeValue,
    compare_complete_auxiliary, encoded, freeze_answer,
)


class NativeAnswerChecks(unittest.TestCase):
    def setUp(self):
        self.scope = CaptureScope('a' * 64, 'synthetic_database', 'synthetic_read_capture')
        self.graph = Graph()
        self.a = Node(self.graph, 'node_a', 1, ['Person'], {'name': 'SameName'})
        self.b = Node(self.graph, 'node_b', 2, ['Person'], {'name': 'SameName'})

    def answer(self, rows, *, columns=None, scope=None, complete=True, max_bytes=100000):
        if columns is None:
            columns = ['value']
        return freeze_answer(columns, rows, scope or self.scope, complete=complete, max_bytes=max_bytes)

    def same(self, a, b, *, ordered=False):
        return compare_complete_auxiliary(a, b, ordered=ordered)

    def relationship(self, name='KNOWS', element_id='rel_1', reverse=False):
        # The public SDK has no endpoint constructor. These two fixture-only
        # assignments match its pinned hydration representation; production
        # code reads only public properties and never mutates driver objects.
        rel = self.graph.relationship_type(name)(self.graph, element_id, 3, {'weight': 1})
        rel._start_node, rel._end_node = (self.b, self.a) if reverse else (self.a, self.b)
        return rel

    def test_bool_int_float_string_null_stay_distinct(self):
        values = [True, 1, 1.0, '1', None]
        encoded_values = [self.answer([[v]])['rows'][0][0] for v in values]
        self.assertEqual(len({encoded(v) for v in encoded_values}), len(values))

    def test_float_bits_preserve_signed_zero_infinity_and_nan_payload(self):
        nan1 = struct.unpack('!d', bytes.fromhex('7ff8000000000001'))[0]
        nan2 = struct.unpack('!d', bytes.fromhex('7ff8000000000002'))[0]
        answer = self.answer([[0.0], [-0.0], [float('inf')], [nan1], [nan2]])
        self.assertEqual(len({encoded(r) for r in answer['rows']}), 5)
        json.loads(encoded(answer))

    def test_nodes_with_identical_properties_keep_distinct_identity(self):
        a, b = self.answer([[self.a]]), self.answer([[self.b]])
        self.assertFalse(self.same(a, b))
        self.assertEqual(a['rows'][0][0]['element_id'], 'node_a')
        self.assertNotIn('eid', dict(self.a))

    def test_node_labels_and_properties_are_retained(self):
        other = Node(self.graph, 'node_a', 1, ['Officer', 'Person'], {'name': 'SameName'})
        self.assertFalse(self.same(self.answer([[self.a]]), self.answer([[other]])))
        self.assertEqual(self.answer([[other]])['rows'][0][0]['labels'], ['Officer', 'Person'])

    def test_relationship_identity_type_direction_and_properties(self):
        rel = self.relationship()
        answer = self.answer([[rel]])
        value = answer['rows'][0][0]
        self.assertEqual((value['element_id'], value['type'], value['start'], value['end']),
                         ('rel_1', 'KNOWS', 'node_a', 'node_b'))
        self.assertFalse(self.same(answer, self.answer([[self.relationship(reverse=True)]])))
        self.assertFalse(self.same(answer, self.answer([[self.relationship(name='LIKES')]])))
        self.assertFalse(self.same(answer, self.answer([[self.relationship(element_id='rel_2')]])))

    def test_path_walk_order_and_direction_do_not_collapse(self):
        rel = self.relationship()
        forward = self.answer([[Path(self.a, rel)]])
        reverse = self.answer([[Path(self.b, rel)]])
        self.assertEqual(forward['rows'][0][0]['directions'], [1])
        self.assertEqual(reverse['rows'][0][0]['directions'], [-1])
        self.assertFalse(self.same(forward, reverse))
        self.assertEqual(len(self.answer([[Path(self.a)]])['rows'][0][0]['nodes']), 1)

    def test_nested_entity_is_still_capture_scoped(self):
        value = {'items': [self.a]}
        first = self.answer([[value]])
        second = self.answer([[value]], scope=replace(self.scope, capture_id='other_capture'))
        self.assertTrue(first['contains_graph_entities'])
        with self.assertRaises(IdentityScopeConflict):
            self.same(first, second)

    def test_map_cannot_impersonate_native_node(self):
        first = self.answer([[{'t': 'node', 'element_id': 'node_a'}]])
        second = self.answer([[self.a]])
        self.assertFalse(first['contains_graph_entities'])
        self.assertFalse(self.same(first, second))

    def test_native_calendar_values_keep_nanoseconds_and_zone_offset(self):
        tz = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        dt = DateTime(2024, 2, 29, 12, 30, 15, 123456789, tzinfo=tz)
        tm = Time(12, 30, 15, 987654321, tzinfo=tz)
        answer = self.answer([[dt, tm, Date(2024, 2, 29)]], columns=['dt', 'time', 'date'])
        self.assertEqual(answer['rows'][0][0]['nanosecond'], 123456789)
        self.assertEqual(answer['rows'][0][1]['nanosecond'], 987654321)
        self.assertEqual(answer['rows'][0][0]['offset'], [0, 19800, 0])
        self.assertFalse(self.same(self.answer([[dt]]), self.answer([[DateTime(2024, 2, 29, 12, 30, 15, 123456788, tzinfo=tz)]])))

    def test_python_calendar_values_have_explicit_native_domain_projection(self):
        self.assertTrue(self.same(self.answer([[Date(2024, 1, 2)]]),
                                  self.answer([[datetime.date(2024, 1, 2)]])))
        self.assertTrue(self.same(self.answer([[DateTime(2024, 1, 2, 3, 4, 5, 123000)]]),
                                  self.answer([[datetime.datetime(2024, 1, 2, 3, 4, 5, 123)]])))

    def test_calendar_duration_is_not_flattened_to_seconds(self):
        month, days = Duration(months=1), Duration(days=30)
        self.assertFalse(self.same(self.answer([[month]]), self.answer([[days]])))
        self.assertEqual(self.answer([[month]])['rows'][0][0]['months'], 1)
        self.assertFalse(self.same(self.answer([[Duration(days=1)]]),
                                   self.answer([[datetime.timedelta(days=1)]])))

    def test_spatial_srid_and_dimensions_are_preserved(self):
        cartesian = self.answer([[CartesianPoint((1.0, 2.0))]])
        earth = self.answer([[WGS84Point((1.0, 2.0))]])
        earth3d = self.answer([[WGS84Point((1.0, 2.0, 3.0))]])
        self.assertFalse(self.same(cartesian, earth))
        self.assertFalse(self.same(earth, earth3d))
        self.assertEqual(earth3d['rows'][0][0]['srid'], 4979)
        self.assertEqual(len(earth3d['rows'][0][0]['coordinates']), 3)

    def test_record_duplicate_aliases_preserve_both_positions(self):
        record = Record([('same', self.a), ('same', self.b)])
        answer = self.answer([record], columns=['same', 'same'])
        self.assertEqual(answer['columns'], ['same', 'same'])
        self.assertEqual([v['element_id'] for v in answer['rows'][0]], ['node_a', 'node_b'])
        with self.assertRaises(ValueError):
            self.answer([record], columns=['wrong', 'same'])

    def test_record_data_projection_is_not_accepted_as_native_identity(self):
        record = Record([('node', self.a)])
        projected = record.data()
        self.assertEqual(projected['node'], {'name': 'SameName'})
        self.assertNotIn('element_id', projected['node'])
        with self.assertRaises(ValueError):
            self.answer([projected], columns=['node'])

    def test_cross_capture_entity_equality_is_not_assumed(self):
        a = self.answer([[self.a]])
        b = self.answer([[self.a]], scope=replace(self.scope, capture_id='another_capture'))
        with self.assertRaises(IdentityScopeConflict):
            self.same(a, b)

    def test_scalar_answers_across_captures_need_the_same_snapshot(self):
        a = self.answer([[1]])
        b = self.answer([[1]], scope=replace(self.scope, capture_id='another_capture'))
        self.assertTrue(self.same(a, b))
        for scope in [replace(self.scope, snapshot_sha256='b' * 64), replace(self.scope, database='other_database')]:
            with self.subTest(scope=scope), self.assertRaises(IdentityScopeConflict):
                self.same(a, self.answer([[1]], scope=scope))

    def test_order_and_row_multiplicity_remain_explicit(self):
        a, b = self.answer([[1], [2]]), self.answer([[2], [1]])
        self.assertTrue(self.same(a, b, ordered=False))
        self.assertFalse(self.same(a, b, ordered=True))
        self.assertFalse(self.same(self.answer([[1], [1]]), self.answer([[1]])))
        self.assertTrue(self.same(self.answer([[1]], columns=['a']), self.answer([[1]], columns=['b'])))

    def test_empty_complete_incomplete_unknown_and_failure_are_not_conflated(self):
        empty, incomplete = self.answer([]), self.answer([], complete=False)
        self.assertTrue(self.same(empty, empty))
        self.assertIsNone(self.same(empty, incomplete))
        self.assertIsNone(self.same(None, empty))
        self.assertIsNone(self.same(empty, {'status': 'REFUSED', 'complete': False}))

    def test_materialized_rows_required_and_header_width_checked(self):
        def live_rows():
            raise AssertionError('An unmaterialized source was consumed')
            yield [1]
        with self.assertRaises(ValueError):
            self.answer(live_rows())
        with self.assertRaises(ValueError):
            self.answer([[1, 2]])

    def test_missing_element_identity_has_no_fallback(self):
        missing = Node(self.graph, '', 999, ['Person'], {'id': 'apparently_unique'})
        with self.assertRaises(UnsupportedNativeValue):
            self.answer([[missing]])
        rel = self.graph.relationship_type('KNOWS')(self.graph, 'rel_unbound', 0, {})
        with self.assertRaises(UnsupportedNativeValue):
            self.answer([[rel]])

    def test_unknown_value_nonstring_map_and_cycles_are_not_stringified(self):
        for value in [Decimal('1.25'), {1: 'value'}, object()]:
            with self.subTest(value_type=type(value).__name__), self.assertRaises(UnsupportedNativeValue):
                self.answer([[value]])
        recursive = []
        recursive.append(recursive)
        with self.assertRaises(UnsupportedNativeValue):
            self.answer([[recursive]])

    def test_byte_limit_raises_without_partial_success(self):
        with self.assertRaises(UnsupportedNativeValue):
            self.answer([['large_value']], max_bytes=1)

    def test_native_bytes_and_map_order_do_not_lose_value(self):
        self.assertTrue(self.same(self.answer([[b'abc']]), self.answer([[bytearray(b'abc')]])))
        self.assertTrue(self.same(self.answer([[{'a': 1, 'b': None}]]),
                                  self.answer([[{'b': None, 'a': 1}]])))
        self.assertFalse(self.same(self.answer([[[1, 2]]]), self.answer([[[2, 1]]])))

    def test_freezing_copies_native_properties(self):
        node = Node(self.graph, 'with_list', 0, ['Person'], {'values': [1, 2]})
        answer = self.answer([[node]])
        before = encoded(answer)
        node['values'].append(3)
        self.assertEqual(encoded(answer), before)

    def test_scope_completeness_and_caps_cannot_be_implicit(self):
        with self.assertRaises(ValueError):
            CaptureScope('not-a-digest', 'db', 'capture')
        with self.assertRaises(ValueError):
            self.answer([[1]], complete=1)
        with self.assertRaises(ValueError):
            self.answer([[1]], max_bytes=True)
        with self.assertRaises(ValueError):
            self.same(self.answer([[1]]), self.answer([[1]]), ordered=1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', required=True, type=FilePath)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(NativeAnswerChecks)
    planned = suite.countTestCases()
    with patch.object(socket.socket, 'connect', side_effect=AssertionError('Network prohibited')):
        result = unittest.TextTestRunner(verbosity=2).run(suite)
    summary = {'kind': 'NATIVE_NEO4J_DRIVER_OBJECT_SYNTHETIC_CHECKS_NOT_EXPERIMENT',
               'planned': planned, 'completed': result.testsRun,
               'failed': len(result.failures) + len(result.errors),
               'checks_passed': result.testsRun - len(result.failures) - len(result.errors),
               'driver': '5.28.1', 'model_calls': 0, 'DB_connections': 0, 'DB_queries': 0,
               'native_entity_cross_capture_identity_certified': False,
               'author_primary_comparator': False, 'formal_admission': False}
    (args.out / 'summary.json').write_bytes(encoded(summary) + b'\n')
    if not result.wasSuccessful():
        raise SystemExit(1)


if __name__ == '__main__':
    main()
