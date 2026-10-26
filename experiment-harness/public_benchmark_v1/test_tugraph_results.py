import json
import unittest

from tugraph_results import parse_rest_result, strict_auxiliary_equal, MalformedNativeResult
from schema_services import IncompleteEvidence


def result(rows, columns=None):
    if columns is None:columns=['value']
    raw=json.dumps({'header':[{'name':c,'type':0} for c in columns],
                    'result':rows,'size':len(rows)}).encode()
    return parse_rest_result(raw,transport_complete=True,execution_complete=True)


class NativeResultContracts(unittest.TestCase):
    def test_bool_integer_string_and_float_are_distinct_diagnostics(self):
        for other in (1,'true',1.0):
            self.assertFalse(strict_auxiliary_equal(result([[True]]),result([[other]]),ordered=False))

    def test_dates_remain_strings_despite_header_type_zero(self):
        r=result([['2020-01-02 00:00:00.123456']])
        self.assertEqual(r['rows'][0][0],'2020-01-02 00:00:00.123456')
        self.assertFalse(r['author_bolt_equivalence_established'])
        self.assertFalse(r['formal_metric_frozen'])

    def test_bag_multiplicity_and_explicit_order(self):
        a=result([[1],[1],[2]])
        self.assertFalse(strict_auxiliary_equal(a,result([[1],[2]]),ordered=False))
        b=result([[2],[1],[1]])
        self.assertTrue(strict_auxiliary_equal(a,b,ordered=False))
        self.assertFalse(strict_auxiliary_equal(a,b,ordered=True))

    def test_empty_results_keep_shape_and_failure_is_not_empty_success(self):
        self.assertFalse(strict_auxiliary_equal(result([]),result([],['a','b']),ordered=False))
        self.assertFalse(strict_auxiliary_equal(result([]),{'status':'UNSUPPORTED','rows':[]},ordered=False))

    def test_incomplete_and_malformed_responses_are_rejected(self):
        with self.assertRaises(IncompleteEvidence):parse_rest_result(b'{}',transport_complete=False)
        with self.assertRaises(IncompleteEvidence):parse_rest_result(b'{}',transport_complete=True)
        for body in ({'header':[{'name':'x'}],'result':[[1,2]],'size':1},
                     {'header':[{'name':'x'}],'result':[[1]],'size':2},
                     {'header':[{'name':'x'}],'result':[[1]],'size':True},
                     {'error_message':'query not supported'}):
            with self.subTest(body=body),self.assertRaises(MalformedNativeResult):
                parse_rest_result(json.dumps(body).encode(),transport_complete=True,execution_complete=True)

    def test_map_key_order_is_irrelevant_without_collapsing_nulls_or_lists(self):
        self.assertTrue(strict_auxiliary_equal(result([[{'a':1,'b':None}]]),
            result([[{'b':None,'a':1}]]),ordered=False))
        self.assertFalse(strict_auxiliary_equal(result([[[1,2]]]),result([[[2,1]]]),ordered=False))

    def test_nonfinite_numbers_are_not_silently_rounded_or_relabelled(self):
        with self.assertRaises(MalformedNativeResult):
            parse_rest_result(b'{"header":[{"name":"x"}],"result":[[NaN]],"size":1}',transport_complete=True,execution_complete=True)

    def test_invalid_encoding_or_json_is_an_explicit_protocol_error(self):
        for raw in (b'\xff',b'{"header":'):
            with self.assertRaises(MalformedNativeResult):
                parse_rest_result(raw,transport_complete=True,execution_complete=True)


if __name__=='__main__':unittest.main()
