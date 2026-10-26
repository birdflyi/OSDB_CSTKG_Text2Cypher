"""Boundary checks for partial usage and nonadditive accounting, no requests."""
import unittest
from reconcile_t1_cost_evidence_20261026 import usage_object, evidence, new_bucket, add


class CostEvidence(unittest.TestCase):
    def test_authentication_failure_unknown_usage_is_not_zero(self):
        b=new_bucket();add(b,{'http_status':401,'usage':None,'elapsed_seconds':0.02})
        self.assertEqual(b['usage_object_unknown'],1)
        self.assertEqual(b['tokens']['total_tokens']['known_attempts'],0)
        self.assertEqual(b['tokens']['total_tokens']['unknown_attempts'],1)

    def test_partial_usage_fields_stay_partial(self):
        b=new_bucket();add(b,{'http_status':200,'body':{'usage':{'prompt_tokens':3}}})
        self.assertEqual(b['usage_object_present'],1)
        self.assertEqual(b['all_three_standard_fields_known'],0)
        self.assertEqual(b['tokens']['completion_tokens']['unknown_attempts'],1)

    def test_boolean_negative_and_fractional_tokens_are_invalid(self):
        for value in (True,-1,1.5,'10'):
            with self.subTest(value=value):self.assertEqual(evidence({'total_tokens':value},'total_tokens'),('invalid',None))

    def test_conflicting_usage_schemas_stop_instead_of_picking_larger(self):
        with self.assertRaises(ValueError):usage_object({'usage':{'total_tokens':20},'body':{'usage':{'total_tokens':10}}})

    def test_reasoning_and_cache_subcounts_not_added_to_total(self):
        b=new_bucket();add(b,{'http_status':200,'body':{'usage':{'prompt_tokens':10,'completion_tokens':6,
            'total_tokens':16,'completion_tokens_details':{'reasoning_tokens':4},'prompt_tokens_details':{'cached_tokens':7}}}})
        self.assertEqual(b['tokens']['total_tokens']['known_sum'],16)
        self.assertEqual(b['tokens']['completion_tokens_details.reasoning_tokens']['known_sum'],4)
        self.assertEqual(b['tokens']['prompt_tokens_details.cached_tokens']['known_sum'],7)

    def test_remote_HTTP_and_client_or_retry_clocks_keep_separate_boundaries(self):
        b=new_bucket();add(b,{'http_status':401,'elapsed_seconds':0.02})
        self.assertEqual(b['clock_fields']['seconds']['unknown_attempts'],1)
        self.assertEqual(b['clock_fields']['elapsed_seconds']['known_attempts'],1)
        self.assertEqual(b['clock_fields']['retry_wait_seconds']['unknown_attempts'],1)


if __name__=='__main__':unittest.main()
