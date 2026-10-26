"""Offline resource change must leave all method read limits untouched."""
import unittest
from recover_osdb_elasticsearch_offline_scan_20261027 import offline_timeout


class OfflineBudgetTests(unittest.TestCase):
 def test_only_fixed_admission_scan_extended(self):
  for kind in ('admit_nodes','admit_edges'):self.assertEqual(offline_timeout(kind,240),600)
 def test_explain_and_method_reads_unchanged(self):
  self.assertEqual(offline_timeout('explain',30),30);self.assertEqual(offline_timeout('read',60),60)
 def test_unexpected_old_profile_is_rejected(self):
  with self.assertRaises(AssertionError):offline_timeout('admit_edges',60)


if __name__=='__main__':unittest.main()
