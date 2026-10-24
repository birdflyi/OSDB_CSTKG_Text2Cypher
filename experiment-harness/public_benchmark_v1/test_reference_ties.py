import unittest
from reference_ties import ranking_tie_witness


class RankingWitnessChecks(unittest.TestCase):
    def test_reordering_inside_tie_is_certified_but_wrong_rank_is_not(self):
        rows = [{'name': 'A', 'key': 3}, {'name': 'B', 'key': 1}, {'name': 'C', 'key': 1}]
        self.assertTrue(ranking_tie_witness(rows, [['A'], ['B'], ['C']],
                        [['A'], ['C'], ['B']], descending=True)['certified'])
        self.assertFalse(ranking_tie_witness(rows, [['A'], ['B'], ['C']],
                         [['B'], ['A'], ['C']], descending=True)['certified'])

    def test_tied_limit_and_null_placement(self):
        rows = [{'name': 'A', 'key': 0}, {'name': 'B', 'key': 0}, {'name': 'C', 'key': None}]
        self.assertTrue(ranking_tie_witness(rows, [['A']], [['B']], limit=1)['certified'])
        self.assertFalse(ranking_tie_witness(rows, [['A']], [['C']], limit=1)['certified'])
        self.assertTrue(ranking_tie_witness(rows, [['C'], ['A'], ['B']],
                        [['C'], ['B'], ['A']], descending=True)['certified'])

    def test_missing_duplicate_or_ambiguous_outputs_fail_closed(self):
        rows = [{'name': 'A', 'key': 0}, {'name': 'B', 'key': 0}]
        for actual in [[['A']], [['A'], ['A']], [['unseen'], ['A']]]:
            self.assertFalse(ranking_tie_witness(rows, [['A'], ['B']], actual)['certified'])
        self.assertFalse(ranking_tie_witness([{'name': 'A', 'key': 0}, {'name': 'A', 'key': 0}],
                         [['A']], [['A']], limit=1)['certified'])

    def test_projection_duplicates_keep_their_per_rank_multiplicity(self):
        rows = [{'name': 'A', 'key': 2}, {'name': 'A', 'key': 1}, {'name': 'B', 'key': 1}]
        self.assertTrue(ranking_tie_witness(rows, [['A'], ['A'], ['B']],
                        [['A'], ['B'], ['A']], descending=True)['certified'])
        self.assertFalse(ranking_tie_witness(rows, [['A'], ['A'], ['B']],
                         [['B'], ['A'], ['A']], descending=True)['certified'])


if __name__ == '__main__':
    unittest.main()
