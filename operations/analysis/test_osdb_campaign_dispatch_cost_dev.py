"""New cost-dispatch wiring only; old test methods are not rerun."""
from contextlib import contextmanager
from unittest.mock import patch
import unittest
import test_osdb_campaign_dispatch_dev as fixtures
from osdb_campaign_dispatch_cost_dev import run_campaign
from osdb_scoped_attempts_dev import read


class CostDispatchTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CampaignTests()
        self.fixture.setUp()
        self.guard = patch('socket.socket', side_effect=AssertionError('Synthetic integration only'))
        self.socket = self.guard.start()
    def tearDown(self):
        self.assertEqual(self.socket.call_count, 0)
        self.guard.stop()
        self.fixture.tearDown()

    def test_two_method_paths_keep_separate_costs_and_drain_after_control(self):
        f = self.fixture
        with f.lease() as lease:
            @contextmanager
            def graph(name):
                with f.graph(name) as handle:
                    original = handle['execute']
                    def execute(q, p):
                        result = original(q, p)
                        result['result_bytes'] = 5
                        # Deliberately end after Direct, Ours, shared detail.
                        if f.db_calls == 3:
                            lease.stopped = True
                        return result
                    handle['execute'] = execute
                    handle['physical_statement_count'] = lambda: 2 * f.db_calls
                    yield handle
            result = run_campaign(f.client(lease), f.manifest, f.root/'cost_scheduler', graph,
                                  max_inflight=1, graph_snapshot_hashes=f.hashes, evidence_kind='synthetic')
            self.assertEqual(result['paths_completed'], 2)
            self.assertEqual(result['undispatched_paths'], 46)
            self.assertEqual(result['unresolved_dispatched_paths'], 0)
            self.assertEqual(f.model_calls, 2)
            self.assertEqual(f.db_calls, 3)
            ours = read(lease.directory/'paths/redis_fixture0__ours.json')
            direct = read(lease.directory/'paths/redis_fixture0__direct.json')
            self.assertEqual(ours['costs']['groups']['method']['physical_statements'], 2)
            self.assertEqual(ours['costs']['groups']['full_detail_control']['physical_statements'], 2)
            self.assertNotIn('full_detail_control', direct['costs']['groups'])
            for key in ('redis_fixture0__direct', 'redis_fixture0__ours'):
                self.assertTrue((lease.directory/'method_finals'/(key+'.json')).is_file())
                self.assertTrue((lease.directory/'costs'/(key+'.json')).is_file())
            self.assertIn('excluding_control_replay', result['path_latency_scope'])
            self.assertEqual(f.legacy.inspect()['pending'], [])
        self.assertEqual(f.graph_events, [('open', 'redis'), ('close', 'redis')])

    def test_new_dispatcher_rejects_above64_before_model_or_graph(self):
        f = self.fixture
        with f.lease() as lease:
            with self.assertRaises(ValueError):
                run_campaign(f.client(lease), f.manifest, f.root/'cost_scheduler', f.graph,
                             max_inflight=65, graph_snapshot_hashes=f.hashes, evidence_kind='synthetic')
        self.assertEqual(f.model_calls, 0)
        self.assertEqual(f.graph_events, [])

    def test_live_mode_rejects_client_without_central_permit_before_graph(self):
        f = self.fixture
        with f.lease() as lease:
            with self.assertRaisesRegex(RuntimeError, 'central permitted'):
                run_campaign(f.client(lease), f.manifest, f.root/'cost_scheduler', f.graph,
                             max_inflight=1, graph_snapshot_hashes=f.hashes, evidence_kind='live_development')
        self.assertEqual(f.model_calls, 0)
        self.assertEqual(f.graph_events, [])


if __name__ == '__main__':
    unittest.main()
