"""File/graph binding fault injection, using tiny invented stores and no DB."""

import argparse
from copy import deepcopy
import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

from neo4j_graph_preparation_dev_20261029 import (
    GraphPreparationError,bind_public_task,prepare_graph,store_snapshot,
)
from property_graph_input_contract_dev_20261029 import digest,encoded


class GraphChecks(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve()
        (self.root/'execution_owner.json').write_bytes(encoded(dict(hostname=socket.gethostname())))
        self.source=self.root/'source'
        for name,data in (('data/databases/neo4j/neostore',b'invented-store-not-Neo4j'),
                          ('data/databases/neo4j/node-store',b'rows'),
                          ('transactions/neo4j/log.0',b'invented-log')):
            path=self.source/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
        self.schema=dict(node_props={'Person':[]},relationships=[],rel_props={})
        path=self.root/'schema.json';path.write_bytes(encoded(self.schema))
        self.spec=dict(benchmark='MindTheQuery',graph='fixture',source_store=str(self.source),
            schema_path=str(path),schema_sha256=digest(path.read_bytes()),bolt_port=22080,
            source_archive_sha256='a'*64)

    def prepare(self,**changes):
        spec=deepcopy(self.spec);spec.update(changes)
        return prepare_graph(spec,self.root/'target',owned_root=self.root)

    def test_copies_exact_bytes_and_requests_read_only_without_starting(self):
        receipt=self.prepare()
        self.assertEqual(store_snapshot(self.source),store_snapshot(self.root/'target'))
        self.assertEqual(receipt['files'],3)
        self.assertFalse(receipt['DB_server_started'])
        self.assertFalse(receipt['live_readonly_verified'])
        self.assertFalse(receipt['authentication_bootstrapped'])
        config=(self.root/'target/conf/neo4j.conf').read_text()
        self.assertIn('server.databases.default_to_read_only=true',config)
        self.assertIn('dbms.security.auth_enabled=true',config)
        self.assertIn('127.0.0.1:22080',config)

    def test_runtime_copy_does_not_modify_source_or_share_a_hardlink(self):
        before=store_snapshot(self.source);self.prepare()
        (self.root/'target/data/databases/neo4j/neostore').write_bytes(b'changed')
        self.assertEqual(store_snapshot(self.source),before)
        self.assertNotEqual(store_snapshot(self.root/'target'),before)

    def test_system_database_credentials_and_plugins_are_not_copied(self):
        for name in ('data/databases/system/secret','data/dbms/auth','plugins/plugin.jar'):
            path=self.source/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b'fixture')
        receipt=self.prepare()
        self.assertEqual(receipt['files'],3)
        self.assertFalse((self.root/'target/data/databases/system').exists())
        self.assertFalse((self.root/'target/data/dbms/auth').exists())
        self.assertEqual(list((self.root/'target/plugins').iterdir()),[])

    def test_target_must_be_fresh_and_old_bytes_survive(self):
        self.prepare();prior=(self.root/'target/preparation.json').read_bytes()
        with self.assertRaises(FileExistsError):self.prepare()
        self.assertEqual((self.root/'target/preparation.json').read_bytes(),prior)

    def test_owner_mismatch_never_starts_or_copies(self):
        (self.root/'execution_owner.json').write_bytes(encoded(dict(hostname='other-host')))
        with self.assertRaises(GraphPreparationError):self.prepare()
        self.assertFalse((self.root/'target').exists())

    def test_source_and_target_must_be_disjoint_inside_owner_root(self):
        for target in (self.source,self.source/'nested',self.root.parent/'outside'):
            with self.subTest(target=target),self.assertRaises((GraphPreparationError,ValueError)):
                prepare_graph(self.spec,target,owned_root=self.root)

    def test_no_empty_store_is_accepted(self):
        (self.source/'data/databases/neo4j/neostore').write_bytes(b'')
        with self.assertRaises(GraphPreparationError):self.prepare()

    def test_public_schema_bytes_must_match(self):
        with self.assertRaises(GraphPreparationError):self.prepare(schema_sha256='b'*64)

    def test_unknown_archive_provenance_is_not_invented(self):
        with self.assertRaises(GraphPreparationError):self.prepare(source_archive_sha256='unknown')

    def test_port_is_a_planned_integer_not_a_hardcoded_capacity(self):
        for port in (True,80,65536,'32'):
            with self.subTest(port=port),self.assertRaises(GraphPreparationError):self.prepare(bolt_port=port)
        self.assertTrue(self.prepare(bolt_port=23001)['bolt_uri'].endswith(':23001'))

    def test_file_population_changes_are_detected(self):
        import neo4j_graph_preparation_dev_20261029 as module
        original=module._files;calls=[]
        def changed(root):
            values=original(root);calls.append(True)
            return values if len(calls)==1 else values[:-1]
        with patch.object(module,'_files',side_effect=changed):
            with self.assertRaises(GraphPreparationError):store_snapshot(self.source)

    def test_copy_error_is_not_reported_as_a_prepared_graph(self):
        with patch('neo4j_graph_preparation_dev_20261029.os.fsync',side_effect=OSError('invented disk failure')):
            with self.assertRaises(OSError):self.prepare()
        self.assertFalse((self.root/'target/preparation.json').exists())

    def test_partial_or_implementation_failure_propagates(self):
        import neo4j_graph_preparation_dev_20261029 as module
        original=module.store_snapshot;calls=[]
        def changed(root):
            snapshot=original(root);calls.append(True)
            if len(calls)>1:snapshot['snapshot_sha256']='c'*64
            return snapshot
        with patch.object(module,'store_snapshot',side_effect=changed):
            with self.assertRaises(GraphPreparationError):self.prepare()
        self.assertTrue((self.root/'target/source_snapshot.json').exists())
        self.assertFalse((self.root/'target/preparation.json').exists())

    def test_public_task_binds_graph_schema_and_snapshot_without_gold(self):
        receipt=self.prepare();common=dict(graph='fixture',schema=self.schema,question='Original public question')
        bound=bind_public_task('task-1',common,receipt)
        self.assertEqual(bound['common_input_sha256'],digest(encoded(common)))
        self.assertEqual(bound['snapshot_sha256'],receipt['snapshot_sha256'])
        self.assertFalse(bound['live_graph_verified'])
        self.assertFalse(bound['complete_prompt_binding_verified'])

    def test_public_task_cannot_change_graph_schema_or_include_reference(self):
        receipt=self.prepare();common=dict(graph='fixture',schema=self.schema,question='Original')
        for change in (dict(graph='other'),dict(schema={}),dict(reference='RETURN gold'),dict(question='')):
            value=deepcopy(common);value.update(change)
            with self.subTest(change=change),self.assertRaises(GraphPreparationError):
                bind_public_task('task-1',value,receipt)


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',required=True);args=parser.parse_args()
    suite=unittest.defaultTestLoader.loadTestsFromTestCase(GraphChecks);planned=suite.countTestCases()
    with patch.object(socket,'create_connection',side_effect=AssertionError('No network')), \
         patch.object(socket.socket,'connect',side_effect=AssertionError('No network')):
        result=unittest.TextTestRunner(verbosity=2).run(suite)
    output=Path(args.output);output.mkdir(parents=True,exist_ok=False)
    summary=dict(kind='INVENTED_STORE_FILE_PREPARATION_NOT_DB_ACCEPTANCE',planned=planned,
        completed=result.testsRun,checks_passed=result.testsRun-len(result.errors)-len(result.failures),
        failed=len(result.errors)+len(result.failures),model_calls=0,DB_connections=0,DB_queries=0,
        DB_server_started=False,formal_admission=False)
    (output/'summary.json').write_bytes(encoded(summary)+b'\n')
    if not result.wasSuccessful():raise SystemExit(1)


if __name__=='__main__':main()
