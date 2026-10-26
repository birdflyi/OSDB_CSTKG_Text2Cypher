"""Fingerprint guards against lossy graph normalization before real copies."""
import csv,json,tempfile,unittest
from pathlib import Path
from materialize_osdb_semantic_snapshots_20261027 import csv_fingerprint


class FingerprintTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
  self.nodes=[{'node_uid':'u1','label':'Actor','entity_id':'same','properties_json':json.dumps({'entity_id':'same','flag':False,'time':'2023-07-01T01:00:00+01:00'})},
   {'node_uid':'u2','label':'Issue','entity_id':'same','properties_json':json.dumps({'entity_id':'same'})}]
  self.edges=[self.edge('e1','u2','u1'),self.edge('e2','u2','u1')]
 def edge(self,key,a,b):return {'native_rel_type':'EVENT_ACTION','service_rel_type':'OPENED_BY','source_node_uid':a,'target_node_uid':b,
  'properties_json':json.dumps({'edge_uid':key,'native_rel_type':'EVENT_ACTION','service_rel_type':'OPENED_BY'})}
 def run_graph(self,nodes=None,edges=None):
  for name,rows in [('nodes',self.nodes if nodes is None else nodes),('edges',self.edges if edges is None else edges)]:
   with (self.root/(name+'.csv')).open('w',encoding='utf-8',newline='') as f:
    w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
  return csv_fingerprint(self.root)
 def test_row_order_irrelevant_but_direction_and_physical_edges_preserved(self):
  a=self.run_graph();b=self.run_graph(list(reversed(self.nodes)),list(reversed(self.edges)))
  self.assertEqual(a['semantic_snapshot_sha256'],b['semantic_snapshot_sha256']);self.assertEqual(a['edges'],2)
  self.assertNotEqual(a['semantic_snapshot_sha256'],self.run_graph(edges=[self.edges[0]])['semantic_snapshot_sha256'])
  self.assertNotEqual(a['semantic_snapshot_sha256'],self.run_graph(edges=[self.edge('e1','u1','u2'),self.edges[1]])['semantic_snapshot_sha256'])
 def test_type_and_original_offset_string_not_coerced(self):
  a=self.run_graph()['semantic_snapshot_sha256'];props=json.loads(self.nodes[0]['properties_json']);props['flag']=0
  self.nodes[0]['properties_json']=json.dumps(props);self.assertNotEqual(a,self.run_graph()['semantic_snapshot_sha256'])
  props['flag']=False;props['time']='2023-07-01T00:00:00Z';self.nodes[0]['properties_json']=json.dumps(props)
  self.assertNotEqual(a,self.run_graph()['semantic_snapshot_sha256'])
 def test_typed_identity_and_edge_uniqueness(self):
  self.assertEqual(self.run_graph()['nodes'],2)
  with self.assertRaises(AssertionError):self.run_graph(edges=[self.edges[0],self.edges[0]])
  self.nodes[1]['label']='Actor'
  with self.assertRaises(AssertionError):self.run_graph()
 def test_only_null_absence_normalization_metadata_rejected(self):
  a=self.run_graph()['semantic_snapshot_sha256'];props=json.loads(self.nodes[0]['properties_json']);props['absent']=None
  self.nodes[0]['properties_json']=json.dumps(props);b=self.run_graph()
  self.assertEqual(a,b['semantic_snapshot_sha256']);self.assertEqual(b['null_property_entries_omitted_by_key'],{'absent':1})
  props['__pilot_uid']='u1';self.nodes[0]['properties_json']=json.dumps(props)
  with self.assertRaises(AssertionError):self.run_graph()


if __name__=='__main__':unittest.main()
