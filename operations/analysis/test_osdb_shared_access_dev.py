"""Behavioral gate checks; fakes do not count as real database acceptance."""
from copy import deepcopy
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0,r'D:\ch7\tools\python_deps\neo4j_5_15')
from osdb_common_input_v2 import messages,public_profile
from osdb_shared_read_executor_dev import SharedReadExecutor,AdmissionError,admitted_kind
from observed_profile_semantic_cases import SCHEMA

SCHEMA=deepcopy(SCHEMA);SCHEMA['entities']=SCHEMA['entities'][:1]


class Cursor:
 def __init__(self,rows,columns,kind='r'):self.rows=rows;self.columns=columns;self.kind=kind
 def __iter__(self):return iter(self.rows)
 def keys(self):return self.columns
 def consume(self):return type('Summary',(),{'query_type':self.kind})()


class Driver:
 def __init__(self,results):self.results=list(results);self.queries=[]
 def session(self,**kwargs):return self
 def __enter__(self):return self
 def __exit__(self,*args):pass
 def run(self,query,params):self.queries.append(str(query));return self.results.pop(0)


class AccessTests(unittest.TestCase):
 def setUp(self):self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
 def executor(self,results,**caps):
  driver=Driver(results);x=SharedReadExecutor(driver,SCHEMA,Path(self.temp.name)/str(len(list(Path(self.temp.name).iterdir()))),**caps)
  return x,driver
 def nodes(self,**changes):
  row={'storage_id':'1','labels':['Person'],'props':{'key':'a','eligible':True}};row.update(changes)
  return Cursor([row],list(row))
 def test_common_bytes_preserve_annotations_but_strip_gold_counts(self):
  schema=deepcopy(SCHEMA);schema['answer']='SECRET';schema['entities'][0]['population']=777
  kwargs=dict(task_id='dev1',graph='synthetic',question='Count eligible accounts',schema=schema)
  a=messages('direct',**kwargs);b=messages('ours',**kwargs)
  self.assertEqual(a['messages'][1],b['messages'][1]);self.assertNotEqual(a['messages'][0],b['messages'][0])
  self.assertEqual(a['common_input_sha256'],b['common_input_sha256'])
  self.assertNotIn('SECRET',a['messages'][1]['content']);self.assertNotIn('population',a['messages'][1]['content'])
  p=public_profile(schema);self.assertEqual(p['relations'][0]['identity_property'],'physical_key')
  self.assertEqual(p['relations'][0]['temporal_properties'],['clock'])
 def test_annotated_wrong_type_and_duplicate_schema_rejected(self):
  x=deepcopy(SCHEMA);x['entities'][0]['temporal_properties']=['eligible']
  with self.assertRaises(ValueError):public_profile(x)
  x=deepcopy(SCHEMA);x['entities']*=2
  with self.assertRaises(ValueError):public_profile(x)
 def test_no_execution_before_admission(self):
  x,d=self.executor([])
  with self.assertRaises(AdmissionError):x.execute('RETURN 1')
  self.assertEqual(d.queries,[])
 def test_overlapping_storage_label_blocks_both_arms_and_latches(self):
  x,d=self.executor([self.nodes(labels=['Person','PilotNode'])])
  with self.assertRaises(AdmissionError):x.admit()
  for q in ('MATCH(n) RETURN n','MATCH(n:Person) RETURN n.key'):
   with self.assertRaises(AdmissionError):x.execute(q)
  with self.assertRaises(AdmissionError):x.admit()
  self.assertEqual(len(d.queries),1)
 def test_hidden_property_also_rejected(self):
  x,d=self.executor([self.nodes(props={'key':'a','__pilot_uid':'x'})])
  with self.assertRaises(AdmissionError):x.admit()
 def test_boolean_is_not_integer_or_string(self):
  x,d=self.executor([self.nodes(props={'key':'a','eligible':1})])
  with self.assertRaises(AdmissionError):x.admit()
 def test_nested_mixed_type_parser(self):
  self.assertTrue(admitted_kind('list[int|str]','mixed[float|list[int|str]]'))
  self.assertFalse(admitted_kind('str','mixed[float|list[int|str]]'))
 def test_write_procedure_multistatement_denied_before_dispatch(self):
  x,d=self.executor([self.nodes(),Cursor([],[])])
  x.admit();n=len(d.queries)
  for q in ('MATCH(n) SET n.key="x" RETURN n','CALL db.labels() YIELD label RETURN label','RETURN 1; RETURN 2'):
   with self.assertRaises(ValueError):x.execute(q)
  self.assertEqual(len(d.queries),n)
 def test_non_read_explain_prevents_execution(self):
  x,d=self.executor([self.nodes(),Cursor([],[]),Cursor([],[],'rw')]);x.admit()
  with self.assertRaises(AdmissionError):x.execute('RETURN 1')
  self.assertEqual(len(d.queries),3);self.assertTrue(d.queries[-1].startswith('EXPLAIN'))
 def test_row_cap_is_incomplete_without_limit_rewrite(self):
  x,d=self.executor([self.nodes(),Cursor([],[]),Cursor([],[]),Cursor([{'x':i} for i in range(3)],['x'])],max_rows=2)
  x.admit();r=x.execute('UNWIND [0,1,2] AS x RETURN x')
  self.assertFalse(r['complete']);self.assertEqual(r['status'],'TRUNCATED');self.assertEqual(len(r['rows']),2)
  self.assertNotIn('LIMIT',d.queries[-1])
 def test_byte_cap_is_incomplete(self):
  x,d=self.executor([self.nodes(),Cursor([],[]),Cursor([],[]),Cursor([{'x':'long'}],['x'])],max_bytes=3)
  x.admit();r=x.execute('RETURN "long" AS x');self.assertFalse(r['complete']);self.assertEqual(r['rows'],[])
 def test_driver_error_latches_no_retry(self):
  x,d=self.executor([self.nodes(),Cursor([],[])]);x.admit()
  with self.assertRaises(IndexError):x.execute('RETURN 1')
  with self.assertRaises(AdmissionError):x.execute('RETURN 1')
  self.assertEqual(len(d.queries),3)
 def test_compiler_cannot_consume_truncated_shared_result(self):
  import observed_neo4j_services_dev_v2 as compiler
  x,d=self.executor([self.nodes(),Cursor([],[]),Cursor([],[]),Cursor([{'id':str(i)} for i in range(3)],['id'])],max_rows=2)
  x.admit();plan={'status':'ANSWER','calls':[{'nodes':[{'id':'n','label':'Person'}],'edges':[],
   'where':[],'returns':[{'field':'n.key','alias':'id'}]}],'combine':{'op':'single'}}
  with self.assertRaises(compiler.IncompleteEvidence):compiler.execute_plan(plan,SCHEMA,x.execute)


if __name__=='__main__':unittest.main()
