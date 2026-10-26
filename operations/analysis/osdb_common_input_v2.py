"""Pure shared observed-schema input and method prompts; not formal admission."""
from copy import deepcopy
from osdb_common_input import canonical,_public_declaration

VERSION='osdb-semantic-only-common-input-dev-2'
ACCESS={'physical_view':'semantic_only_single_label_snapshot',
 'read_policy':'One read-only Cypher; no write/admin/procedure/database-switch clauses; EXPLAIN query_type must be r.',
 'max_rows':1000000,'max_result_bytes':64*1024*1024,'max_query_characters':50000,
 'explain_timeout_seconds':30,'execution_timeout_seconds':60,
 'storage_metadata':'PilotNode and __pilot_uid must be physically absent, not just hidden in prompts.',
 'graph_values':'Output normalization and final task/scoring admission remain separate gates.'}
NOTICE=('Neo4j 5.15.0. Only declared semantic labels and properties exist in the admitted physical snapshot. '
 'No metadata labels/properties may remain. Entity identity is (label,declared identity_property); '
 'these are account/artifact keys, not inferred people. Directed physical relationships have their '
 'declared identity_property. Preserve NULL, bag multiplicity and requested half-open bounds. '
 'Temporal annotations leave storage as STRING. For absolute-instant predicates compare '
 '[datetime(value).epochSeconds,datetime(value).nanosecond] on both operands, with explicit non-null '
 'guards, so equivalent offsets and nanoseconds are preserved. Plain datetime equality/order may '
 'retain timezone information at the same instant. Raw temporal property projections stay STRING. '
 'Do not infer whole histories, people resolution, employment or bot exclusion. No implicit LIMIT. '
 'Both methods receive the same empty entity suggestions; retrieval is not supplied yet.')
DIRECT_SYSTEM=('Return one read-only Neo4j 5.15 Cypher query only. Use the common input schema, '
 'database notes and access limits. Do not invent properties or implicit result limits. '
 'Native read patterns, aggregation, UNION and read subqueries remain available. No procedures, '
 'write/admin clauses or database switching. Return the actual requested answer.')
OURS_SYSTEM='''Return one JSON object only. Use the common input schema and database notes.
For unsupported requests return {"status":"UNSUPPORTED","reason":"..."}; never fall back to Direct.
Otherwise {"status":"ANSWER","calls":[selection],"combine":{"op":"single"}}.
A selection has nodes:[{id,label}], edges:[{id,source,target,label,optional?}], where:[{field,op,value?|other_field?,negate?}], returns:[{field,alias,agg?,distinct?}], and optional distinct,order_by,limit,group_by.
Use one to four nodes, at most three connected directed edges and one OPTIONAL edge. Conditions are AND, at most16. Ops:eq,ne,lt,le,gt,ge,in,contains,starts_with,ends_with,is_null,not_null; bool only equality/membership/null. Literals must match declared types. Temporal predicates use schema annotations automatically, retain timezone/fraction literals. Returns1..8; aggregates:count,sum,avg,min,max,collect. Explicit group_by preserves declared identity grain. Count an optional neighbor or relationship identity to obtain zero, not count(*).
For two calls combine op:union|intersection|difference,key:<shared projected key alias>,output:rows|count,columns:<aliases for rows>|alias:<count alias>. Same entity type, required non-null declared identity as key, payload tied to that entity, matching columns, unaggregated/unordered/untruncated inputs. At most two calls. Full-detail and contract are controls sharing this plan, not independent model methods. No query text, new tools or metadata attributes.'''


def public_profile(schema):
 entities=[];relations=[]
 for row in schema['entities']:
  value=_public_declaration(row)
  value['temporal_properties']=deepcopy(row.get('temporal_properties',[]));entities.append(value)
 for row in schema['relations']:
  value=_public_declaration(row,True)
  key=row.get('identity_property')
  if value['properties'].get(key)!='str':raise ValueError('Declared string physical relationship identity required')
  value['identity_property']=key;value['temporal_properties']=deepcopy(row.get('temporal_properties',[]));relations.append(value)
 for row in entities+relations:
  if type(row['temporal_properties']) is not list or any(row['properties'].get(k)!='str' for k in row['temporal_properties']):
   raise ValueError('Temporal annotation requires observed STRING properties')
 labels=[e['label'] for e in entities];signatures=[(r['label'],r['subj_label'],r['obj_label']) for r in relations]
 if len(labels)!=len(set(labels)) or len(signatures)!=len(set(signatures)):raise ValueError('Duplicate schema declaration')
 if any(a not in labels or b not in labels for _,a,b in signatures):raise ValueError('Unknown endpoint')
 return {'entities':entities,'relations':relations}


def common_input(*,task_id,graph,question,schema):
 import hashlib
 if any(type(v) is not str or not v.strip() for v in (task_id,graph,question)):raise ValueError('Explicit task fields required')
 payload={'task_id':task_id,'graph':graph,'question':question,'schema':public_profile(schema),
  'entity_candidates':[],'database_notes':NOTICE,'access_contract':deepcopy(ACCESS)}
 wire=canonical(payload)
 return {'payload':payload,'serialized':wire,'sha256':hashlib.sha256(wire.encode('utf-8')).hexdigest(),
  'version':VERSION,'formal_admission':False}


def messages(arm,*,task_id,graph,question,schema):
 if arm not in ('direct','ours'):raise ValueError('Unknown arm')
 common=common_input(task_id=task_id,graph=graph,question=question,schema=schema)
 return {'messages':[{'role':'system','content':DIRECT_SYSTEM if arm=='direct' else OURS_SYSTEM},
  {'role':'user','content':common['serialized']}], 'common_input_sha256':common['sha256'],'formal_admission':False}
