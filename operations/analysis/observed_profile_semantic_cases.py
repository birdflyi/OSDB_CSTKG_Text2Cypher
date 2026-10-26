"""Fixed synthetic graph and independent expected rows; no task/gold inputs."""
from copy import deepcopy
from itertools import permutations

PEOPLE=[
 {'key':'a','display':'same','eligible':True,'clock':'2023-07-01T00:00:00Z'},
 {'key':'b','display':'same','eligible':False,'clock':'2023-07-01T01:00:00+01:00'},
 {'key':'c','display':'other','clock':'2023-07-01T00:00:00.1Z'},
 {'key':'d','display':'zero','eligible':False},
 {'key':'e','display':'nano','eligible':True,'clock':'2023-07-01T00:00:00.000000001Z'},
 {'key':'f','display':'earlier','eligible':True,'clock':'2023-06-30T23:59:59Z'}]
EDGES=[{'key':'r1','a':'a','b':'b','clock':'2023-07-01T00:00:00Z'},
 {'key':'r2','a':'a','b':'b','clock':'2023-07-01T00:00:00.1Z'},
 {'key':'r3','a':'b','b':'c','clock':'2023-07-01T00:00:00.000000001Z'},
 {'key':'r4','a':'b','b':'a','clock':'2023-07-01T01:00:00+01:00'}]
SCHEMA={'entities':[{'label':'Person','identity_property':'key',
 'properties':{'key':'str','display':'str','eligible':'bool','clock':'str'},'temporal_properties':['clock']},
 {'label':'NumberPerson','identity_property':'number','properties':{'number':'int'}}],
 'relations':[{'label':'LINK','subj_label':'Person','obj_label':'Person','identity_property':'physical_key',
 'properties':{'physical_key':'str','clock':'str'},'temporal_properties':['clock']}]}


def call():
 return {'nodes':[{'id':'n','label':'Person'}],'edges':[],'where':[],
  'returns':[{'field':'n.key','alias':'id'}]}


def single(c):return {'status':'ANSWER','calls':[c],'combine':{'op':'single'}}


def cases():
 rows=[]
 c=call();c['where']=[{'field':'n.eligible','op':'eq','value':False,'negate':True}]
 rows.append(('null_negation',single(c),['id'],[[p['key']] for p in PEOPLE if p.get('eligible') is True]))
 c=call();c['where']=[{'field':'n.eligible','op':'eq','value':False}]
 rows.append(('strict_boolean',single(c),['id'],[[p['key']] for p in PEOPLE if p.get('eligible') is False]))
 c=call();c['nodes'].append({'id':'m','label':'Person'})
 c['edges']=[{'id':v,'source':'n','target':'m','label':'LINK'} for v in ('r','s')]
 c['returns']=[{'field':'n.key','alias':'a'},{'field':'m.key','alias':'b'}]
 expected=[[x['a'],x['b']] for x,y in permutations(EDGES,2) if (x['a'],x['b'])==(y['a'],y['b'])]
 rows.append(('same_match_physical_uniqueness',single(c),['a','b'],expected))
 c=call();c['nodes'].append({'id':'m','label':'Person'})
 c['edges']=[{'id':'r','source':'n','target':'m','label':'LINK','optional':True}]
 c['group_by']=['n.key'];c['returns']=[{'field':'n.key','alias':'id'},
  {'field':'n.display','alias':'display'},{'field':'m.key','agg':'count','alias':'n'}]
 expected=[[p['key'],p['display'],sum(e['a']==p['key'] for e in EDGES)] for p in PEOPLE]
 rows.append(('optional_zero_and_identity_group',single(c),['id','display','n'],expected))
 c=deepcopy(c);c['where']=[{'field':'m.key','op':'is_null'}]
 rows.append(('optional_post_join_filter',single(c),['id','display','n'],[row for row in expected if row[2]==0]))
 # Independent absolute-time offsets, in nanoseconds relative to the boundary.
 offsets={'a':0,'b':0,'c':100000000,'d':None,'e':1,'f':-1000000000}
 for name,low,high,lo,hi in (
  ('instant_half_open','2023-07-01T00:00:00Z','2023-07-01T00:00:00.1Z',0,100000000),
  ('nanosecond_boundary','2023-07-01T00:00:00Z','2023-07-01T00:00:00.000000001Z',0,1),
  ('offset_equivalent_window','2023-07-01T01:00:00+01:00','2023-07-01T00:00:00.1Z',0,100000000)):
  c=call();c['where']=[{'field':'n.clock','op':'ge','value':low},{'field':'n.clock','op':'lt','value':high}]
  rows.append((name,single(c),['id'],[[k] for k,v in offsets.items() if v is not None and lo<=v<hi]))
 a=call();b=call();a['where']=[{'field':'n.clock','op':'eq','value':'2023-07-01T00:00:00Z'}]
 b['where']=[{'field':'n.clock','op':'eq','value':'2023-07-01T01:00:00+01:00'}]
 rows.append(('instant_equality',single(deepcopy(a)),['id'],[['a'],['b']]))
 p={'status':'ANSWER','calls':[a,b],'combine':{'op':'union','key':'id','output':'count','alias':'n'}}
 rows.append(('instant_equivalent_union',p,['n'],[[2]]))
 a=call();b=call();a['where']=[{'field':'n.eligible','op':'eq','value':True}]
 b['where']=[{'field':'n.key','op':'in','value':['a','b']}]
 p={'status':'ANSWER','calls':[a,b],'combine':{'op':'difference','key':'id','output':'count','alias':'n'}}
 rows.append(('real_identity_difference',p,['n'],[[2]]))
 c={'nodes':[{'id':'n','label':'NumberPerson'}],'edges':[],'where':[],
  'returns':[{'field':'n.number','alias':'id'}]}
 p={'status':'ANSWER','calls':[c,deepcopy(c)],'combine':{'op':'intersection','key':'id','output':'count','alias':'n'}}
 rows.append(('integer_key_intersection',p,['n'],[[2]]))
 c=call();c['where']=[{'field':'n.key','op':'eq','value':'e'}]
 c['returns']=[{'field':'n.clock','alias':'raw'}]
 rows.append(('raw_nanosecond_projection',single(c),['raw'],[['2023-07-01T00:00:00.000000001Z']]))
 c={'nodes':[{'id':'a','label':'Person'},{'id':'b','label':'Person'}],
  'edges':[{'id':'r','source':'a','target':'b','label':'LINK'}],
  'where':[{'field':'r.clock','op':'ge','value':'2023-07-01T00:00:00Z'},
   {'field':'r.clock','op':'lt','value':'2023-07-01T00:00:00.1Z'}],
  'returns':[{'field':'r.physical_key','alias':'id'}]}
 rows.append(('relationship_instant_window',single(c),['id'],[['r1'],['r3'],['r4']]))
 return [{'name':name,'plan':p,'expected_columns':columns,'expected_rows':expected} for name,p,columns,expected in rows]
