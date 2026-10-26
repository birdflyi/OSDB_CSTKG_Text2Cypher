"""Conservative workload screen, NOT a Cypher parser/compiler or support oracle.

Only explicit RETURN COUNT(DISTINCT bound-node/declared-primary) is recognized.
No plan is generated; no source method, score, input or task membership changes.
Partition-shaped predicates do not establish distinct values, safe summaries,
or the mechanism chosen by an NL planner. Unrecognized tasks stay in the census.
"""
import re

VERSION = 'task-composition-screen-1'


def tokens(text):
    out=[];i=0
    while i<len(text):
        c=text[i]
        if c.isspace():i+=1
        elif text.startswith('//',i):
            end=text.find('\n',i+2);i=len(text) if end<0 else end
        elif text.startswith('/*',i):
            end=text.find('*/',i+2)
            if end<0:raise ValueError('Unclosed comment')
            i=end+2
        elif c in "'\"`":
            quote=c;i+=1;value=[];closed=False
            while i<len(text):
                if text[i]=='\\' and quote!='`':i+=2
                elif text[i]==quote:
                    i+=1
                    if i<len(text) and text[i]==quote:value.append(quote);i+=1
                    else:closed=True;break
                else:value.append(text[i]);i+=1
            if not closed:raise ValueError('Unclosed literal/identifier')
            out.append(('identifier',''.join(value)) if quote=='`' else ('literal',''))
        elif c.isalpha() or c=='_':
            end=i+1
            while end<len(text) and (text[end].isalnum() or text[end]=='_'):end+=1
            out.append(('word',text[i:end]));i=end
        else:out.append(('punct',c));i+=1
    return out


def _word(token,value):return token[0]=='word' and token[1].upper()==value
def _identifier(token):return token[0] in ('word','identifier')


def _keyword(ts,i,value):
    return _word(ts[i],value) and (i==0 or ts[i-1][1] not in ('.',':','$'))


def _strip_outer(ts):
    while len(ts)>=2 and ts[0][1]=='(' and ts[-1][1]==')':
        depth=0;whole=True
        for i,token in enumerate(ts):
            depth+=(token[1]=='(')-(token[1]==')')
            if depth==0 and i<len(ts)-1:whole=False;break
        if not whole:break
        ts=ts[1:-1]
    return ts


def _split(ts,word):
    ts=_strip_outer(ts);parts=[];start=0;depth=0
    for i,t in enumerate(ts):
        if t[1]=='(':depth+=1
        elif t[1]==')':depth-=1
        elif depth==0 and _keyword(ts,i,word):parts.append(ts[start:i]);start=i+1
    parts.append(ts[start:]);return parts


def screen(query, primary_properties):
    ts=tokens(query)
    if ts and ts[-1][1]==';':ts=ts[:-1]
    stack=[];top=[];pairs={')':'(',']':'[','}':'{'}
    for i,t in enumerate(ts):
        if t[0] not in ('word','identifier','literal'):
            if t[1] in ('(', '[', '{'):stack.append(t[1])
            elif t[1] in pairs:
                if not stack or stack.pop()!=pairs[t[1]]:raise ValueError('Unbalanced delimiters')
        if not stack and t[0]=='word' and (i==0 or ts[i-1][1] not in ('.',':','$')):
            top.append((i,t[1].upper()))
    if stack:raise ValueError('Unbalanced delimiters')
    flags={k:any(_keyword(ts,i,k) for i in range(len(ts)))
           for k in ('UNION','WITH','UNWIND','OPTIONAL','OR','DISTINCT')}
    result={'recognized_distinct_entity_count':False,
            'one_stage_entity_count':False,'one_stage_entity_count_with_OR':False,
            'equality_partition_shape_only':False,'flags':flags,
            'recognition':'OTHER_OR_UNRECOGNIZED_NOT_EXCLUDED'}
    # WITH/UNWIND/subqueries may rebind a variable: do not carry a label from
    # an earlier scope into an apparent entity count.
    if any(w in ('WITH','UNWIND','CALL') for _,w in top):return result
    returns=[i for i,w in top if w=='RETURN']
    if len(returns)!=1:return result
    begin=returns[0]+1
    end=next((i for i,w in top if i>=begin and w in ('ORDER','SKIP','LIMIT','UNION')),len(ts))
    expr=ts[begin:end]
    # Optional alias must be exactly one identifier; no extra return items.
    if len(expr)>=2 and _word(expr[-2],'AS') and _identifier(expr[-1]):expr=expr[:-2]
    if not (len(expr)>=5 and _word(expr[0],'COUNT') and expr[1][1]=='('
            and _word(expr[2],'DISTINCT') and expr[-1][1]==')'):return result
    target=expr[3:-1]
    if not (len(target) in (1,3) and _identifier(target[0])):return result
    if len(target)==3 and not (target[1][1]=='.' and _identifier(target[2])):return result
    var=target[0][1];labels=set()
    for i in range(len(ts)-3):
        if ts[i][1]=='(' and _identifier(ts[i+1]) and ts[i+1][1]==var and ts[i+2][1]==':' and _identifier(ts[i+3]):
            labels.add(ts[i+3][1])
    if len(labels)!=1:return result
    label=next(iter(labels))
    if label not in primary_properties:return result
    if len(target)==3 and target[2][1]!=primary_properties[label]:return result
    result.update(recognized_distinct_entity_count=True,
                  recognition='EXPLICIT_DISTINCT_BOUND_ENTITY_COUNT_SHAPE')
    if any(w in ('WITH','UNION','UNWIND','CALL','OPTIONAL') for _,w in top):return result
    if sum(w=='MATCH' for _,w in top)!=1:return result
    result['one_stage_entity_count']=True
    wheres=[i for i,w in top if w=='WHERE']
    if len(wheres)!=1:return result
    predicate=ts[wheres[0]+1:returns[0]]
    result['one_stage_entity_count_with_OR']=any(_keyword(predicate,i,'OR') for i in range(len(predicate)))
    branches=_split(predicate,'OR')
    # Explicit 2..8 single scalar equalities on the counted variable, same field.
    # This shape alone does not check literal values, disjointness or data types.
    if not 2<=len(branches)<=8:return result
    fields=[]
    for branch in branches:
        a=_strip_outer(branch)
        if not (len(a)==5 and _identifier(a[0]) and a[0][1]==var and a[1][1]=='.'
                and _identifier(a[2]) and a[3][1]=='=' and a[4][0]=='literal'):return result
        fields.append(a[2][1])
    result['equality_partition_shape_only']=len(set(fields))==1
    return result
