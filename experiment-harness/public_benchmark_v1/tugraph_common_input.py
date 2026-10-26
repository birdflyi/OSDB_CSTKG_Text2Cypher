"""Pure common input contract. No model, gold, database or network dependency."""
from copy import deepcopy
import hashlib
import json

from entity_candidates import tokens
from schema_services import UnsupportedPlan


VERSION = 'native-common-input-dev-1'
CANDIDATE_LIMIT = 12
CANDIDATE_NOTICE = (
    'Candidates come only from graph STRING attributes and declared primary keys. '
    'They are lexical suggestions, may be ambiguous/incomplete, and are not answers. '
    'Use exact declared properties and typed values only when they fit the question. '
    'Do not rewrite the question or broaden a condition to get a nonempty answer.')

DIALECT_NOTICE = """The engine is pinned native TuGraph 4.5.2, not Neo4j.
Only actual schema properties exist; entity identity is identity_property.
Do not assume eid, rid, aliases, name or description unless actually declared.
Use actual simple schema identifiers; quoted label spelling was not equivalent
in independent checks. Use explicit variable lists in WITH, not WITH *.
Separate consecutive MATCH/OPTIONAL stages using explicit WITH bindings.
Global post-OPTIONAL predicates require a subsequent explicit WITH stage;
carry needed nodes and scalar edge properties, avoiding unused nullable edges.
Preserve relationship-bag multiplicity, NULLs and zero-neighbor groups.
For three-valued WHERE negation, guard compared operands IS NOT NULL before NOT.
When counting entities, count the actual declared non-null primary property;
do not use a node-object COUNT DISTINCT that can bypass an expansion here.
Native datetime literals use a space separator and up to six fractional digits.
Do not guess timezones or silently drop precision. BOOL values are true/false.
No writes/admin/procedures. Return only requested scalar/list/map properties,
not node/relationship/path objects. No default LIMIT; preserve requested order.
"""

DIRECT_INSTRUCTIONS = """Translate the question into one read-only native Cypher query.
Return only the query without Markdown or explanation. Use the complete schema,
common entity suggestions and native-engine notes. General read query constructs
supported by the engine are permitted; you are not restricted to the service IR.
Keep the requested predicates, multiplicities, distinctness, grouping and order.
If the engine cannot express the request, do not replace it with an easier task.
"""

PLAN_INSTRUCTIONS = """Return one JSON object only, without Markdown.
If outside the finite language return {"status":"UNSUPPORTED","reason":"..."}.
Otherwise return {"status":"ANSWER","calls":[selection],"combine":{"op":"single"}}.
A selection has nodes:[{id,label}], edges:[{id,source,target,label,optional?}],
where:[{field,op,value?|other_field?,negate?}], returns:[{field,alias,agg?,distinct?}].
Use actual schema labels/properties and directed endpoints. Field is a variable
plus actual property, e.g. p.<actual primary property>, never a fabricated eid.
Up to four connected nodes and three edges; the first node is the required root.
Mandatory edges share one MATCH, with distinct relationship bindings. At most
one optional edge can attach to required endpoints and introduce a nullable node.
Global where filters apply after optional expansion. Count nullable primary
properties/nodes for zero groups; count(*) also counts the unmatched root row.
where is a conjunction, with eq,ne,lt,le,gt,ge,in,contains,starts_with,ends_with,
is_null,not_null. negate applies three-valued NOT; other_field compares same types.
BOOL admits eq/ne/in/null checks. DATE uses ISO dates. DATETIME admits scalar
comparisons of naive ISO timestamps at microsecond precision; no timezone guess.
returns aggregates: count,sum,avg,min,max,collect; count also admits * or a node variable.
Optional group_by lists actual property keys; to keep same-named entities apart,
group by the declared primary property. Other selection fields: distinct,
order_by:[{column,direction:"ASC"|"DESC"}], limit only when the question requests it.
For union/intersection/difference use exactly two unaggregated/unordered/unlimited
selections, with aligned columns, projecting the answer entity's primary as id.
All payload must belong to that same entity type. combine is {op,key:"id",
output:"count",alias}, or output:"rows",columns:[requested aliases].
No arbitrary query/expression text, subqueries, variable-length paths, multiple
optional edges, HAVING, regex, arithmetic or multi-stage summary joins.
Do not discard an unsupported condition or silently fall back to direct Cypher.
"""


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


class NativeEntityCandidates:
    def __init__(self, schema, entities):
        declarations = {e['label']: e for e in schema['entities']}
        self.rows = []
        seen = set()
        for row in entities:
            entity = declarations[row['label']]
            primary = entity['identity_property']
            properties = deepcopy(row['properties'])
            value = properties[primary]
            kind = entity['properties'][primary]
            if type(value) is not {'str': str, 'int': int}[kind] or value == '':
                raise UnsupportedPlan('Catalog identity violates the declared primary type')
            identity = (row['label'], type(value).__name__, value)
            if identity in seen:
                raise UnsupportedPlan('Duplicate graph identity in entity catalog')
            seen.add(identity)
            searchable = {key: v for key, v in properties.items()
                          if entity['properties'].get(key) == 'str' and type(v) is str and v}
            if kind == 'int':
                searchable[primary] = str(value)
            self.rows.append({'label': row['label'], 'primary_property': primary,
                              'primary_value': value, 'searchable': searchable})

    def find(self, question, limit=CANDIDATE_LIMIT):
        if type(limit) is not int or not 1 <= limit <= CANDIDATE_LIMIT:
            raise UnsupportedPlan('Invalid common candidate limit')
        terms = set(tokens(question))
        ranked = []
        for row in self.rows:
            matched, score = {}, 0.0
            for key, value in row['searchable'].items():
                words = set(tokens(value)); overlap = len(terms & words)
                if overlap:
                    matched[key] = row['primary_value'] if key == row['primary_property'] else value
                    score = max(score, overlap/len(words) + overlap/max(len(terms),1))
            if score:
                ranked.append({k: row[k] for k in ('label','primary_property','primary_value')})
                ranked[-1].update({'matched_properties': matched, 'lexical_score': round(score,8)})
        ranked.sort(key=lambda r: (-r['lexical_score'],r['label'],canonical(r['primary_value'])))
        return ranked[:limit]


def common_input(*, task_id, graph, question, schema, candidates):
    if any(type(v) is not str or not v.strip() for v in (task_id,graph,question)):
        raise UnsupportedPlan('Common task identity and question must be nonempty strings')
    if type(candidates) is not list or len(candidates)>CANDIDATE_LIMIT:
        raise UnsupportedPlan('Common input exceeds the fixed candidate limit')
    # Explicit arguments prevent an entire task record/reference from passing
    # through by accident. Do not copy difficulty/source/gold/query metadata.
    public_schema = {'entities':deepcopy(schema['entities']), 'relations':deepcopy(schema['relations'])}
    payload = {'task_id':task_id,'graph':graph,'question':question,'schema':public_schema,
               'entity_candidates':deepcopy(candidates),'candidate_notice':CANDIDATE_NOTICE,
               'database_dialect':DIALECT_NOTICE}
    serialized = canonical(payload)
    return {'payload':payload,'serialized':serialized,
            'sha256':hashlib.sha256(serialized.encode('utf-8')).hexdigest(),
            'formal_admission':False,'input_version':VERSION}


def messages(shared, arm):
    if arm not in ('direct','ours'):
        raise UnsupportedPlan('Unknown native comparison arm')
    return [{'role':'system','content':DIALECT_NOTICE+'\n'+
             (DIRECT_INSTRUCTIONS if arm=='direct' else PLAN_INSTRUCTIONS)},
            {'role':'user','content':shared['serialized']}]
