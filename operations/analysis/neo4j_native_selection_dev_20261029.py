"""Bounded native Neo4j selection compiler for offline preparation.

Uses public author schema declarations, not observed DB types or TEST queries.
This explicit new profile does not change the frozen CypherBench compiler.
No database I/O, model client, graph loader, set executor or primary scorer.
"""

from copy import deepcopy
from datetime import date
import math
import re


PROFILE = 'native-neo4j-selection-preparation-v1'
TYPES = {'STRING', 'INTEGER', 'FLOAT', 'BOOLEAN', 'LIST', 'DATE', 'TIME',
         'LOCAL_TIME', 'DATE_TIME', 'LOCAL_DATE_TIME', 'DURATION', 'POINT'}
CONSTRUCTORS = {'DATE':'date', 'TIME':'time', 'LOCAL_TIME':'localtime',
                'DATE_TIME':'datetime', 'LOCAL_DATE_TIME':'localdatetime',
                'DURATION':'duration', 'POINT':'point'}
COMPARISONS = {'eq':'=', 'ne':'<>', 'lt':'<', 'le':'<=', 'gt':'>', 'ge':'>='}


class UnsupportedSelection(ValueError):
    pass


def fields(value, required, optional=()):
    if type(value) is not dict or set(required)-value.keys() or value.keys()-set(required)-set(optional):
        raise UnsupportedSelection('Expected only the declared required/optional object fields')


def name(value):
    if type(value) is not str or not re.fullmatch('[A-Za-z][A-Za-z0-9_]{0,31}', value):
        raise UnsupportedSelection('Invalid bounded variable or output alias')
    return value


def quote(value):
    if type(value) is not str or not value or '\x00' in value:
        raise UnsupportedSelection('Invalid schema identifier')
    return '`' + value.replace('`', '``') + '`'


def boolean(value):
    if type(value) is not bool:
        raise UnsupportedSelection('Expected a boolean flag')
    return value


def number(value):
    # Bolt integers are signed int64; test floats without converting large ints.
    return (type(value) is int and -(2**63) <= value < 2**63
            or type(value) is float and math.isfinite(value))


def primitive(value):
    return type(value) in (str,bool) or number(value)


class NativeSchema:
    """Retain label-scoped MTQ or global-vocabulary ZOG property declarations.

    Global vocabulary does NOT certify that a property exists on each label,
    is non-null, is unique or has an observed storage type. No identity field,
    relation property or temporal annotation is invented from another dataset.
    """

    def __init__(self, author_schema, *, format):
        self.format = format
        self.nodes, self.relations, self.global_properties = {}, {}, None
        self.endpoint_only_nodes = set()

        def property_type(value):
            if type(value) is not str or value not in TYPES:
                raise UnsupportedSelection('Unsupported declared property type')
            return value

        def property_list(rows):
            if type(rows) is not list:
                raise UnsupportedSelection('Expected author property declarations')
            output = {}
            for row in rows:
                fields(row, ['property','datatype'])
                key = row['property']; quote(key)
                if key in output:
                    raise UnsupportedSelection('Duplicate schema property')
                output[key] = property_type(row['datatype'])
            return output

        if format == 'MTQ':
            fields(author_schema, ['node_props','rel_props','relationships'])
            if type(author_schema['node_props']) is not dict or type(author_schema['rel_props']) is not dict:
                raise UnsupportedSelection('Expected label-scoped property declarations')
            for label, properties in author_schema['node_props'].items():
                quote(label); self.nodes[label] = property_list(properties)
            if type(author_schema['relationships']) is not list:
                raise UnsupportedSelection('Expected directed relation signatures')
            for relation in author_schema['relationships']:
                fields(relation, ['start','end','type'])
                signature = relation['type'], relation['start'], relation['end']
                if signature in self.relations:
                    raise UnsupportedSelection('Duplicate directed relation signature')
                self.relations[signature] = property_list(author_schema['rel_props'].get(relation['type'], []))
            self.property_scope = 'author_label_declarations'
        elif format == 'ZOGRASCOPE':
            fields(author_schema, ['classes','properties','relations'])
            if any(type(author_schema[key]) is not dict for key in author_schema):
                raise UnsupportedSelection('Expected author class/property/relation vocabularies')
            for label in author_schema['classes']:
                quote(label); self.nodes[label] = {}
            self.global_properties = {}
            for prop, declaration in author_schema['properties'].items():
                quote(prop)
                self.global_properties[prop] = property_type(declaration['type'])
            for label, declaration in author_schema['relations'].items():
                signature = label, declaration['domain'], declaration['range']
                self.relations[signature] = {}
            self.property_scope = 'author_global_vocabulary_not_per_label_presence'
        else:
            raise UnsupportedSelection('Explicit author schema format required')
        if not self.nodes:
            raise UnsupportedSelection('Empty author label vocabulary')
        for label, start, end in self.relations:
            quote(label)
            for endpoint in (start,end):
                quote(endpoint)
                if endpoint not in self.nodes:
                    # A public relation signature itself declares this label.
                    # Missing property declarations remain missing, not guessed.
                    self.nodes[endpoint] = {}
                    self.endpoint_only_nodes.add(endpoint)

    def properties(self, kind, label_or_signature):
        if self.global_properties is not None:
            return self.global_properties
        return self.nodes[label_or_signature] if kind == 'node' else self.relations[label_or_signature]


class _Compiler:
    def __init__(self, schema):
        if not isinstance(schema, NativeSchema):
            raise UnsupportedSelection('Explicit prepared native schema required')
        self.schema, self.bindings, self.parameters = schema, {}, {}

    def field(self, reference, *, entity=False):
        if type(reference) is not str:
            raise UnsupportedSelection('Expected a declared field reference')
        if entity and reference in self.bindings:
            return quote(reference), self.bindings[reference]['kind']
        variable, dot, prop = reference.partition('.')
        if not dot or variable not in self.bindings or prop not in self.bindings[variable]['properties']:
            raise UnsupportedSelection('Unknown property; no injected eid/rid/name fields')
        return quote(variable)+'.'+quote(prop), self.bindings[variable]['properties'][prop]

    def pattern(self, call):
        nodes, edges = call['nodes'], call['edges']
        if type(nodes) is not list or not 1 <= len(nodes) <= 4 or type(edges) is not list or len(edges) > 3:
            raise UnsupportedSelection('One to four nodes and at most three edges required')
        for node in nodes:
            fields(node, ['id','label'])
            variable = name(node['id']); label = node['label']
            quote(label)
            if label not in self.schema.nodes or variable in self.bindings:
                raise UnsupportedSelection('Unknown node label or duplicate variable')
            self.bindings[variable] = dict(kind='node', label=label,
                properties=self.schema.properties('node', label), nullable=False)
        node_names = set(self.bindings)
        required = {nodes[0]['id']}
        adjacency = {v:set() for v in node_names}
        optional, mandatory = [], []
        for edge in edges:
            fields(edge, ['id','source','target','label'], ['optional'])
            variable = name(edge['id']); a, b = edge['source'], edge['target']
            quote(a); quote(b); quote(edge['label'])
            if variable in self.bindings or a not in node_names or b not in node_names:
                raise UnsupportedSelection('Duplicate relationship variable or undeclared endpoint')
            signature = edge['label'], self.bindings[a]['label'], self.bindings[b]['label']
            if signature not in self.schema.relations:
                raise UnsupportedSelection('Relation type or direction not declared')
            is_optional = boolean(edge.get('optional', False))
            self.bindings[variable] = dict(kind='relationship', label=edge['label'],
                properties=self.schema.properties('relationship', signature), nullable=is_optional)
            (optional if is_optional else mandatory).append(edge)
            if not is_optional:
                required.update((a,b)); adjacency[a].add(b); adjacency[b].add(a)
        if len(optional) > 1:
            raise UnsupportedSelection('At most one optional relationship in this bounded profile')
        reached = {nodes[0]['id']}
        while True:
            grown = reached | {b for a in reached for b in adjacency[a]}
            if grown == reached: break
            reached = grown
        if reached != required:
            raise UnsupportedSelection('Mandatory pattern must connect to the root node')
        covered = set(required)
        for edge in optional:
            if not ({edge['source'],edge['target']} & required):
                raise UnsupportedSelection('Optional pattern needs a bound endpoint')
            covered.update((edge['source'],edge['target']))
        if covered != node_names:
            raise UnsupportedSelection('Disconnected/unbound nodes are not admitted')
        for variable in node_names-required:
            self.bindings[variable]['nullable'] = True
        parts = ['('+quote(n['id'])+':'+quote(n['label'])+')' for n in nodes if n['id'] in required]
        def edge_pattern(edge, introduce=False):
            def endpoint(variable):
                label = ':'+quote(self.bindings[variable]['label']) if introduce and variable not in required else ''
                return '('+quote(variable)+label+')'
            return endpoint(edge['source'])+'-['+quote(edge['id'])+':'+quote(edge['label'])+']->'+endpoint(edge['target'])
        # One MATCH clause preserves relationship uniqueness across all required
        # edge variables. Separate OPTIONAL MATCH retains zero-association rows.
        parts.extend(edge_pattern(edge) for edge in mandatory)
        clauses = ['MATCH '+','.join(parts)]
        clauses.extend('OPTIONAL MATCH '+edge_pattern(edge, True) for edge in optional)
        return clauses

    def literal(self, value, kind, parameter):
        matches = {'STRING':type(value) is str, 'INTEGER':type(value) is int and number(value),
                   'FLOAT':number(value),
                   'BOOLEAN':type(value) is bool,
                   'LIST':type(value) is list and all(primitive(x) for x in value)}
        if kind in CONSTRUCTORS:
            # Keep native temporal text/precision unchanged. Neo4j validates the
            # constructor's full syntax during execution; no UTC/string rewrite.
            valid = type(value) is str
            if kind == 'POINT':
                numeric = {'x','y','z','longitude','latitude','height','srid'}
                valid = type(value) is dict and bool(value) and set(value) <= numeric|{'crs'}
                if valid:
                    for key,item in value.items():
                        if key == 'crs':
                            valid = valid and type(item) is str
                        elif key == 'srid':
                            valid = valid and type(item) is int and number(item)
                        else:
                            valid = valid and number(item)
            if not valid:
                raise UnsupportedSelection('Expected a native temporal/spatial constructor argument')
            if kind == 'DATE':
                if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):
                    raise UnsupportedSelection('Date literals require extended ISO YYYY-MM-DD')
                try: date.fromisoformat(value)
                except ValueError as error: raise UnsupportedSelection('Invalid ISO date literal') from error
            expression = CONSTRUCTORS[kind]+'($'+parameter+')'
        else:
            if not matches.get(kind, False):
                raise UnsupportedSelection('Literal does not match its declared property type')
            expression = '$'+parameter
        self.parameters[parameter] = deepcopy(value)
        return expression

    def predicate(self, predicate, index):
        fields(predicate, ['field','op'], ['value','other_field','negate'])
        negate = boolean(predicate.get('negate', False))
        left, kind = self.field(predicate['field'], entity=True)
        op = predicate['op']
        if type(op) is not str:
            raise UnsupportedSelection('Expected a predicate operator name')
        if op in ('is_null','not_null'):
            if 'value' in predicate or 'other_field' in predicate:
                raise UnsupportedSelection('NULL predicate accepts no argument')
            expression = left+(' IS NULL' if op == 'is_null' else ' IS NOT NULL')
        else:
            if ('value' in predicate) == ('other_field' in predicate):
                raise UnsupportedSelection('Exactly one predicate argument required')
            if kind in ('BOOLEAN','POINT','DURATION','node','relationship') and op not in ('eq','ne','in'):
                raise UnsupportedSelection('Ordering/string operations not admitted for this type')
            if 'other_field' in predicate:
                right, other_kind = self.field(predicate['other_field'], entity=True)
                if op not in COMPARISONS or not (kind == other_kind or {kind,other_kind} <= {'INTEGER','FLOAT'}):
                    raise UnsupportedSelection('Incompatible field comparison')
            elif op == 'in':
                value = predicate['value']
                if type(value) is not list or kind in ('LIST','node','relationship'):
                    raise UnsupportedSelection('IN requires a list of typed scalar values')
                # Constructors apply per list member, including native dates.
                right = '['+','.join(self.literal(v,kind,'p'+str(index)+'_'+str(i)) for i,v in enumerate(value))+']'
            elif op == 'contains' and kind == 'LIST':
                value = predicate['value']
                if not primitive(value):
                    raise UnsupportedSelection('Untyped author LIST membership needs a finite primitive')
                key = 'p'+str(index); self.parameters[key] = value; right = '$'+key
            else:
                right = self.literal(predicate['value'], kind, 'p'+str(index))
            if op in COMPARISONS: expression = left+COMPARISONS[op]+right
            elif op == 'in': expression = left+' IN '+right
            elif op == 'contains' and kind == 'LIST': expression = right+' IN '+left
            elif op in ('contains','starts_with','ends_with') and kind == 'STRING':
                expression = left+' '+{'contains':'CONTAINS','starts_with':'STARTS WITH','ends_with':'ENDS WITH'}[op]+' '+right
            else: raise UnsupportedSelection('Unsupported predicate operation/type')
        # Preserve Cypher three-valued logic. No NULL->false rewrite before NOT.
        return 'NOT ('+expression+')' if negate else expression

    def projection(self, call, clauses):
        returns = call['returns']
        if type(returns) is not list or not 1 <= len(returns) <= 8:
            raise UnsupportedSelection('One to eight return items required')
        outputs, columns, projected, aggregates = [], [], {}, []
        for item in returns:
            fields(item, ['field','alias'], ['agg','distinct'])
            alias = name(item['alias']); aggregate = item.get('agg')
            if alias in columns: raise UnsupportedSelection('Duplicate output alias')
            expression, kind = ('*','record') if item['field'] == '*' and aggregate == 'count' else self.field(item['field'],entity=True)
            if aggregate is not None:
                if aggregate not in ('count','sum','avg','min','max','collect'):
                    raise UnsupportedSelection('Unsupported aggregate')
                distinct = boolean(item.get('distinct',False))
                if expression == '*' and distinct or aggregate in ('sum','avg') and kind not in ('INTEGER','FLOAT'):
                    raise UnsupportedSelection('Invalid numeric/COUNT DISTINCT * aggregate')
                if aggregate in ('min','max') and kind in ('node','relationship','LIST','POINT','DURATION'):
                    raise UnsupportedSelection('This bounded aggregate requires an ordered scalar type')
                expression = aggregate+'('+('DISTINCT ' if distinct else '')+expression+')'
                aggregates.append(alias)
            else:
                if 'distinct' in item: raise UnsupportedSelection('Per-item DISTINCT requires aggregation')
                projected[alias] = {'field':item['field'],'kind':kind}
            outputs.append(expression+' AS '+quote(alias)); columns.append(alias)
        distinct = boolean(call.get('distinct',False))
        if 'group_by' in call:
            keys = call['group_by']
            if type(keys) is not list or not 1 <= len(keys) <= 4 or any(type(k) is not str for k in keys) or len(set(keys)) != len(keys) or not aggregates:
                raise UnsupportedSelection('Explicit grouping needs aggregation and one to four unique keys')
            grouped = [self.field(key,entity=True)[0] for key in keys]
            for item in projected.values():
                reference = item['field']
                if reference not in keys and reference.partition('.')[0] not in keys:
                    raise UnsupportedSelection('Projection is not determined by declared grouping keys')
            hidden = [value+' AS `__group_'+str(i)+'`' for i,value in enumerate(grouped)]
            clauses.append('WITH '+','.join(hidden+outputs))
            clauses.append('RETURN '+('DISTINCT ' if distinct else '')+','.join(quote(c) for c in columns))
        else:
            clauses.append('RETURN '+('DISTINCT ' if distinct else '')+','.join(outputs))
        ordering = call.get('order_by',[])
        if type(ordering) is not list: raise UnsupportedSelection('Expected an ordering list')
        order = []
        for item in ordering:
            fields(item, ['column','direction'])
            if item['column'] not in columns or item['direction'] not in ('ASC','DESC'):
                raise UnsupportedSelection('Ordering requires a returned alias and ASC/DESC')
            order.append(quote(item['column'])+' '+item['direction'])
        if order: clauses.append('ORDER BY '+','.join(order))
        if 'limit' in call:
            if type(call['limit']) is not int or not 1 <= call['limit'] <= 10000:
                raise UnsupportedSelection('Explicit limit must be within the bounded IR contract')
            self.parameters['requested_limit'] = call['limit']; clauses.append('LIMIT $requested_limit')
        return columns, projected, aggregates


def compile_selection(call, schema):
    """Lower the bounded selection IR; actual Neo4j acceptance is still pending.

    Bare entity fields (e.g. field='n') project native values, including for
    explicit entity grouping. Properties remain variable.property. No .eid
    alias substitution, gold input, automatic LIMIT or extra DB probe is used.
    """
    fields(call, ['nodes','edges','where','returns'], ['distinct','group_by','order_by','limit'])
    compiler = _Compiler(schema)
    clauses = compiler.pattern(call)
    predicates = call['where']
    if type(predicates) is not list or len(predicates) > 16:
        raise UnsupportedSelection('At most sixteen AND predicates required')
    if predicates:
        if any(edge.get('optional',False) for edge in call['edges']): clauses.append('WITH *')
        clauses.append('WHERE '+' AND '.join('('+compiler.predicate(p,i)+')' for i,p in enumerate(predicates)))
    columns, projected, aggregates = compiler.projection(call,clauses)
    return dict(profile=PROFILE, query=' '.join(clauses), parameters=compiler.parameters, columns=columns,
                contract=dict(bindings=deepcopy(compiler.bindings), projected=projected, aggregates=aggregates,
                    group_by=deepcopy(call.get('group_by')), explicit_limit=call.get('limit'),
                    distinct=call.get('distinct',False), property_scope=schema.property_scope,
                    endpoint_only_labels_without_property_declarations=sorted(schema.endpoint_only_nodes),
                    entity_identity='native_object_in_one_explicit_transaction',
                    actual_schema_storage_types_verified=False, formal_admission=False))
