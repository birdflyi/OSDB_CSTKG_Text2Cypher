"""Portable public-input preparation for MTQ and ZOGRASCOPE.

No prompt template, model settings, database client or experiment admission.
Model-facing objects are explicit projections; evaluation labels and reference
queries live in another directory and never enter the public loader.
"""

from collections import Counter
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import shutil


PROFILES = {
    'MindTheQuery': {'variants': {'plain': 'question'}, 'split_field': 'track'},
    'ZOGRASCOPE': {'variants': {'plain': 'question', 'gold_linked': 'gold_linked_question'},
                  'split_field': 'split'},
}


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def public_input(row, schema, graph, question_field):
    """The same common object feeds every method; task metadata is not a prompt."""
    question = row.get(question_field)
    if type(question) is not str or not question.strip():
        raise ValueError('Missing question for the explicitly selected variant')
    if type(graph) is not str or not graph or type(schema) is not dict:
        raise ValueError('Expected a graph binding and public schema')
    return {'graph': graph, 'question': question, 'schema': deepcopy(schema)}


def _jsonl(path):
    with Path(path).open(encoding='utf-8') as stream:
        for line in stream:
            yield json.loads(line)


def _write_jsonl(path, values):
    with path.open('xb') as stream:
        for value in values:
            stream.write(encoded(value) + b'\n')


def _inside(root, relative):
    path = (root / relative).resolve()
    path.relative_to(root.resolve())
    return path


def prepare_package(benchmark, source, bindings, output):
    """Validate the full source population, then write a preparation-only package."""
    if benchmark not in PROFILES:
        raise ValueError('Unknown preparation profile')
    source, output = Path(source), Path(output)
    profile = PROFILES[benchmark]
    source_manifest = json.loads((source / 'manifest.json').read_text(encoding='utf-8'))
    public_path, reference_path = source / 'public_inputs.jsonl', source / 'evaluator_references.jsonl'
    for path in (public_path, reference_path):
        if digest(path.read_bytes()) != source_manifest[path.name]['sha256']:
            raise ValueError('Prepared source bytes changed')
    rows, references = list(_jsonl(public_path)), list(_jsonl(reference_path))
    identities = [r['task_id'] for r in rows]
    if (not identities or len(identities) != len(set(identities))
            or len(rows) != source_manifest['N']):
        raise ValueError('Missing or duplicate public task positions')
    reference_ids = [r['task_id'] for r in references]
    if len(reference_ids) != len(set(reference_ids)) or set(reference_ids) != set(identities):
        raise ValueError('Reference population mismatch; never truncate or deduplicate')
    references_by_id = {r['task_id']: r for r in references}
    aliases, schemas, graph_bindings = {}, {}, {}
    for binding in bindings:
        if binding['benchmark'] != benchmark:
            continue
        graph = binding['task_graph']
        path = Path(binding['schema_path'])
        raw = path.read_bytes()
        if digest(raw) != binding['schema_sha256']:
            raise ValueError('Public schema digest mismatch')
        schema = json.loads(raw)
        schema_file = 'schemas/' + digest(graph.encode())[:16] + '.json'
        schemas[graph] = schema
        graph_bindings[graph] = {'schema_file': schema_file, 'schema_sha256': digest(raw),
                                 'offline_store': binding['offline_store'],
                                 'live_schema_certified': False}
        for alias in binding.get('task_aliases', [graph]):
            if alias in aliases:
                raise ValueError('Ambiguous task graph alias')
            aliases[alias] = graph
    projected = {variant: [] for variant in profile['variants']}
    cohorts = []
    for row in rows:
        graph = aliases[row['graph']]
        for variant, field in profile['variants'].items():
            common = public_input(row, schemas[graph], graph, field)
            projected[variant].append({'task_id': row['task_id'], 'graph': graph,
                'question': common['question'], 'input_variant': variant,
                'schema_file': graph_bindings[graph]['schema_file'],
                'schema_sha256': graph_bindings[graph]['schema_sha256'],
                'common_input_sha256': digest(encoded(common))})
        reference = references_by_id[row['task_id']]
        cohorts.append({'task_id': row['task_id'], 'graph': graph,
                        'split': row[profile['split_field']], 'category': row.get('category'),
                        'source_file': row['source_file'], 'source_position': row['source_position'],
                        'query_type': reference.get('query_type'),
                        'template_id': reference.get('template_id'),
                        'source_validation_labels': deepcopy(reference.get('validation_labels'))})
    output.mkdir(parents=True, exist_ok=False)
    for name in ('public', 'schemas', 'evaluation'):
        (output / name).mkdir()
    for graph, binding in graph_bindings.items():
        original = next(b for b in bindings if b['benchmark'] == benchmark and b['task_graph'] == graph)
        shutil.copyfile(original['schema_path'], output / binding['schema_file'])
    for variant, values in projected.items():
        _write_jsonl(output / 'public' / (variant + '.jsonl'), values)
    _write_jsonl(output / 'evaluation/cohorts.jsonl', cohorts)
    shutil.copyfile(reference_path, output / 'evaluation/references.jsonl')
    files = {p.relative_to(output).as_posix(): digest(p.read_bytes())
             for p in sorted(output.rglob('*')) if p.is_file()}
    manifest = {'kind': 'PUBLIC_INPUT_PACKAGE_PREPARATION_NOT_MODEL_PROMPTS_OR_ADMISSION',
        'benchmark': benchmark, 'positions_per_variant': len(rows),
        'variants': list(projected), 'splits': dict(Counter(r['split'] for r in cohorts)),
        'graphs': graph_bindings, 'files': files,
        'source_public_sha256': digest(public_path.read_bytes()),
        'source_reference_sha256': digest(reference_path.read_bytes()),
        'scoring_contract_frozen': False, 'prompt_frozen': False,
        'common_input_hash_is_not_a_prediction_cache_key': True,
        'prediction_reuse_requires_identical_model_prompt_permissions_training_and_ICL': True,
        'model_calls': 0, 'DB_queries': 0, 'experiment_enabled': False}
    (output / 'manifest.json').write_bytes(encoded(manifest) + b'\n')
    return manifest


def iter_public_tasks(package, variant, *, expected_manifest_sha256):
    """Load one declared input variant. Never opens the evaluation directory."""
    package = Path(package)
    raw = (package / 'manifest.json').read_bytes()
    if digest(raw) != expected_manifest_sha256:
        raise ValueError('Package manifest digest mismatch')
    manifest = json.loads(raw)
    if variant not in manifest['variants']:
        raise ValueError('Input variant must be explicitly prepared')
    name = 'public/' + variant + '.jsonl'
    path = _inside(package, name)
    if digest(path.read_bytes()) != manifest['files'][name]:
        raise ValueError('Public task file changed')
    rows, schemas, seen = list(_jsonl(path)), {}, set()
    if len(rows) != manifest['positions_per_variant']:
        raise ValueError('Public input population changed')
    prepared = []
    for row in rows:
        identity = row['task_id']
        if identity in seen or row['input_variant'] != variant:
            raise ValueError('Duplicate identity or mixed input variant')
        seen.add(identity)
        binding = manifest['graphs'][row['graph']]
        if (row['schema_file'] != binding['schema_file']
                or row['schema_sha256'] != binding['schema_sha256']):
            raise ValueError('Task schema binding mismatch')
        schema_file = row['schema_file']
        if not schema_file.startswith('schemas/'):
            raise ValueError('Public schemas must stay inside the schemas directory')
        if schema_file not in schemas:
            raw = _inside(package / 'schemas', schema_file[len('schemas/'):]).read_bytes()
            if digest(raw) != row['schema_sha256'] or digest(raw) != manifest['files'][schema_file]:
                raise ValueError('Schema bytes changed')
            schemas[schema_file] = json.loads(raw)
        common = public_input(row, schemas[schema_file], row['graph'], 'question')
        if digest(encoded(common)) != row['common_input_sha256']:
            raise ValueError('Common model input digest mismatch')
        prepared.append((identity, common))
    # Validate the whole declared input before releasing the first task.
    yield from prepared
