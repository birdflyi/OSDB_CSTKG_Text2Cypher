"""Three-arm development comparison. No proposed-method arm or held-out claim."""
import argparse
from collections import Counter
import json
from pathlib import Path

import run_baselines as b


QUERY_PROMPT = """Generate read-only Cypher for the provided schema and questions.
Return ONLY a JSON array with keys id,status,cypher,reason. Keep reason brief.
ANSWER means one query; other statuses require cypher null. Status options:
ANSWER,NEEDS_INPUT,UNSUPPORTED,INSUFFICIENT_EVIDENCE,ERROR.
Preserve all conditions, requested columns and record grain. Use the full graph
for aggregates. Do not guess missing anchors or replace unsupported semantics.
Do not add a result limit when the question requests all records or a count.
"""

# Adaptation of the task/schema/few-shot structure in RepoQA revision-1.
# These are schematic examples, not answers or identifiers from this pilot.
FEWSHOT = """
Additional examples for the supplied schema (DEMO identifiers are schematic):
Question: List Issue identifiers opened by Actor A_DEMO, retaining event records.
Query: MATCH (i:Issue)-[r:EVENT_ACTION]->(a:Actor {entity_id:'A_DEMO'})
WHERE r.service_rel_type='OPENED_BY' RETURN i.entity_id AS issue_id
Question: Count outgoing LINKS_TO records for PullRequest PR_DEMO, including zero.
Query: MATCH (p:PullRequest {entity_id:'PR_DEMO'})-[r:REFERENCE]->(x:ExternalResource)
WHERE r.service_rel_type='LINKS_TO' RETURN count(r) AS record_count
Question: For comments whose identifiers start with IC_DEMO, count REFERENCES
records per Commit identifier, keeping only records in [2021-01-01,2022-01-01) UTC.
Query: MATCH (c:IssueComment)-[r:REFERENCE]->(k:Commit)
WHERE c.entity_id STARTS WITH 'IC_DEMO' AND r.service_rel_type='REFERENCES'
AND r.source_event_time >= '2021-01-01T00:00:00Z'
AND r.source_event_time < '2022-01-01T00:00:00Z'
RETURN k.entity_id AS commit_id,count(r) AS record_count
Question: Who opened the issue? No identifier or earlier context is available.
Decision: NEEDS_INPUT, cypher null. Ask for the Issue identifier.
Use these examples as syntax/semantics illustrations, not as database evidence.
"""


def bag(rows):
    return Counter(json.dumps(r, ensure_ascii=False, sort_keys=True) for r in rows)


def execute(query):
    # One terminal delimiter is presentation, not an additional statement.
    if isinstance(query, str):
        query = query.strip()
        if query.endswith(';'):
            query = query[:-1].rstrip()
    return b.execute(query)


def reference_queries(specs):
    """Private executable reference, never supplied to baseline models."""
    issue = specs[0]['question'].split('Issue ', 1)[1].split('?', 1)[0]
    pr = specs[1]['question'].split('PullRequest ', 1)[1].split(',', 1)[0]
    prefix = specs[3]['question'].split('literal prefix ', 1)[1].split('?', 1)[0]
    quote = lambda s: "'" + s.replace('\\', '\\\\').replace("'", "\\'") + "'"
    iq, pq, fq = map(quote, (issue, pr, prefix))
    links = f"MATCH (p:PullRequest {{entity_id:{pq}}})-[r:REFERENCE]->(x:ExternalResource) WHERE r.service_rel_type='LINKS_TO'"
    return [
        f"MATCH (i:Issue {{entity_id:{iq}}})-[r:EVENT_ACTION]->(a:Actor) WHERE r.service_rel_type='OPENED_BY' RETURN a.entity_id AS actor_id",
        links + ' RETURN r.url_domain_etld1 AS domain,x.entity_id AS external_resource_id',
        links + " AND r.source_event_time >= '2023-01-01T00:00:00Z' AND r.source_event_time < '2024-01-01T00:00:00Z' RETURN r.url_domain_etld1 AS domain,count(r) AS record_count",
        f'MATCH (i:Issue) WHERE i.entity_id STARTS WITH {fq} RETURN count(i) AS issue_count',
        f"MATCH (c:IssueComment)-[a:EVENT_ACTION]->(i:Issue),(c)-[r:REFERENCE]->(k:Commit) WHERE i.entity_id STARTS WITH {fq} AND a.service_rel_type='COMMENTED_ON_ISSUE' AND r.service_rel_type='REFERENCES' RETURN count(*) AS path_record_count",
        links + " AND r.source_event_time >= '2022-01-01T00:00:00Z' AND r.source_event_time < '2023-01-01T00:00:00Z' RETURN count(r) AS record_count",
    ]


class RecordedClient(b.Client):
    def call(self, stage, prompt, data, ids, keys, max_tokens=12000):
        try:
            return super().call(stage, prompt, data, ids, keys, max_tokens)
        except (RuntimeError, ValueError, TypeError, KeyError) as exc:
            folder = self.out / 'calls' / stage
            if not (folder / 'response.json').exists() and not (folder / 'transport_error.json').exists():
                raise  # A pending request is not permission to redispatch.
            record = {'ids': ids, 'generated_by': 'RUNNER_FAILURE_RECORD_NOT_MODEL_ANSWER',
                      'error_type': type(exc).__name__, 'retry': False}
            if not (folder / 'failure.json').exists():
                b.save(folder / 'failure.json', record)
            if 'cypher' in keys:
                return [{'id': i, 'status': 'ERROR', 'cypher': None, 'reason': 'Recorded dispatch/protocol failure'} for i in ids]
            return [{'id': i, 'status': 'ERROR', 'rows': [], 'evidence_ids': []} for i in ids]


def normalize(answers, tasks, out, arm):
    result, invalid = [], []
    for a in answers:
        try:
            n = b.normalize_tables([a], tasks)[0]
            columns = next(t['columns'] for t in tasks if t['id'] == a['id'])
            if not isinstance(n['rows'], list) or not isinstance(n['evidence_ids'], list):
                raise ValueError('Table and citations must be arrays')
            if any(not isinstance(r, list) or len(r) != len(columns) for r in n['rows']):
                raise ValueError('Wrong public table shape')
            if n['status'] != 'ANSWER' and n['rows']:
                raise ValueError('Nonanswer contains table')
            result.append(n)
        except (ValueError, TypeError, KeyError):
            invalid.append(a['id'])
            result.append({'id': a['id'], 'status': 'ERROR', 'rows': [], 'evidence_ids': []})
    b.save(out / (arm + '_presentation_failures.json'), invalid)
    return result


def graph_arm(client, arm, prompt, tasks, out):
    queries = client.batches(arm + '_queries', prompt, b.SCHEMA, tasks,
                            {'id', 'status', 'cypher', 'reason'}, 2)
    b.save(out / (arm + '_queries.json'), queries)
    executions, contexts, decisions = [], [], []
    for task in tasks:
        q = next(q for q in queries if q['id'] == task['id'])
        if q['status'] == 'ANSWER':
            execution = execute(q['cypher'])
        else:
            execution = {'status': 'NO_QUERY'}
            if q['cypher'] is not None:
                execution = {'status': 'PROTOCOL_ERROR'}
        executions.append({'id': task['id'], 'generation': q, 'execution': execution})
        if execution['status'] == 'EXECUTED':
            contexts.append({**task, 'query': q['cypher'], 'execution': execution,
                             'evidence_id': 'EXEC:' + task['id']})
        else:
            status = q['status'] if execution['status'] == 'NO_QUERY' else 'ERROR'
            decisions.append({'id': task['id'], 'status': status, 'rows': [], 'evidence_ids': []})
    b.save(out / (arm + '_execution.json'), executions)
    raw = decisions + client.batches(arm + '_answers', b.ANSWER_PROMPT +
        '\nCite EXEC identifiers. Empty successful results are evidence; failed execution is not zero facts.',
        b.SCHEMA, contexts, {'id', 'status', 'rows', 'evidence_ids'}, 2)
    b.save(out / (arm + '_raw_answers.json'), raw)
    answers = normalize(raw, tasks, out, arm)
    # Also measure query-result agreement before answer generation.
    query_answers = []
    for r in executions:
        e, q = r['execution'], r['generation']
        status = 'ANSWER' if e['status'] == 'EXECUTED' else (q['status'] if e['status'] == 'NO_QUERY' else 'ERROR')
        query_answers.append({'id': r['id'], 'status': status,
                              'rows': e['rows'] if e['status'] == 'EXECUTED' else [],
                              'evidence_ids': ['EXEC:' + r['id']] if e['status'] == 'EXECUTED' else []})
    evidence = {r['id']: {'EXEC:' + r['id']} if r['execution']['status'] == 'EXECUTED' else set() for r in executions}
    return answers, evidence, query_answers


def run(args):
    out, data = Path(args.output).resolve(), Path(args.data).resolve()
    if out.exists():
        raise FileExistsError('Use a new run directory; do not replace attempts')
    if Path(__file__).resolve().parents[1] in out.parents:
        raise ValueError('Artifacts must stay outside checkout')
    cfg = json.loads(Path(args.config).read_text(encoding='utf-8'))
    receipt = json.loads(Path(args.graph_receipt).read_text(encoding='utf-8'))
    hashes = {n: b.sha(data / n) for n in ('nodes.csv', 'edges.csv')}
    if hashes != receipt['source_hashes'] or b.db('CALL db.info() YIELD id,name RETURN id,name')['data'] != receipt['database_identity']:
        raise ValueError('Source/import/database identity mismatch')
    nodes, edges, docs = b.load_graph(data)
    specs = b.build_specs(nodes, edges)
    tasks = [{k: s[k] for k in ('id', 'question', 'columns')} for s in specs]
    b.save(out / 'manifest.json', {'mode': 'THREE_ARM_DEVELOPMENT_NOT_HELDOUT',
        'data_hashes': hashes, 'code_hashes': {p.name: b.sha(p) for p in (Path(__file__), Path(b.__file__))},
        'model': cfg['model'], 'generation': {k: cfg[k] for k in ('temperature', 'top_p', 'reasoning_effort')},
        'batch_size': 2, 'max_completion_tokens': 12000, 'max_calls': 20, 'automatic_retries': 0,
        'schema': b.SCHEMA, 'query_prompt': QUERY_PROMPT, 'fewshot_examples': FEWSHOT,
        'arms': ['BM25_FACT_RAG', 'ZERO_SHOT_GRAPH', 'REPOQA_INSPIRED_FEWSHOT_GRAPH'],
        'comparability': 'Same data/model/tasks/answer protocol; access mechanisms and token budgets consumed differ.',
        'fewshot_status': 'Adapted prompt structure, not exact RepoQA reproduction; see peer_assets provenance',
        'failure_policy': 'Retain all; no answer call for failed/no query; no completion of truncated output'})
    b.save(out / 'task_specifications.json', specs)
    b.save(out / 'tasks.json', tasks)
    references = []
    for s, query in zip(specs, reference_queries(specs)):
        e = execute(query)
        valid = e['status'] == 'EXECUTED' and e['columns'] == s['columns'] and bag(e['rows']) == bag(s['expected_rows'])
        references.append({'id': s['id'], 'query': query, 'execution': e, 'agrees_with_csv': valid})
    b.save(out / 'private_reference_execution.json', references)
    if not all(r['agrees_with_csv'] for r in references):
        raise RuntimeError('Reference mismatch; no model evaluation')
    client, index = RecordedClient(cfg, out), b.BM25(docs)
    contexts = [{**task, 'retrieved_facts': index.retrieve(task['question'])} for task in tasks]
    b.save(out / 'retrieval_contexts.json', contexts)
    raw = client.batches('rag', b.ANSWER_PROMPT, b.SCHEMA, contexts,
                         {'id', 'status', 'rows', 'evidence_ids'}, 2)
    b.save(out / 'rag_raw_answers.json', raw)
    rag = normalize(raw, tasks, out, 'rag')
    allowed = {r['id']: {f['id'] for f in r['retrieved_facts']} for r in contexts}
    scores = {'BM25_FACT_RAG': b.score(specs, rag, allowed)}
    answers = {'BM25_FACT_RAG': rag}
    b.save(out / 'rag_scores.json', scores['BM25_FACT_RAG'])
    for arm, prompt in [('ZERO_SHOT_GRAPH', QUERY_PROMPT), ('REPOQA_INSPIRED_FEWSHOT_GRAPH', QUERY_PROMPT + FEWSHOT)]:
        a, evidence, qa = graph_arm(client, arm, prompt, tasks, out)
        answers[arm] = a
        scores[arm] = b.score(specs, a, evidence)
        # Shape errors in raw query results are a failed query-result task, not a crash.
        qnorm = normalize(qa, tasks, out, arm + '_query_result')
        b.save(out / (arm + '_query_result_scores.json'), b.score(specs, qnorm, evidence))
        b.save(out / (arm + '_scores.json'), scores[arm])
    b.save(out / 'answers.json', answers)
    b.save(out / 'scores.json', scores)
    responses = [(p.parent.name, json.loads(p.read_text(encoding='utf-8'))) for p in (out / 'calls').glob('*/response.json')]
    summary = {'mode': 'DEVELOPMENT_ONLY', 'cases': len(specs), 'arm_scores': {k: v['summary'] for k, v in scores.items()},
        'provider_response_count': len(responses), 'http_failures': sum(r['http_status'] != 200 for _, r in responses),
        'failed_batches': len(list((out / 'calls').glob('*/failure.json'))),
        'known_reported_tokens': sum((r.get('body', {}).get('usage') or {}).get('total_tokens', 0) for _, r in responses),
        'missing_usage_responses': sum(not r.get('body', {}).get('usage') for _, r in responses),
        'recorded_seconds': sum(r['seconds'] for _, r in responses),
        'no_new_method_advantage_claim': True}
    b.save(out / 'summary.json', summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    for name in ('config', 'data', 'graph-receipt', 'output'):
        parser.add_argument('--' + name, required=True)
    run(parser.parse_args())
