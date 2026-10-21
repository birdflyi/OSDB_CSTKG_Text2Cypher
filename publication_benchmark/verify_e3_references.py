"""Verify separately written E3 Cypher against CSV-set references."""
import argparse
import json
from pathlib import Path

import run_baselines as b
import run_strong_baselines as r
import strong_baselines as s


def verify(a):
    root=Path(a.task_dir)
    manifest=json.loads((root/'manifest.json').read_text(encoding='utf-8'))
    receipt=json.loads(Path(a.receipt).read_text(encoding='utf-8'))
    specs=json.loads((root/'specifications.json').read_text(encoding='utf-8'))
    if manifest['input_hashes']!=receipt['source_hashes']:
        raise ValueError('Reference source does not match import')
    if b.db('CALL db.info() YIELD id,name RETURN id,name')['data']!=receipt['database_identity']:
        raise ValueError('Wrong dedicated database')
    records=[]
    for spec in specs:
        prediction={'status':'ANSWER','cypher':spec['reference_query'],'parameters':spec['reference_parameters']}
        observation=s.evaluate(prediction,'G')
        records.append({'id':spec['id'],'correct':r.score(observation,spec),'observation':observation})
    result={'mode':'E3_REFERENCE_CONFORMANCE_NOT_MODEL_ACCURACY','correct':sum(x['correct'] for x in records),
            'cases':len(specs),'specifications_sha256':b.sha(root/'specifications.json'),
            'builder_sha256':manifest['builder_sha256'],'database_identity':receipt['database_identity'],'records':records}
    b.save(Path(a.output),result)
    print(json.dumps({k:result[k] for k in ('correct','cases')},indent=2))
    if result['correct']!=len(specs):
        raise ValueError('Reference mismatch; do not run models')


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for k in ('task-dir','receipt','output'):
        p.add_argument('--'+k,required=True)
    p.add_argument('--port',type=int,choices=(17475,17476),required=True)
    a=p.parse_args()
    b.DB_URL=f'http://127.0.0.1:{a.port}/db/neo4j/tx/commit'
    verify(a)
