"""Invoke the frozen comparison engine on a preselected owned loopback DB.

The original development CLI exposed only ports17475/17476. Calling its library
entry point avoids changing frozen prediction logic for ports17477/17478.
"""
import argparse
import json
from pathlib import Path
import sys
from types import SimpleNamespace


def run(a):
    engine=Path(a.engine_root).resolve()
    sys.path.insert(0,str(engine/'publication_benchmark'))
    import run_baselines as b
    import run_information_comparison as comparison
    manifest=json.loads(Path(a.task_dir).joinpath('manifest.json').read_text(encoding='utf-8'))
    project=a.project
    if manifest['project']!=project:
        raise ValueError('Wrong project task bindings')
    port={'iotdb':17477,'elasticsearch':17478}[project]
    b.DB_URL=f'http://127.0.0.1:{port}/db/neo4j/tx/commit'
    output=Path(a.output)
    if output.exists():raise FileExistsError('Use a separate explicit version, never silently rerun holdout')
    args=SimpleNamespace(task_dir=a.task_dir,project=project,data=a.data,receipt=a.receipt,
        config=a.config,output=a.output,budget_dir=a.budget_dir,port=port,context='compact')
    comparison.run(args)


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--project',choices=('iotdb','elasticsearch'),required=True)
    for key in ('engine-root','task-dir','data','receipt','config','output','budget-dir'):
        parser.add_argument('--'+key,required=True)
    run(parser.parse_args())
