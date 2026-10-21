"""Sequential remaining predictions, started only after recorded later probe.

No generation, method tuning, infinite retry, or automatic recovery v3.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import os
from datetime import datetime,timezone


def run(a):
    root=Path(a.root).resolve();private=root.parent;projects=private.parent
    engine=Path(a.engine_root).resolve()
    wrapper=Path(__file__).parent
    if (root/'remaining_queue_started.json').exists():
        raise FileExistsError('Queue already dispatched; inspect status, never duplicate')
    for name in ('development_queue_status.json','recovery_queue_status.json','source_queue_status.json'):
        status=json.loads((root/name).read_text())['status']
        if any(x in status for x in ('RUNNING','WAITING')):
            raise RuntimeError('Another model queue may be active: '+name)
    # The source recovery validates the probe before its first actual dispatch.
    with (root/'remaining_queue_started.json').open('x',encoding='utf-8') as f:
        json.dump({'pid':os.getpid(),'started_utc':datetime.now(timezone.utc).isoformat(),'wrapper':str(wrapper),
            'engine_root':str(engine),'policy':'No repeated generation. Stop on first incomplete batch. Only source Redis v1 -> v2 recovery.'},f,indent=2)
    def state(value):
        value.update(pid=os.getpid(),updated_utc=datetime.now(timezone.utc).isoformat())
        p=root/'remaining_queue_status.tmp';p.write_text(json.dumps(value,indent=2),encoding='utf-8')
        p.replace(root/'remaining_queue_status.json')
    data={
        'redis':projects/'OSDB_CSTKG_Text2Cypher/data_real/d1_typed_projection_staging_v1_2/execution_csv',
        'duckdb':private/'duckdb_projection_publication_v1_20261021/execution_csv',
        'iotdb':root/'e8_iotdb_projection/execution_csv',
        'elasticsearch':root/'e8_elasticsearch_projection/execution_csv'}
    receipts={'redis':private/'automated_dev_pilot_v1_20261021/neo4j_runtime/import_receipt.json',
        'duckdb':private/'publication_duckdb_runtime_v1_20261021/import_receipt.json',
        'iotdb':root/'e8_iotdb_runtime_v1/import_receipt.json',
        'elasticsearch':root/'e8_elasticsearch_runtime_v1/import_receipt.json'}
    jobs=[('source','redis',root/'e7_source_tasks_v1/redis',root/'e7_source_redis_v2')]
    jobs += [('source',p,root/'e7_source_tasks_v1'/p,root/('e7_source_'+p+'_v1')) for p in ('duckdb','iotdb','elasticsearch')]
    for tag,folder in [('grounded','e7_grounded_tasks_v1'),('boundary','e7_blind_intake_v1/tasks')]:
        jobs += [(tag,p,root/folder/p,root/('e7_'+tag+'_'+p+'_v1')) for p in ('redis','duckdb','iotdb','elasticsearch')]
    jobs += [('known_transfer',p,root/'e8_known_goal_transfer_v1'/p,root/('e8_known_transfer_'+p+'_v1')) for p in ('iotdb','elasticsearch')]
    completed=[]
    for tag,project,tasks,output in jobs:
        if output.exists():
            state({'status':'STOPPED_EXISTING_OUTPUT','project':project,'stratum':tag,'completed':completed})
            return
        common=['--task-dir',str(tasks),'--data',str(data[project]),'--receipt',str(receipts[project]),
            '--config',a.config,'--output',str(output),'--budget-dir',str(root/'budget')]
        if tag=='source' and project=='redis':
            command=[sys.executable,str(wrapper/'run_source_transport_recovery.py'),
                '--engine-root',str(engine),'--source-run',str(root/'e7_source_redis_v1'),
                '--probe-terminal',a.probe_terminal,*common]
        elif project in ('iotdb','elasticsearch'):
            command=[sys.executable,str(wrapper/'run_selected_holdout.py'),'--engine-root',str(engine),'--project',project,*common]
        else:
            command=[sys.executable,str(engine/'publication_benchmark/run_information_comparison.py'),
                '--project',project,'--port',str({'redis':17475,'duckdb':17476}[project]),'--context','compact',*common]
        state({'status':'RUNNING_FROZEN_REMAINING','project':project,'stratum':tag,'output':str(output),'completed':completed})
        with (root/('remaining_'+tag+'_'+project+'_console.log')).open('x',encoding='utf-8') as log:
            code=subprocess.call(command,cwd=engine,stdout=log,stderr=subprocess.STDOUT)
        summary=json.loads((output/'summary.json').read_text()) if (output/'summary.json').exists() else {}
        if code or summary.get('status')!='COMPLETED':
            state({'status':'STOPPED_REMAINING_BATCH','project':project,'stratum':tag,'exit_code':code,
                'result_status':summary.get('status'),'completed':completed})
            return
        completed.append({'project':project,'stratum':tag,'output':str(output)})
    state({'status':'ALL_FROZEN_REMAINING_PREDICTIONS_COMPLETED','completed':completed})


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for key in ('root','engine-root','config','probe-terminal'):p.add_argument('--'+key,required=True)
    run(p.parse_args())
