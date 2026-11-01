import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import s5_batch as batch


def test_batch_isolates_errors_and_reuses_success(tmp_path):
    script=tmp_path/'stage.py'
    script.write_text('import json,sys\nfrom pathlib import Path\nPath(sys.argv[1]).write_text(json.dumps(dict(status="EXECUTED",complete=True,columns=["id"],rows=[["x"]])))\n')
    pin=dict(path=str(script),sha256=batch.sha(script))
    stage=dict(argv=[sys.executable,str(script),'{out}'],inputs=[pin])
    job=dict(task_id='ready',graph='g',layer='event',snapshot_sha256='snapshot',source=pin,
             graph_evidence=[pin],reference=stage,graph_query=stage,
             output_contract=dict(kind='list',columns=['id'],types=['string'],order='ordered',duplicates='forbid'))
    bad=dict(job,task_id='error',graph_query=dict(argv=[sys.executable,'-c','raise ValueError()'],inputs=[]))
    manifest=tmp_path/'jobs.jsonl'
    manifest.write_text('\n'.join(json.dumps(j) for j in [bad,job,dict(task_id='blocked',blocked_reasons=['missing query'])]))
    out=tmp_path/'run'
    result=batch.run(manifest,out)
    assert result['counts']=={'ERROR':1,'PASS':1,'BLOCKED':1}
    before=list(out.glob('*/attempt-*/result.json'))
    assert batch.run(manifest,out)['counts']==result['counts']
    assert list(out.glob('*/attempt-*/result.json'))==before
    manifest.write_text(manifest.read_text()+'\n'+json.dumps(dict(task_id='new',blocked_reasons=['missing'])))
    with pytest.raises(ValueError,match='Manifest changed'):
        batch.run(manifest,out)


def test_foreign_lock_is_not_removed(tmp_path):
    (tmp_path/'RUNNING.lock').write_text('other')
    with pytest.raises(FileExistsError):
        batch.run('missing',tmp_path)
    assert (tmp_path/'RUNNING.lock').read_text()=='other'
