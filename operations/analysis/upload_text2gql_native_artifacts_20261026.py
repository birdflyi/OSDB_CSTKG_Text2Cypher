"""Copy previously verified public packages to one stopped private preparation."""
from pathlib import Path
from datetime import datetime
import hashlib,json,subprocess

BASE=Path('D:/ch7/reports/text2gql_native_engine_20261026')
spec=json.loads((BASE/'remote_preparation_spec.json').read_text(encoding='utf-8'))
prefix=spec['root']+'/'+spec['subdirectory']
key='C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key'
ssh=['ssh','-i',key,'-p','2491','-o','BatchMode=yes','-o','ConnectTimeout=15','root@117.50.195.94']
code='''import pathlib,platform,json,os
root=pathlib.Path(ROOT);prefix=root/SUB
assert platform.node()==json.loads((root/'execution_owner.json').read_text())['hostname']
assert not (root/'.public-runtime.lock').exists()
receipt=json.loads((prefix/'preparation_receipt.json').read_text())
assert receipt['phase']=='PREPARATION_STOPPED' and receipt['files']==[]
assert not pathlib.Path('/proc/'+str(receipt['pid'])).exists(), 'Old preparation pid alive/reused'
assert not (prefix/'tugraph-4.5.2-1.x86_64.deb').exists()
assert not (prefix/'libgfortran4.deb').exists()
print(json.dumps({'pass':True,'hostname':platform.node(),'old_preparation_pid_absent':True,'lock_absent':True}))
'''.replace('ROOT',repr(spec['root'])).replace('SUB',repr(spec['subdirectory']))
gate=subprocess.run(ssh+['python3 -'],input=code,text=True,capture_output=True,timeout=30)
assert gate.returncode==0, 'Archive/audit existing preparation before changing files'
record={'started_local':datetime.now().astimezone().isoformat(),'gate':json.loads(gate.stdout),
        'destination_prefix':prefix,'files':[],'new_model_calls':0,'server_started':False}
(BASE/'local_artifact_upload.json').write_text(json.dumps(record,indent=2)+'\n',encoding='utf-8')
for source,destination,metadata in [
    (BASE/'tugraph-4.5.2-1.x86_64.deb','tugraph-4.5.2-1.x86_64.deb',spec['engine']),
    (BASE/'libgfortran4_7.5.0-3ubuntu1~18.04_amd64.deb','libgfortran4.deb',spec['library'])]:
    assert source.stat().st_size==metadata['bytes']
    h=hashlib.sha256()
    with source.open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    assert h.hexdigest()==metadata['sha256']
    run=subprocess.run(['scp','-P','2491','-i',key,'-o','BatchMode=yes','-o','ConnectTimeout=15',
                        str(source),'root@117.50.195.94:'+prefix+'/'+destination],
                       capture_output=True,text=True,timeout=650)
    record['files'].append({'name':destination,'returncode':run.returncode,'expected_sha256':metadata['sha256'],
                            'expected_bytes':metadata['bytes'],'error':run.stderr[:500]})
    (BASE/'local_artifact_upload.json').write_text(json.dumps(record,indent=2)+'\n',encoding='utf-8')
    if run.returncode:break
record['finished_local']=datetime.now().astimezone().isoformat()
(BASE/'local_artifact_upload.json').write_text(json.dumps(record,indent=2)+'\n',encoding='utf-8')
print(json.dumps(record,ensure_ascii=True))
