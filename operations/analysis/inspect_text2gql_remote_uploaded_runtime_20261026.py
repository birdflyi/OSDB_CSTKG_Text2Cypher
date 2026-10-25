"""Continue exact stopped CPU preparation from verified local uploads.

Keep original failed network receipt. Run no TuGraph server/importer or queries.
"""
from pathlib import Path
import json,subprocess

BASE=Path('D:/ch7/reports/text2gql_native_engine_20261026')
upload=json.loads((BASE/'local_artifact_upload.json').read_text(encoding='utf-8'))
assert len(upload['files'])==2 and all(f['returncode']==0 for f in upload['files'])
src=(BASE/'remote_preparation_script.py').read_text(encoding='utf-8')
old="""assert prefix.parent==root and not prefix.exists(), 'Audit existing preparation before continuing'
prefix.mkdir(mode=0o700)"""
new="""assert prefix.parent==root and prefix.is_dir()
old_receipt=json.loads((prefix/'preparation_receipt.json').read_text())
assert old_receipt['phase']=='PREPARATION_STOPPED' and old_receipt['files']==[]
assert not Path('/proc/'+str(old_receipt['pid'])).exists()
assert not (prefix/'local_transfer_preparation_receipt.json').exists()
assert not (prefix/'bin').exists() and not (prefix/'lib').exists()"""
assert old in src;src=src.replace(old,new,1)
src=src.replace("(prefix/'preparation_receipt.json').write_text", "(prefix/'local_transfer_preparation_receipt.json').write_text",1)
old="""    record['phase']='DOWNLOADING_PINNED_ENGINE';save()
    deb=download('tugraph-4.5.2-1.x86_64.deb',spec['engine'])
    lib=download('libgfortran4.deb',spec['library'])"""
new="""    record['phase']='VERIFYING_LOCAL_UPLOADS';save()
    deb=prefix/'tugraph-4.5.2-1.x86_64.deb'
    lib=prefix/'libgfortran4.deb'
    for path,metadata in [(deb,spec['engine']),(lib,spec['library'])]:
        assert path.stat().st_size==metadata['bytes']
        h=hashlib.sha256()
        with path.open('rb') as handle:
            for block in iter(lambda:handle.read(1024*1024),b''):h.update(block)
        assert h.hexdigest()==metadata['sha256']
        record['files'].append({'path':path.name,'bytes':metadata['bytes'],'sha256':h.hexdigest()})"""
assert old in src;src=src.replace(old,new,1)
(BASE/'remote_uploaded_runtime_inspection_script.py').write_text(src,encoding='utf-8')
run=subprocess.run(['ssh','-i','C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key',
     '-p','2491','-o','BatchMode=yes','-o','ConnectTimeout=15','root@117.50.195.94','python3 -'],
    input=src,capture_output=True,text=True,timeout=180)
(BASE/'remote_uploaded_runtime_client_result.json').write_text(json.dumps({'returncode':run.returncode,
    'stdout':run.stdout,'stderr':run.stderr},indent=2)+'\n',encoding='utf-8')
print(json.dumps({'returncode':run.returncode,'stdout':run.stdout,'stderr':run.stderr},ensure_ascii=True))
