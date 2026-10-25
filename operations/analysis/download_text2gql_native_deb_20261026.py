"""Download one pinned official Debian artifact; inspect archive without install."""
from pathlib import Path
from datetime import datetime
import hashlib,json,time,requests,tarfile,io

BASE=Path('D:/ch7/reports/text2gql_native_engine_20261026')
release=json.loads((BASE/'release_latest.json').read_text(encoding='utf-8'))
asset=next(a for a in release['assets'] if a['name']=='tugraph-4.5.2-1.x86_64.deb')
target=BASE/asset['name']
assert release['tag_name']=='v4.5.2'
record={'kind':'OFFICIAL_ENGINE_ARTIFACT_PREPARATION_NOT_INSTALL','time_local':datetime.now().astimezone().isoformat(),
        'release_tag':release['tag_name'],'release_target':release['target_commitish'],
        'asset_id':asset['id'],'url':asset['browser_download_url'],'expected_bytes':asset['size'],
        'upstream_digest':asset.get('digest'),'remote_write':False,'installed':False,'new_model_calls':0}
try:
    if not target.exists():
        partial=target.with_suffix('.deb.partial')
        assert not partial.exists(),'Inspect partial artifact before any retry'
        started=time.monotonic()
        with requests.get(asset['browser_download_url'],stream=True,timeout=(20,40)) as r:
            record['http_status']=r.status_code
            record['final_host']=requests.utils.urlparse(r.url).hostname
            r.raise_for_status()
            received=0
            with partial.open('xb') as out:
                for chunk in r.iter_content(1024*1024):
                    if time.monotonic()-started>600: raise TimeoutError('Bounded download deadline')
                    out.write(chunk);received+=len(chunk)
                    if received>asset['size']: raise ValueError('Artifact exceeds published size')
        assert partial.stat().st_size==asset['size']
        partial.rename(target)
    assert target.stat().st_size==asset['size']
    h=hashlib.sha256()
    with target.open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk)
    record['sha256']=h.hexdigest();record['bytes']=target.stat().st_size
    members=[]
    with target.open('rb') as f:
        assert f.read(8)==b'!<arch>\n'
        while True:
            header=f.read(60)
            if not header:break
            assert header[-2:]==b'`\n'
            name=header[:16].decode().strip().rstrip('/')
            size=int(header[48:58]);offset=f.tell()
            members.append({'name':name,'offset':offset,'bytes':size})
            f.seek(offset+size+(size%2))
    record['ar_members']=members
    # Inspect Debian metadata first; never execute package maintainer scripts.
    ctrl=next(m for m in members if m['name'].startswith('control.tar'))
    with target.open('rb') as f:
        f.seek(ctrl['offset']);content=f.read(ctrl['bytes'])
    with tarfile.open(fileobj=io.BytesIO(content),mode='r:*') as tf:
        record['control_members']=[{'name':m.name,'bytes':m.size} for m in tf]
        control=tf.extractfile('./control') or tf.extractfile('control')
        record['package_control']=control.read().decode('utf-8')
    record['status']='ARTIFACT_DOWNLOADED_CONTROL_INSPECTED'
except Exception as exc:
    record['status']='PREPARATION_FAILED';record['error_type']=type(exc).__name__
    record['error']=str(exc).split('?')[0][:240]
finally:
    (BASE/'artifact_receipt.json').write_text(json.dumps(record,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps(record,ensure_ascii=True))
