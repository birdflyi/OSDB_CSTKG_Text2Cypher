"""Prepare a private CPU runtime and inspect dynamic dependencies; no server launch."""
from pathlib import Path
from datetime import datetime
import hashlib,json,subprocess

BASE=Path('D:/ch7/reports/text2gql_native_engine_20261026')
engine=json.loads((BASE/'artifact_receipt.json').read_text(encoding='utf-8'))
library=json.loads((BASE/'libgfortran4_receipt.json').read_text(encoding='utf-8'))
initial=json.loads((BASE/'archive_inspection_initial.json').read_text(encoding='utf-8'))
catalog=json.loads((BASE/'archive_inspection.json').read_text(encoding='utf-8'))
items=initial['selected_elf']+catalog['selected_elf']
assert {x['local_name'] for x in items}=={'lgraph_server','lgraph_import','liblgraph.so','libvsag.so'}
spec={'root':'/mnt/data/louzehua/ch7_t1_public_benchmark_20261023',
      'subdirectory':'text2gql_native_v452_prepare_20261026',
      'engine':{k:engine[k] for k in ['url','sha256','bytes']},
      'library':{k:library[k] for k in ['url','sha256','bytes']},'elf_files':items}
code=r'''
from pathlib import Path
from datetime import datetime
import os,platform,json,urllib.request,hashlib,time,tarfile,io,subprocess,resource
spec=SPEC
root=Path(spec['root']).resolve()
assert platform.node()==json.loads((root/'execution_owner.json').read_text())['hostname']
assert not (root/'.public-runtime.lock').exists(), 'Other benchmark lock present'
prefix=root/spec['subdirectory']
assert prefix.parent==root and not prefix.exists(), 'Audit existing preparation before continuing'
prefix.mkdir(mode=0o700)
resource.setrlimit(resource.RLIMIT_AS,(4*1024**3,4*1024**3))
os.nice(10)
record={'started_local':datetime.now().astimezone().isoformat(),'pid':os.getpid(),
        'hostname':platform.node(),'prefix':str(prefix),'phase':'BEGIN','server_started':False,
        'new_model_calls':0,'new_database_queries':0,'global_install':False,'files':[]}
def save():
    (prefix/'preparation_receipt.json').write_text(json.dumps(record,indent=2)+'\n')
    print(json.dumps({'phase':record['phase'],'pid':os.getpid()}),flush=True)
save()
def download(name,metadata):
    target=prefix/name
    started=time.monotonic();h=hashlib.sha256();size=0
    request=urllib.request.Request(metadata['url'],headers={'User-Agent':'ch7-native-reference-preparation'})
    with urllib.request.urlopen(request,timeout=30) as response,target.open('xb') as out:
        while True:
            if time.monotonic()-started>600:raise TimeoutError('Bounded artifact transfer deadline')
            chunk=response.read(1024*1024)
            if not chunk:break
            size+=len(chunk);assert size<=metadata['bytes'];out.write(chunk);h.update(chunk)
    assert size==metadata['bytes'] and h.hexdigest()==metadata['sha256']
    record['files'].append({'path':name,'bytes':size,'sha256':h.hexdigest()})
    return target
def ar_members(path):
    with path.open('rb') as f:
        assert f.read(8)==b'!<arch>\n'
        while True:
            header=f.read(60)
            if not header:break
            assert header[-2:]==b'`\n'
            size=int(header[48:58]);off=f.tell();name=header[:16].decode().strip().rstrip('/')
            yield name,off,size
            f.seek(off+size+size%2)
try:
    record['phase']='DOWNLOADING_PINNED_ENGINE';save()
    deb=download('tugraph-4.5.2-1.x86_64.deb',spec['engine'])
    lib=download('libgfortran4.deb',spec['library'])
    record['phase']='EXTRACTING_NAMED_FILES';save()
    (prefix/'bin').mkdir();(prefix/'lib').mkdir()
    wanted={x['archive_path']:x for x in spec['elf_files']}
    data=next(m for m in ar_members(deb) if m[0]=='data.tar.gz')
    with deb.open('rb') as f:
        f.seek(data[1])
        with tarfile.open(fileobj=f,mode='r|gz') as tf:
            for member in tf:
                if member.name not in wanted:continue
                expected=wanted[member.name]
                assert member.isfile() and member.size==expected['bytes']
                name=expected['local_name']
                dst=prefix/('bin' if name.startswith('lgraph_') else 'lib')/name
                h=hashlib.sha256();received=0
                with tf.extractfile(member) as inp,dst.open('xb') as out:
                    while True:
                        block=inp.read(1024*1024)
                        if not block:break
                        h.update(block);out.write(block);received+=len(block)
                assert received==expected['bytes'] and h.hexdigest()==expected['sha256']
                dst.chmod(0o700 if name.startswith('lgraph_') else 0o600)
                record['files'].append({'path':str(dst.relative_to(prefix)),
                                        'bytes':received,'sha256':h.hexdigest()})
    data=next(m for m in ar_members(lib) if m[0].startswith('data.tar'))
    with lib.open('rb') as f:
        f.seek(data[1]);payload=f.read(data[2])
    with tarfile.open(fileobj=io.BytesIO(payload),mode='r:*') as tf:
        member=next(m for m in tf if m.name=='./usr/lib/x86_64-linux-gnu/libgfortran.so.4.0.0')
        raw=tf.extractfile(member).read();assert len(raw)==1959960
        (prefix/'lib/libgfortran.so.4.0.0').write_bytes(raw)
        (prefix/'lib/libgfortran.so.4').symlink_to('libgfortran.so.4.0.0')
        record['files'].append({'path':'lib/libgfortran.so.4.0.0','bytes':len(raw),
                                'sha256':hashlib.sha256(raw).hexdigest()})
    assert len([x for x in record['files'] if x['path'].startswith(('bin/','lib/'))])==5
    record['phase']='DEPENDENCY_INSPECTION';save()
    env=dict(os.environ,LD_LIBRARY_PATH=str(prefix/'lib'))
    inspections=[]
    for name in ['lgraph_server','lgraph_import']:
        result=subprocess.run(['/lib64/ld-linux-x86-64.so.2','--list',str(prefix/'bin'/name)],
                              env=env,capture_output=True,text=True,timeout=30)
        inspections.append({'file':name,'returncode':result.returncode,
                            'stdout':result.stdout,'stderr':result.stderr})
    record['dynamic_dependency_inspections']=inspections
    record['phase']='DEPENDENCIES_RESOLVED_NO_SERVER_STARTED' if all(x['returncode']==0 for x in inspections) else 'DEPENDENCY_BLOCKED'
except Exception as exc:
    record['phase']='PREPARATION_STOPPED';record['error_type']=type(exc).__name__
    record['error']=str(exc)[:300]
finally:
    record['finished_local']=datetime.now().astimezone().isoformat();save()
'''.replace('SPEC',repr(spec),1)
(BASE/'remote_preparation_spec.json').write_text(json.dumps(spec,indent=2)+'\n',encoding='utf-8')
(BASE/'remote_preparation_script.py').write_text(code,encoding='utf-8')
run=subprocess.Popen(['ssh','-i','C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key',
       '-p','2491','-o','BatchMode=yes','-o','ConnectTimeout=15','root@117.50.195.94','python3 -'],
       stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
(BASE/'local_preparation_client.json').write_text(json.dumps({'time_local':datetime.now().astimezone().isoformat(),
    'ssh_pid':run.pid,'prefix':spec['root']+'/'+spec['subdirectory'],'purpose':'CPU engine preparation only; no model or database query'}),encoding='utf-8')
stdout,stderr=run.communicate(code,timeout=850)
(BASE/'remote_preparation_client_result.json').write_text(json.dumps({'returncode':run.returncode,'stdout':stdout,
    'stderr':stderr,'time_local':datetime.now().astimezone().isoformat()},indent=2)+'\n',encoding='utf-8')
print(json.dumps({'returncode':run.returncode,'stdout':stdout,'stderr':stderr},ensure_ascii=True))
