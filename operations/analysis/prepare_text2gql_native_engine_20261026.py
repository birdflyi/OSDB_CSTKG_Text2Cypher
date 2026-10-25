"""Bounded public release discovery and read-only authorized-host inspection."""
from pathlib import Path
from datetime import datetime
import hashlib
import json
import subprocess
import requests

BASE=Path('D:/ch7/reports/text2gql_native_engine_20261026')
BASE.mkdir(exist_ok=True)
session=requests.Session()
session.headers['User-Agent']='ch7-reproducibility-audit'
urls={
    'release_latest':'https://api.github.com/repos/TuGraph-family/tugraph-db/releases/latest',
    'release_list':'https://api.github.com/repos/TuGraph-family/tugraph-db/releases?per_page=8',
}
trace=[]
for name,url in urls.items():
    try:
        response=session.get(url,timeout=(15,40))
        raw=response.content
        (BASE/(name+'.json')).write_bytes(raw)
        trace.append({'name':name,'url':url,'final_url':response.url,'status':response.status_code,
                      'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()})
    except requests.RequestException as exc:
        trace.append({'name':name,'url':url,'error':type(exc).__name__})

remote_code=r'''
import json,os,platform,pathlib,shutil,subprocess
root=pathlib.Path('/mnt/data/louzehua/ch7_t1_public_benchmark_20261023')
owner=json.loads((root/'execution_owner.json').read_text())
assert platform.node()==owner['hostname'], 'Shared mount owner mismatch'
processes=[]
for path in pathlib.Path('/proc').iterdir():
    if not path.name.isdigit(): continue
    try:
        cmd=(path/'cmdline').read_bytes().replace(b'\0',b' ').decode(errors='replace')
        if str(root) in cmd and any(s in cmd for s in ['native_public','lgraph_server','lgraph_import','java','core_journal_resume']):
            processes.append({'pid':int(path.name),'executable':os.readlink(str(path/'exe'))})
    except (OSError,PermissionError): pass
try:
    listeners=subprocess.run(['ss','-ltn'],capture_output=True,text=True,timeout=10)
    ports=sorted(set(line.split()[3].rsplit(':',1)[-1] for line in listeners.stdout.splitlines()[1:] if len(line.split())>3))
except (OSError,subprocess.TimeoutExpired): ports=None
print(json.dumps({'hostname':platform.node(),'owner_hostname':owner['hostname'],
 'lock_present':(root/'.public-runtime.lock').exists(),'os_release':platform.freedesktop_os_release() if hasattr(platform,'freedesktop_os_release') else pathlib.Path('/etc/os-release').read_text(),
 'arch':platform.machine(),'libc':platform.libc_ver(),'cpu_count':os.cpu_count(),
 'memory_lines':[x for x in pathlib.Path('/proc/meminfo').read_text().splitlines() if x.startswith(('MemTotal:','MemAvailable:'))],
 'commands':{n:shutil.which(n) for n in ['docker','podman','lgraph_server','lgraph_import','quota','rpm','dpkg','tar','unzip','ldd','curl']},
 'root_children':[p.name for p in root.iterdir() if p.is_dir()],
 'relevant_processes':processes,'listening_ports':ports,'read_only':True}))
'''
try:
    run=subprocess.run(['ssh','-i','C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key',
        '-p','2491','-o','BatchMode=yes','-o','ConnectTimeout=15','root@117.50.195.94','python3 -'],
        input=remote_code,text=True,capture_output=True,timeout=45)
    remote={'returncode':run.returncode,'data':json.loads(run.stdout) if run.returncode==0 else None,
            'error_type':None if run.returncode==0 else 'SSH_OR_READONLY_INSPECTION_FAILED'}
except (subprocess.TimeoutExpired,json.JSONDecodeError) as exc:
    remote={'error_type':type(exc).__name__}
result={'time_local':datetime.now().astimezone().isoformat(),'public_fetches':trace,
        'remote':remote,'new_model_calls':0,'new_database_queries':0,
        'remote_files_written':0,'engine_installed':False,'author_release_matches_paper_version':False}
(BASE/'discovery.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
release=json.loads((BASE/'release_latest.json').read_text(encoding='utf-8')) if (BASE/'release_latest.json').exists() else {}
print(json.dumps({'time_local':result['time_local'],'release_tag':release.get('tag_name'),
    'published_at':release.get('published_at'),'assets':[{'name':a['name'],'bytes':a['size'],'url':a['browser_download_url']} for a in release.get('assets',[])],
    'remote':remote},ensure_ascii=True))
