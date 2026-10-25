"""Download only non-credential graph audit receipts from authorized host."""
from pathlib import Path
import argparse,hashlib,io,json,subprocess,tarfile

parser=argparse.ArgumentParser();parser.add_argument('--index',type=int,required=True)
parser.add_argument('--job-version',default='v1',choices=['v1','v2','v3']);args=parser.parse_args()
suffix='' if args.job_version=='v1' else '_'+args.job_version
local=Path('D:/ch7/reports/text2gql_native_reference_graph_'+str(args.index).zfill(2)+suffix+'_20261026')
assert local.is_dir()
remote='/mnt/data/louzehua/ch7_t1_public_benchmark_20261023/text2gql_reference_audit_20261026/graph_'+str(args.index).zfill(2)+'_native_'+args.job_version
archive=local/'evidence.tar.gz';assert not archive.exists(),'Verify existing evidence before fetching again'
run=subprocess.run(['ssh','-i','C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key','-p','2491',
 '-o','BatchMode=yes','-o','ConnectTimeout=15','root@117.50.195.94',
 'tar -C '+remote+' -czf - receipt.json spec.json reference_records responses worker.py server.json'],
 capture_output=True,timeout=90)
assert run.returncode==0,run.stderr[:300]
archive.write_bytes(run.stdout);dest=local/'evidence';dest.mkdir()
files=[]
with tarfile.open(fileobj=io.BytesIO(run.stdout),mode='r:gz') as tf:
    total=0
    for item in tf:
        assert not Path(item.name).is_absolute() and '..' not in Path(item.name).parts
        assert item.name.split('/')[0] in ['receipt.json','spec.json','reference_records','responses','worker.py','server.json']
        if item.isdir():continue
        assert item.isfile() and item.size<=64*1024**2
        total+=item.size;assert total<=512*1024**2
        target=dest/item.name;target.parent.mkdir(parents=True,exist_ok=True)
        raw=tf.extractfile(item).read();assert len(raw)==item.size
        target.write_bytes(raw);files.append({'path':item.name,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()})
(local/'evidence_manifest.json').write_text(json.dumps({'remote':remote,'archive_sha256':hashlib.sha256(run.stdout).hexdigest(),'files':files},indent=2)+'\n',encoding='utf-8')
print(json.dumps({'index':args.index,'version':args.job_version,'files':len(files),'archive_bytes':len(run.stdout)}))
