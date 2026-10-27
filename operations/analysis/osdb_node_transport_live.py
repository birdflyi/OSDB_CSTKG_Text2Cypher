"""Pinned one-shot SSH transport; no probe, retry or process manager."""
import ast, base64, hashlib, json, subprocess
from pathlib import Path
from osdb_scoped_attempts_dev import wire, immutable


class UnknownRemoteAttempt(BaseException): pass


def node_worker_source():
    path=Path(__file__).with_name('osdb_budgeted_model_client_dev.py')
    source=path.read_text(encoding='utf-8-sig')
    node=next(n for n in ast.parse(source).body if isinstance(n,ast.ClassDef) and n.name=='OneShotHTTP')
    transport=ast.get_source_segment(source,node)
    return '''import pathlib,sys,json,socket,time,http.client,threading,base64,os,re,hashlib
from urllib.parse import urlsplit
def wire(v):return (json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)+'\\n').encode('utf-8')
def save(p,v):
 with p.open('xb') as f:f.write(wire(v));f.flush();os.fsync(f.fileno())
'''+transport+'''
def main():
 root=pathlib.Path(__file__).resolve().parent
 owner=json.loads((root/'execution_owner.json').read_text())
 assert owner['hostname']==socket.gethostname()
 assert owner['purpose']=='T1 OSDB exposed development, single-shot only'
 assert hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()==owner['worker_sha256']
 raw=sys.stdin.buffer.read(1024*1024+1);assert len(raw)<=1024*1024
 envelope=json.loads(raw);rid=envelope['request_id'];body=envelope['body']
 assert re.fullmatch(r'[A-Za-z0-9_-]+__(?:direct|ours)__(?:initial|feedback)',rid)
 assert rid in owner['allowed_request_ids']
 assert all(body.get(k)==v for k,v in owner['sampling'].items())
 assert set(body)==set(owner['sampling'])|{'messages'}
 folder=root/'requests'/rid;folder.mkdir(parents=True,exist_ok=False)
 save(folder/'request.json',body)
 os.close(os.open(str(folder/'dispatch.claim'),os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600))
 private_path=pathlib.Path(owner['private_configuration'])
 assert hashlib.sha256(private_path.read_bytes()).hexdigest()==owner['private_configuration_sha256']
 private=json.loads(private_path.read_text())
 assert private['model']==body['model']
 assert private['endpoint']=='http://127.0.0.1:18894/gpt-oss-120b/litellm/v1/chat/completions'
 result=OneShotHTTP(private)(body)
 raw=result.pop('raw_response_bytes',None)
 if raw is not None:
  with (folder/'response.raw').open('xb') as f:f.write(raw)
 result.update(request_id=rid,hostname=socket.gethostname(),automatic_retry=False)
 save(folder/'terminal.json',result)
 if raw is not None:result['raw_response_base64']=base64.b64encode(raw).decode('ascii')
 print(json.dumps(result,ensure_ascii=True,allow_nan=False),flush=True)
if __name__=='__main__':main()
'''


class NodeTransport:
    def __init__(self,ssh,remote_root,initial,evidence,run=subprocess.run):
        self.ssh=list(ssh);self.remote_root=remote_root;self.evidence=Path(evidence);self.run=run
        self.identities={hashlib.sha256(wire(messages)).hexdigest():key for key,messages in initial.items()}
        if len(self.identities)!=48:raise ValueError('48 distinct sealed method inputs required')
    def __call__(self,body):
        messages=body['messages'];key=self.identities[hashlib.sha256(wire(messages[:2])).hexdigest()]
        if len(messages) not in (2,4):raise ValueError('Unadmitted message phase')
        rid=key+'__'+('initial' if len(messages)==2 else 'feedback')
        folder=self.evidence/rid;folder.mkdir(parents=True,exist_ok=False)
        envelope={'request_id':rid,'body':body}
        try:
            # Paths are fixed by owned deployment, never model-controlled shell text.
            result=self.run(self.ssh+['python3 '+self.remote_root+'/node_model_once.py'],
                input=wire(envelope),capture_output=True,timeout=275)
        except subprocess.TimeoutExpired:
            immutable(folder/'ssh.json',{'request_id':rid,'outcome_unknown':True,'reason':'SSH timeout','no_retry':True})
            raise UnknownRemoteAttempt(rid)
        receipt={'request_id':rid,'returncode':result.returncode,'stderr_utf8':result.stderr.decode('utf-8',errors='replace')[:1500]}
        immutable(folder/'ssh.json',receipt)
        if result.returncode!=0:raise UnknownRemoteAttempt(rid)
        try:terminal=json.loads(result.stdout)
        except (ValueError,UnicodeError):raise UnknownRemoteAttempt(rid)
        if terminal.get('request_id')!=rid:raise UnknownRemoteAttempt(rid)
        raw=terminal.pop('raw_response_base64',None)
        if raw is not None:terminal['raw_response_bytes']=base64.b64decode(raw,validate=True)
        if terminal.get('remote_outcome_unknown'):
            copy=dict(terminal);raw=copy.pop('raw_response_bytes',None)
            immutable(folder/'remote_unknown_terminal.json',copy)
            if raw is not None:(folder/'response.raw').write_bytes(raw)
            raise UnknownRemoteAttempt(rid)
        return terminal
