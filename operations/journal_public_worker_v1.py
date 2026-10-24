"""Short SSH RPCs with stable durable ids around the ORIGINAL native worker."""
import hashlib
import json
from pathlib import Path
import shlex
import time
import uuid
import paramiko

ROOT = '/mnt/data/louzehua/ch7_t1_public_benchmark_20261023'
HOST = 'interactive-xuyl0q80gc01-6d6f5ddb9b-xtpfj'


class JournalPublicWorker:
    def connect(self):
        client = paramiko.SSHClient()
        client.load_host_keys(str(Path.home()/'.ssh/known_hosts'))
        client.set_missing_host_key_policy(paramiko.RejectPolicy())
        try:
            client.connect('117.50.195.94', port=2491, username='root', key_filename=str(self.key),
                           timeout=12, banner_timeout=20, auth_timeout=20,
                           allow_agent=False, look_for_keys=False)
            client.get_transport().set_keepalive(15)
            _, out, err = client.exec_command('hostname; cat '+ROOT+'/execution_owner.json', timeout=20)
            lines = out.read().decode('utf-8').splitlines()
            assert out.channel.recv_exit_status() == 0 and lines[0] == HOST
            assert json.loads(lines[1])['hostname'] == HOST
            return client
        except Exception:
            client.close()
            raise

    def __init__(self, key, graph, log, remote):
        self.key = key
        self.log = Path(log).open('x', encoding='utf-8')
        self.session = graph+'_'+uuid.uuid4().hex
        self.folder = ROOT+'/transport_journal_v1/'+self.session
        self.sequence = 0
        self.closed = False
        self.remote_code = ROOT+'/transport_journal_v1/durable_native_journal_v1.py'
        client = self.connect()
        try:
            with client.open_sftp() as sftp:
                # Never upload executable code while a worker is running. Deployment
                # happens once before admission. Verify exactly the local code hash.
                expected = hashlib.sha256(Path(__file__).with_name('durable_native_journal_v1.py').read_bytes()).hexdigest()
                with sftp.open(self.remote_code, 'rb') as f:
                    assert hashlib.sha256(f.read()).hexdigest() == expected
                sftp.mkdir(self.folder)
                with sftp.open(self.folder+'/spec.json', 'wx') as f:
                    f.write(json.dumps({'session': self.session, 'graph': graph, 'remote': remote}))
            command = shlex.join([ROOT+'/.venv/bin/python', self.remote_code, 'serve',
                                  '--session', self.session, '--graph', graph])
            _, out, err = client.exec_command('nohup '+command+' >'+self.folder+'/daemon.log 2>&1 </dev/null &', timeout=20)
            assert out.channel.recv_exit_status() == 0
            # Preserve identity before any query/model work. If startup is ambiguous,
            # do NOT create another session automatically.
            self.log.write(json.dumps({'session': self.session, 'folder': self.folder,
                                       'native_source_unchanged': True})+'\n');self.log.flush()
            deadline = time.monotonic()+300
            with client.open_sftp() as sftp:
                while time.monotonic() < deadline:
                    try:
                        with sftp.open(self.folder+'/ready.json','rb') as f:
                            self.ready=json.loads(f.read())
                        assert self.ready['kind']=='READY' and self.ready['graph']==graph
                        break
                    except FileNotFoundError:
                        try:
                            with sftp.open(self.folder+'/error.json','rb') as f: error=f.read()
                        except FileNotFoundError:
                            time.sleep(.25);continue
                        raise RuntimeError(error.decode('utf-8'))
                else: raise TimeoutError('Journal startup unknown; inspect session, no automatic restart')
        finally: client.close()

    def rpc(self, value, index):
        raw = json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf-8')
        digest = hashlib.sha256(raw).hexdigest()
        command = shlex.join([ROOT+'/.venv/bin/python', self.remote_code,'call',
                              '--session',self.session,'--index',str(index)])
        for attempt in range(3):
            client=None;started=time.monotonic()
            try:
                client=self.connect()
                stdin,out,err=client.exec_command(command,timeout=150)
                stdin.write(raw);stdin.flush();out.channel.shutdown_write()
                response=out.read();status=out.channel.recv_exit_status()
                if status:
                    diagnostic=err.read().decode('utf-8',errors='replace')[-1500:]
                    # Remote daemon/execution errors are not transport retry cases.
                    raise ValueError('Journal command failed: '+diagnostic)
                result=json.loads(response)
                assert result['session']==self.session and result['id']==index
                assert result['journal']['sha256']==digest and result['journal']['native_writes']==1
                self.log.write(json.dumps({'journal_id':index,'physical_rpc_attempt':attempt+1,
                    'status':'RECEIVED','seconds':time.monotonic()-started,'journal':result['journal']})+'\n')
                self.log.flush()
                return result['response']
            except (OSError,EOFError,paramiko.SSHException) as error:
                self.log.write(json.dumps({'journal_id':index,'physical_rpc_attempt':attempt+1,
                    'status':'TRANSPORT_ERROR','type':type(error).__name__,
                    'seconds':time.monotonic()-started,'reexecute_native':False})+'\n');self.log.flush()
                if attempt==2: raise
                time.sleep(10)
            finally:
                if client:client.close()

    def request(self,value):
        started=time.monotonic()
        index=self.sequence
        # Sequence advances only after received result; bounded RPC retry uses SAME id.
        result=self.rpc(value,index)
        self.sequence+=1
        result['worker_roundtrip_seconds']=time.monotonic()-started
        return result

    def execute(self,query,parameters):
        return self.request({'query':query,'parameters':parameters})

    def close(self):
        if self.closed:return
        self.closed=True
        client=None
        try:
            client=self.connect()
            with client.open_sftp() as sftp:
                with sftp.open(self.folder+'/stop.request','wx') as f:f.write('stop after current request\n')
                deadline=time.monotonic()+45
                while time.monotonic()<deadline:
                    try:
                        with sftp.open(self.folder+'/closed.json','rb') as f:receipt=json.loads(f.read())
                        self.log.write(json.dumps({'closed':receipt})+'\n');break
                    except FileNotFoundError:time.sleep(.25)
                else:self.log.write('{"close_unknown":true,"remote_audit_required":true}\n')
        except Exception as error:
            self.log.write(json.dumps({'close_unknown':True,'type':type(error).__name__})+'\n')
        finally:
            if client:client.close()
            self.log.close()
