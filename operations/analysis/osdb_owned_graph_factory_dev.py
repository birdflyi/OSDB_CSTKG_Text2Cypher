"""One owned local graph copy at a time, with pinned cold bytes and deadlines.

Development only. No model, import, source-store startup, foreign process stop,
lock recovery or automatic database statement replay. Windows Neo4j runtime.
"""
from contextlib import contextmanager
from pathlib import Path
import json, os, shutil, socket, subprocess, threading, time, uuid
import requests
from neo4j import GraphDatabase
from osdb_scoped_attempts_dev import read, sha, immutable, wire
from osdb_model_read_executor_dev_v2 import ModelReadExecutor
from accept_osdb_actor_graph_references_20261026 import stores, store_directories, stop_owned, live

VERSION = 'osdb-owned-graph-factory-dev-2'


class ClientDeadlineExceeded(RuntimeError): pass


def bounded_database_call(function, abort_owned, seconds):
    """A deadline on this known driver's I/O, enforced by stopping its own JVM.

    Not an arbitrary Python callback sandbox. A timeout invalidates the graph;
    it is infrastructure evidence, never a license to replay a statement.
    """
    expired = threading.Event(); abort_errors = []
    def abort():
        expired.set()
        try: abort_owned()
        except BaseException as exc: abort_errors.append(type(exc).__name__)
    timer = threading.Timer(seconds, abort); timer.daemon = True; timer.start()
    try:
        try: result = function()
        except BaseException as exc:
            if expired.is_set(): raise ClientDeadlineExceeded('Owned database client deadline expired') from exc
            raise
        if expired.is_set(): raise ClientDeadlineExceeded('Owned database client deadline expired')
        return result
    finally:
        timer.cancel(); timer.join()
        if abort_errors: raise RuntimeError('Owned deadline cleanup failed; preserve graph lock: '+','.join(abort_errors))


class FactoryExecutor(ModelReadExecutor):
    def _statement(self, query, params, kind, consumer=None, timeout=60):
        # Full physical fingerprint is offline admission, not a method query.
        if kind in ('admit_nodes', 'admit_edges'):
            if timeout != 240: raise RuntimeError('Unexpected inherited offline scan budget')
            timeout = 600
        return super()._statement(query, params, kind, consumer, timeout=timeout)


class OwnedGraphFactory:
    def __init__(self, directory, inventory, inventory_sha256, profiles, home, lock_path,
                 *, http=18531, bolt=18532):
        self.directory = Path(directory).resolve(); self.inventory = Path(inventory)
        if sha(self.inventory) != inventory_sha256: raise RuntimeError('Cold inventory changed')
        self.pins = read(self.inventory)['cold_receipts']; self.profiles = profiles
        self.home = Path(home).resolve(); self.lock_path = Path(lock_path)
        if set(self.pins) != {'redis','duckdb','iotdb','elasticsearch'}: raise ValueError('Four graph pins required')
        if type(http) is not int or type(bolt) is not int or http == bolt: raise ValueError('Distinct ports required')
        self.http, self.bolt = http, bolt
        self.active = False; self.busy = False; self.unknown = False; self.used = set()

    def __enter__(self):
        if live(self.http) or live(self.bolt): raise RuntimeError('Selected local port occupied; never stop it')
        self.lock_wire = wire({'pid':os.getpid(),'hostname':socket.gethostname(),
            'directory':str(self.directory),'lease':uuid.uuid4().hex,'kind':VERSION})
        descriptor = os.open(self.lock_path, os.O_CREAT|os.O_EXCL|os.O_WRONLY, 0o600)
        with os.fdopen(descriptor,'wb') as f: f.write(self.lock_wire); f.flush(); os.fsync(f.fileno())
        try:
            self.directory.mkdir(parents=True,exist_ok=False)
            immutable(self.directory/'factory_owner.json',json.loads(self.lock_wire))
            self.active = True; return self
        except BaseException:
            if self.lock_path.read_bytes() == self.lock_wire: self.lock_path.unlink()
            raise

    def __exit__(self,*unused):
        self.active = False
        if self.busy or self.unknown: raise RuntimeError('Graph ownership or shutdown unresolved; lock retained')
        if self.lock_path.read_bytes() != self.lock_wire: raise RuntimeError('Changed graph factory lock retained')
        self.lock_path.unlink()

    @contextmanager
    def __call__(self, graph):
        if not self.active or self.busy or self.unknown or graph not in self.pins or graph in self.used:
            raise RuntimeError('Inactive/busy/unknown/repeated graph; no automatic graph recovery')
        if self.lock_path.read_bytes() != self.lock_wire: raise RuntimeError('Factory ownership changed')
        if live(self.http) or live(self.bolt): raise RuntimeError('Port occupied before boot')
        self.busy = True; self.used.add(graph)
        folder = self.directory/graph; folder.mkdir()
        pin = self.pins[graph]; source = Path(pin['root']).resolve()
        profile = self.profiles[graph]; profile_path = Path(profile['path'])
        if sha(profile_path) != profile['sha256']: raise RuntimeError('Pinned profile changed')
        schema = read(profile_path)
        cold = source/'new_store_manifest_after_shutdown.json'
        process = driver = session = stdout = stderr = None
        stopped = None; stop_mutex = threading.Lock(); admitted = None; source_verified = False
        def stop():
            nonlocal stopped
            with stop_mutex:
                if stopped is None and process is not None:
                    stopped = stop_owned(process,folder/'conf',self.http)
                    stopped['bolt_port_closed'] = not live(self.bolt)
                    if not stopped['bolt_port_closed']: raise RuntimeError('Owned Bolt port did not close')
                return stopped
        try:
            if sha(cold) != pin['cold_manifest_sha256']: raise RuntimeError('Cold manifest changed')
            expected_bytes = read(cold)
            if stores(source) != expected_bytes: raise RuntimeError('Actual cold source bytes differ')
            source_verified = True
            immutable(folder/'source_bytes_before.json',expected_bytes)
            if shutil.disk_usage(folder.anchor).free < sum(v['bytes'] for v in expected_bytes.values())+2*1024**3:
                raise RuntimeError('Insufficient space for a new isolated copy')
            for name in store_directories(source): shutil.copytree(source/name,folder/name)
            if stores(folder) != expected_bytes: raise RuntimeError('Copied bytes differ; do not boot')
            for name in ('conf','logs','run','plugins'): (folder/name).mkdir()
            config = ['server.default_listen_address=127.0.0.1',
                f'server.http.listen_address=127.0.0.1:{self.http}', 'server.http.enabled=true',
                'server.https.enabled=false','server.bolt.enabled=true',
                f'server.bolt.listen_address=127.0.0.1:{self.bolt}',
                f'server.bolt.advertised_address=127.0.0.1:{self.bolt}',
                'dbms.security.auth_enabled=false','initial.dbms.default_database=neo4j',
                'server.memory.heap.initial_size=256m','server.memory.heap.max_size=512m',
                'server.memory.pagecache.size=128m','server.jvm.additional=-XX:ActiveProcessorCount=2',
                'dbms.security.allow_csv_import_from_file_urls=false']
            for name in ('data','logs','run','plugins'):
                config.append('server.directories.'+name+'='+str(folder/name).replace('\\','/'))
            if 'transactions' in store_directories(source):
                config.append('server.directories.transaction.logs.root='+str(folder/'transactions').replace('\\','/'))
            (folder/'conf/neo4j.conf').write_text('\n'.join(config)+'\n',encoding='utf-8')
            launch = folder/'launch_owned.ps1'
            launch.write_text("& '"+str(self.home/'bin/neo4j.bat')+"' console\n",encoding='utf-8')
            stdout=(folder/'console_stdout.log').open('wb'); stderr=(folder/'console_stderr.log').open('wb')
            process=subprocess.Popen(['powershell.exe','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(launch)],
                env=dict(os.environ,NEO4J_CONF=str(folder/'conf'),NEO4J_HOME=str(self.home)),
                stdout=stdout,stderr=stderr,creationflags=subprocess.CREATE_NO_WINDOW)
            immutable(folder/'execution_owner.json',{'hostname':socket.gethostname(),'runner_pid':os.getpid(),
                'wrapper_pid':process.pid,'conf':str(folder/'conf'),'graph':graph,'source':str(source)})
            session=requests.Session(); session.trust_env=False; ready=False
            for index in range(3):
                time.sleep(12 if index == 0 else 10); receipt={'attempt':index+1}
                try:
                    response=session.get(f'http://127.0.0.1:{self.http}/',timeout=(2,3))
                    receipt['http_status']=response.status_code; ready=response.status_code==200
                except requests.RequestException as exc: receipt['error_type']=type(exc).__name__
                immutable(folder/f'readiness_{index+1}.json',receipt)
                if ready or process.poll() is not None: break
            if not ready: raise RuntimeError('Bounded readiness failed; no restart')
            driver=GraphDatabase.driver(f'bolt://127.0.0.1:{self.bolt}',auth=None,
                connection_timeout=3,connection_acquisition_timeout=5,max_transaction_retry_time=0,
                max_connection_pool_size=1)
            executor=FactoryExecutor(driver,schema,folder/'reads')
            actual=bounded_database_call(executor.admit,stop,1240)
            if any(actual[k] != pin[k] for k in ('nodes','edges','semantic_snapshot_sha256')):
                raise RuntimeError('Current physical graph fingerprint differs')
            admitted={'graph':graph,'admitted':True,'model_views_equal':True,
                'current_snapshot_sha256':actual['semantic_snapshot_sha256'],
                'client_and_server_deadlines_accepted':True,'factory_version':VERSION,
                'proof':actual,'cold_bytes_sha256':pin['cold_manifest_sha256'],
                'source_store_never_started':True,'same_executor_for_both_methods':True,
                'offline_scan_seconds':600,'explain_seconds':30,'method_read_seconds':60,
                'client_answer_call_seconds':100,'client_admission_call_seconds':1240,
                'timeout_action':'Stop only owned JVM; invalidate graph; no statement replay',
                'formal_admission':False}
            immutable(folder/'admission.json',admitted)
            def execute(query,params):
                if stopped is not None: raise RuntimeError('Owned graph already stopped; no further reads')
                return bounded_database_call(lambda:executor.execute(query,params),stop,100)
            yield {'admission':admitted,'schema':schema,'execute':execute,
                'physical_statement_count':lambda:executor.number,'abort_owned':stop}
        except BaseException as exc:
            immutable(folder/'failure.json',{'error_type':type(exc).__name__,'message':str(exc)[:1200],
                'no_model_or_query_replay':True})
            raise
        finally:
            try:
                # Stop on ownership proof before driver teardown can block on I/O.
                receipt=stop()
                if receipt is not None: immutable(folder/'shutdown_receipt.json',receipt)
                if driver: driver.close()
                if session: session.close()
                if stdout: stdout.close()
                if stderr: stderr.close()
                if source_verified:
                    if stores(source) != expected_bytes: raise RuntimeError('Cold source changed during use')
                    immutable(folder/'source_bytes_after.json',expected_bytes)
            except BaseException:
                self.unknown=True; raise
            finally:
                self.busy=False
