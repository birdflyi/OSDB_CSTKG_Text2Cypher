"""Independent native-engine fixture worker; no benchmark or model prediction.

Runs only a newly created local CPU server, REST bound to loopback, Bolt/RPC off.
Never starts/stops existing services, rewrites existing jobs or reads TEST queries.
"""
from pathlib import Path
from datetime import datetime
import argparse,csv,hashlib,json,os,platform,resource,socket,subprocess,time,urllib.request,urllib.error

def main():
    parser=argparse.ArgumentParser();parser.add_argument('job');args=parser.parse_args()
    job=Path(args.job).resolve();root=job.parents[1]
    assert root==Path('/mnt/data/louzehua/ch7_t1_public_benchmark_20261023')
    assert platform.node()==json.loads((root/'execution_owner.json').read_text())['hostname']
    assert not (root/'.public-runtime.lock').exists()
    assert not (job/'receipt.json').exists(), 'Audit previous job instead of replaying it'
    spec=json.loads((job/'spec.json').read_text());auth=json.loads((job/'.client-private.json').read_text())
    runtime=root/'text2gql_native_v452_prepare_20261026'
    config=spec['server_config'];assert config['host']=='127.0.0.1' and config['bolt_port']==0 and not config['enable_rpc']
    port=config['port'];s=socket.socket();s.bind(('127.0.0.1',port));s.close()
    os.nice(10)
    # TuGraph uses large file mappings. Resident memory is separately supervised.
    # Source core/defs.h reserves 4 TiB per graph; default + fixture need >8 TiB.
    resource.setrlimit(resource.RLIMIT_AS,(16*1024**4,16*1024**4))
    resource.setrlimit(resource.RLIMIT_CPU,(180,180))
    env=dict(os.environ,LD_LIBRARY_PATH=str(runtime/'lib'),OMP_NUM_THREADS='2',OPENBLAS_NUM_THREADS='2')
    result={'kind':'NATIVE_INDEPENDENT_FIXTURES_NOT_BENCHMARK_EX','started':datetime.now().astimezone().isoformat(),
      'hostname':platform.node(),'worker_pid':os.getpid(),'phase':'BEGIN','model_calls':0,
      'benchmark_queries':0,'fixture_queries':0,'cases':[],'child_pids':[],
      'root_runtime_lock_present_at_start':False,'existing_services_changed':False,
      'address_space_limit_bytes':16*1024**4,'resident_limit_bytes':4*1024**3}
    server=None
    def save():
        (job/'receipt.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    def check_child(proc):
        status=Path('/proc')/str(proc.pid)/'status'
        if status.exists():
            line=next((x for x in status.read_text().splitlines() if x.startswith('VmRSS:')),None)
            if line and int(line.split()[1])>4*1024**2:raise MemoryError('Child RSS exceeds 4 GiB')
    def bounded_wait(proc,seconds):
        deadline=time.monotonic()+seconds
        try:
            while proc.poll() is None:
                check_child(proc)
                if time.monotonic()>deadline:raise TimeoutError('Own child exceeded phase timeout')
                time.sleep(.2)
        except (MemoryError,TimeoutError):
            if proc.poll() is None:
                proc.terminate()
                try:proc.wait(timeout=5)
                except subprocess.TimeoutExpired:proc.kill();proc.wait()
            raise
        return proc.returncode
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    def post(endpoint,body,token=None):
        headers={'Content-Type':'application/json; charset=utf-8'}
        if token:headers['Authorization']='Bearer '+token
        request=urllib.request.Request('http://127.0.0.1:'+str(port)+endpoint,
                                      data=json.dumps(body).encode('utf-8'),headers=headers)
        with opener.open(request,timeout=25) as response:
            raw=response.read(64*1024**2+1)
            if len(raw)>64*1024**2:raise ValueError('TRUNCATED_RESPONSE')
            return json.loads(raw),hashlib.sha256(raw).hexdigest()
    try:
        result['phase']='IMPORTING_FIXTURE';save()
        db_path=job/'db'
        importer=[str(runtime/'bin/lgraph_import'),'--online','false','-c',str(job/'import_config.json'),
          '--dir',str(job/'db'),'--graph','fixture_v1','--overwrite','false','--continue_on_error','false',
          '--parse_block_threads','2','--parse_file_threads','2','--generate_sst_threads','2',
          '--read_rocksdb_threads','2','-u',auth['user'],'-p',auth['password']]
        if spec.get('reuse_fixture_job'):
            assert spec['reuse_fixture_job']=='fixtures_v2'
            prior=job.parent/spec['reuse_fixture_job']
            previous=json.loads((prior/'receipt.json').read_text())
            assert previous['import_returncode']==0 and all(previous['child_pids_absent'].values())
            assert all(not Path('/proc/'+str(p)).exists() for p in previous['child_pids'])
            for filename in ['Person.csv','LINK.csv','import_config.json']:
                assert hashlib.sha256((prior/filename).read_bytes()).hexdigest()==spec['source_files'][filename]['sha256']
            db_path=prior/'db';rc=0
            result['reused_fixture_db']=str(db_path)
        else:
            with (job/'import.log').open('xb') as log:
                proc=subprocess.Popen(importer,cwd=job,env=env,stdout=log,stderr=subprocess.STDOUT)
                result['child_pids'].append(proc.pid);save()
                rc=bounded_wait(proc,90)
        result['import_returncode']=rc
        if rc:raise RuntimeError('Importer failed; inspect retained private log')
        result['phase']='STARTING_OWN_LOOPBACK_SERVER';save()
        config['directory']=str(db_path);config['log_dir']=str(job/'logs')
        (job/'server.json').write_text(json.dumps(config))
        with (job/'server.log').open('xb') as log:
            server=subprocess.Popen([str(runtime/'bin/lgraph_server'),'--config',str(job/'server.json'),'-d','run'],
                cwd=job,env=env,stdout=log,stderr=subprocess.STDOUT)
        result['server_pid']=server.pid;result['child_pids'].append(server.pid);save()
        deadline=time.monotonic()+30
        while True:
            if server.poll() is not None:raise RuntimeError('Own server exited during startup')
            check_child(server)
            try:
                with socket.create_connection(('127.0.0.1',port),timeout=.3):break
            except OSError:
                if time.monotonic()>deadline:raise TimeoutError('Own server did not become ready')
                time.sleep(.2)
        # Verify the actual TCP bind, rather than trusting the config alone.
        bindings=[]
        for path in ['/proc/net/tcp','/proc/net/tcp6']:
            for line in Path(path).read_text().splitlines()[1:]:
                fields=line.split()
                if int(fields[1].split(':')[1],16)==port and fields[3]=='0A':bindings.append(fields[1])
        assert bindings==['0100007F:'+format(port,'04X')], 'Unexpected bind address'
        result['listen_bindings']=bindings
        login,_=post('/login',{'user':auth['user'],'password':auth['password']})
        token=login['jwt'];result['phase']='RUNNING_FIXED_FIXTURES';save()
        for case in spec['cases']:
            item={'id':case['id'],'query':case['query'],'expected_rows':case['expected_rows'],
                  'diagnostic_only':case.get('diagnostic_only',False)}
            result['fixture_queries']+=1
            try:
                body,digest=post('/cypher',{'graph':'fixture_v1','script':case['query'],'timeout':15},token)
                item.update({'status':'EXECUTED','response':body,'response_sha256':digest,
                             'matches_expected':None if case['expected_rows'] is None else body.get('result')==case['expected_rows']})
            except urllib.error.HTTPError as exc:
                raw=exc.read();item.update({'status':'QUERY_ERROR','http_status':exc.code,
                    'error':raw.decode(errors='replace').replace(auth['password'],'[REDACTED]'),
                    'response_sha256':hashlib.sha256(raw).hexdigest(),'matches_expected':False})
            except Exception as exc:
                item.update({'status':'TRANSPORT_OR_DECODE_ERROR','error_type':type(exc).__name__,
                             'matches_expected':False})
            result['cases'].append(item);save()
        result['matches_expected']=sum(x['matches_expected'] is True for x in result['cases'] if not x['diagnostic_only'])
        result['fixed_check_denominator']=sum(not x['diagnostic_only'] for x in result['cases'])
        result['phase']='FIXTURE_CHECKS_FINISHED'
    except Exception as exc:
        result['phase']='STOPPED';result['error_type']=type(exc).__name__
        result['error']=str(exc).replace(auth['password'],'[REDACTED]')[:300]
    finally:
        if server is not None and server.poll() is None:
            server.terminate()
            try:server.wait(timeout=10)
            except subprocess.TimeoutExpired:server.kill();server.wait()
        result['own_server_exitcode']=None if server is None else server.returncode
        result['child_pids_absent']={str(p):not Path('/proc/'+str(p)).exists() for p in result['child_pids']}
        result['finished']=datetime.now().astimezone().isoformat();save()
        print(json.dumps({'phase':result['phase'],'fixture_queries':result['fixture_queries'],
                          'matches_expected':result.get('matches_expected'),'error_type':result.get('error_type')}),flush=True)

if __name__=='__main__':main()
