"""Native CSV identity and reference audit on one isolated CPU graph.

No models, no modification of queries, no comparison with method predictions.
Retain every source task, including syntax errors and unissued tasks after a stop.
"""
from pathlib import Path
from datetime import datetime,date,timezone
from collections import Counter,defaultdict
import argparse,csv,hashlib,json,math,os,platform,re,resource,socket,struct,subprocess,threading,time,urllib.request,urllib.error,urllib.parse

def native_decimal(text):
    # Pinned TuGraph fma-common/text_parser.h:521-556; no rounding tolerance.
    m=re.fullmatch(r'([+-]?)([0-9]+)(?:\.([0-9]*))?(?:[eE]([+-]?[0-9]+))?',text)
    assert m
    value=0.0
    for digit in m[2]:value=value*10+int(digit)
    tail=0.0;mult=.1
    for digit in m[3] or '':tail=tail+mult*int(digit);mult*=.1
    value+=tail
    if m[4]:value*=math.pow(10,int(m[4]))
    return -value if m[1]=='-' else value

def canonical(value,kind,correctly_rounded_decimal=False):
    if value is None:return None
    kind=kind.upper()
    if kind=='STRING':
        assert isinstance(value,str);return value
    if kind.startswith('INT'):
        assert not isinstance(value,bool);return int(value)
    if kind in ('FLOAT','DOUBLE'):
        assert not isinstance(value,bool)
        number=float(value) if not isinstance(value,str) or correctly_rounded_decimal else native_decimal(value)
        if kind=='FLOAT':number=struct.unpack('<f',struct.pack('<f',number))[0]
        return number.hex()
    if kind=='BOOL':
        if isinstance(value,str):assert value.lower() in ['true','false','0','1'];return value.lower() in ['true','1']
        assert isinstance(value,bool);return value
    if kind=='DATE':return date.fromisoformat(value).isoformat()
    if kind=='DATETIME':
        obj=datetime.fromisoformat(value)
        if obj.tzinfo:obj=obj.astimezone(timezone.utc)
        return obj.isoformat(sep=' ',timespec='microseconds')
    raise ValueError('Unverified property kind '+kind)

def rowkey(row,kinds,correctly_rounded_decimal=False):
    assert len(row)==len(kinds)
    return json.dumps([canonical(v,k,correctly_rounded_decimal) for v,k in zip(row,kinds)],ensure_ascii=False,separators=(',',':'))

def ident(value):
    # This engine retains quoting characters in label lookup in the tested API.
    # Configured names in this audit must be simple identifiers; never interpolate
    # arbitrary task text. Original reference queries are not rewritten.
    assert re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',value)
    return value

def main():
    parser=argparse.ArgumentParser();parser.add_argument('job');args=parser.parse_args()
    job=Path(args.job).resolve();root=job.parents[1]
    assert root==Path('/mnt/data/louzehua/ch7_t1_public_benchmark_20261023')
    assert platform.node()==json.loads((root/'execution_owner.json').read_text())['hostname']
    assert not (root/'.public-runtime.lock').exists()
    assert not (job/'receipt.json').exists(),'Do not replay an existing graph audit'
    spec=json.loads((job/'spec.json').read_text());auth=json.loads((job/'.client-private.json').read_text())
    runtime=root/'text2gql_native_v452_prepare_20261026';inputs=job/'inputs'
    cfg=json.loads((inputs/'import_config.json').read_text());schemas={s['label']:s for s in cfg['schema']}
    for name,receipt in spec['source_files'].items():
        p=inputs/name;assert p.stat().st_size==receipt['bytes']
        assert hashlib.sha256(p.read_bytes()).hexdigest()==receipt['sha256']
    graph=spec['database_alias'];port=spec['http_port']
    probe=socket.socket();probe.bind(('127.0.0.1',port));probe.close()
    os.nice(10);resource.setrlimit(resource.RLIMIT_AS,(16*1024**4,16*1024**4))
    env=dict(os.environ,LD_LIBRARY_PATH=str(runtime/'lib'),OMP_NUM_THREADS='2',OPENBLAS_NUM_THREADS='2')
    report={'kind':'NATIVE_GRAPH_REFERENCE_AUDIT_NOT_METHOD_EX','started':datetime.now().astimezone().isoformat(),
      'hostname':platform.node(),'worker_pid':os.getpid(),'graph':spec['graph'],'database_alias':graph,
      'source_spec_sha256':hashlib.sha256((job/'spec.json').read_bytes()).hexdigest(),
      'phase':'BEGIN','new_model_calls':0,'planned_references':len(spec['references']),
      'dispatched_references':0,'completed_reference_receipts':0,'reference_status_counts':{},
      'data_checks':[],'metadata_checks':[],'native_data_queries':0,'child_pids':[],
      'api':'TuGraph 4.5.2 loopback REST, not yet proven equivalent to author Bolt outputs',
      'is_cypher_v2':True,'method_prediction_admitted':False,'identity_pass':False}
    server=None;current_child=None;memory_stop=threading.Event();watch_stop=threading.Event()
    (job/'responses').mkdir();(job/'reference_records').mkdir()
    def save():
        temporary=job/'receipt.next'
        temporary.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
        temporary.replace(job/'receipt.json')
    def supervise():
        while not watch_stop.wait(.2):
            child=current_child
            if child is None or child.poll() is not None:continue
            try:
                p=Path('/proc')/str(child.pid)/'status'
                line=next((x for x in p.read_text().splitlines() if x.startswith('VmRSS:')),None)
                if line and int(line.split()[1])>4*1024**2:
                    memory_stop.set();child.terminate()
            except OSError:pass
    watchdog=threading.Thread(target=supervise,daemon=True);watchdog.start()
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}));token=None
    def request(path,payload=None,timeout=65):
        headers={'Content-Type':'application/json; charset=utf-8'}
        if token:headers['Authorization']='Bearer '+token
        req=urllib.request.Request('http://127.0.0.1:'+str(port)+path,
             data=None if payload is None else json.dumps(payload).encode(),headers=headers)
        try:
            with opener.open(req,timeout=timeout) as response:
                raw=response.read(64*1024**2+1)
                if len(raw)>64*1024**2:raise ValueError('RESPONSE_SIZE_LIMIT')
                return response.status,raw,json.loads(raw)
        except urllib.error.HTTPError as exc:
            raw=exc.read(1024*1024)
            return exc.code,raw,json.loads(raw)
    def query(script,category,name):
        status,raw,body=request('/cypher',{'graph':graph,'script':script,'timeout':60})
        (job/'responses'/(category+'_'+name+'.json')).write_bytes(raw)
        return status,body,hashlib.sha256(raw).hexdigest()
    try:
        report['phase']='IMPORTING';save()
        cmd=[str(runtime/'bin/lgraph_import'),'--online','false','-c',str(inputs/'import_config.json'),
          '--dir',str(job/'db'),'--graph',graph,'--overwrite','false','--continue_on_error','false',
          '--parse_block_threads','2','--parse_file_threads','2','--generate_sst_threads','2',
          '--read_rocksdb_threads','2','-u',auth['user'],'-p',auth['password']]
        db_path=job/'db'
        if spec.get('existing_import_job'):
            prior=job.parent/spec['existing_import_job']
            assert prior.parent==job.parent and prior!=job
            previous=json.loads((prior/'receipt.json').read_text())
            prior_spec=json.loads((prior/'spec.json').read_text())
            assert previous['import_returncode']==0 and all(previous['child_pids_absent'].values())
            assert previous['dispatched_references']==0
            assert prior_spec['source_files']==spec['source_files'] and prior_spec['database_alias']==graph
            assert all(not Path('/proc/'+str(p)).exists() for p in previous['child_pids'])
            rc=0;db_path=prior/'db';report['reused_import_database']=str(db_path)
        else:
            with (job/'import.log').open('xb') as log:
                current_child=subprocess.Popen(cmd,cwd=inputs,env=env,stdout=log,stderr=subprocess.STDOUT)
                report['child_pids'].append(current_child.pid);save()
                try:rc=current_child.wait(timeout=180)
                except subprocess.TimeoutExpired:raise TimeoutError('Import phase deadline')
        report['import_returncode']=rc
        if memory_stop.is_set():raise MemoryError('Importer resident limit')
        if rc:raise RuntimeError('Import failed; retained log needs audit')
        report['phase']='STARTING_LOOPBACK';save()
        server_cfg={'directory':str(db_path),'host':'127.0.0.1','port':port,'bolt_port':0,
          'enable_rpc':False,'enable_ha':False,'thread_limit':2,'verbose':1,
          'log_dir':str(job/'logs'),'log_file_size':16,'max_n_log_files':2,
          'enable_audit_log':False,'is_cypher_v2':True}
        (job/'server.json').write_text(json.dumps(server_cfg))
        with (job/'server.log').open('xb') as log:
            server=subprocess.Popen([str(runtime/'bin/lgraph_server'),'--config',str(job/'server.json'),'-d','run'],
              cwd=job,env=env,stdout=log,stderr=subprocess.STDOUT)
        current_child=server;report['child_pids'].append(server.pid);save()
        deadline=time.monotonic()+30
        while True:
            if server.poll() is not None:raise RuntimeError('Server startup exit')
            try:
                with socket.create_connection(('127.0.0.1',port),timeout=.3):break
            except OSError:
                if time.monotonic()>deadline:raise TimeoutError('Server startup deadline')
                time.sleep(.2)
        bindings=[]
        for name in ['/proc/net/tcp','/proc/net/tcp6']:
            for line in Path(name).read_text().splitlines()[1:]:
                cols=line.split()
                if cols[3]=='0A' and int(cols[1].split(':')[1],16)==port:bindings.append(cols[1])
        assert bindings==['0100007F:'+format(port,'04X')]
        report['listen_bindings']=bindings
        status,_,login=request('/login',auth);assert status==200
        token=login['jwt'];report['phase']='VERIFYING_ALL_CONFIGURED_DATA';save()
        for schema in cfg['schema']:
            kind='node' if schema['type']=='VERTEX' else 'relationship'
            label=schema['label']
            code,raw,body=request('/db/'+graph+'/label/'+kind+'/'+urllib.parse.quote(label))
            digest=hashlib.sha256(raw).hexdigest()
            (job/'responses'/('schema_'+kind+'_'+hashlib.sha256(label.encode()).hexdigest()[:12]+'.json')).write_bytes(raw)
            expected={p['name']:{'type':p['type'].upper(),'optional':p.get('optional',False)} for p in schema.get('properties',[])}
            actual={k:{'type':v['type'].upper(),'optional':v['optional']} for k,v in body.items()} if code==200 else None
            report['metadata_checks'].append({'label':label,'kind':kind,'http_status':code,
                 'matches':actual==expected,'response_sha256':digest})
        grouped=defaultdict(list)
        for file in cfg['files']:
            schema=schemas[file['label']]
            group=(file['label'],file.get('SRC_ID'),file.get('DST_ID'))
            grouped[group].append(file)
        for idx,((label,src,dst),files) in enumerate(grouped.items()):
            schema=schemas[label];props=schema.get('properties',[]);prefix=[]
            if schema['type']=='VERTEX':
                script='MATCH (n:'+ident(label)+') RETURN '+','.join('n.'+ident(p['name'])+' AS p'+str(i) for i,p in enumerate(props))
                fields=[p['name'] for p in props];kinds=[p['type'] for p in props]
            else:
                src_key=schemas[src]['primary'];dst_key=schemas[dst]['primary']
                src_kind=next(p['type'] for p in schemas[src]['properties'] if p['name']==src_key)
                dst_kind=next(p['type'] for p in schemas[dst]['properties'] if p['name']==dst_key)
                projection=['a.'+ident(src_key)+' AS src','b.'+ident(dst_key)+' AS dst']
                projection+=['r.'+ident(p['name'])+' AS p'+str(i) for i,p in enumerate(props)]
                script='MATCH (a:'+ident(src)+')-[r:'+ident(label)+']->(b:'+ident(dst)+') RETURN '+','.join(projection)
                fields=['SRC_ID','DST_ID']+[p['name'] for p in props];kinds=[src_kind,dst_kind]+[p['type'] for p in props]
            expected=Counter();correctly_rounded=Counter()
            for file in files:
                with (inputs/file['path']).open(encoding='utf-8-sig',newline='') as stream:
                    for lineno,row in enumerate(csv.reader(stream),1):
                        if lineno<=file.get('header',0):continue
                        values=dict(zip(file['columns'],row))
                        rawrow=[None if values.get(f,'')=='' else values[f] for f in fields]
                        expected[rowkey(rawrow,kinds)]+=1
                        correctly_rounded[rowkey(rawrow,kinds,True)]+=1
            report['native_data_queries']+=1
            code,body,digest=query(script,'identity',str(idx))
            actual=Counter(rowkey(row,kinds) for row in body['result']) if code==200 else Counter()
            report['data_checks'].append({'label':label,'source_label':src,'target_label':dst,
                'expected_rows':sum(expected.values()),'actual_rows':sum(actual.values()),'http_status':code,
                'matches':code==200 and actual==expected,'missing_rows':sum((expected-actual).values()),
                'extra_rows':sum((actual-expected).values()),'response_sha256':digest,
                'matches_correctly_rounded_decimal':code==200 and actual==correctly_rounded,
                'decimal_rounding_difference_rows':sum((correctly_rounded-actual).values()) if code==200 else None,
                'numeric_identity_policy':'Exact pinned native decimal-parser operation order, not a tolerance or score change'})
            save()
        report['identity_pass']=all(x['matches'] for x in report['metadata_checks']+report['data_checks'])
        if not report['identity_pass']:raise RuntimeError('Native metadata/property/endpoint mismatch; no reference dispatch')
        report['phase']='AUDITING_REFERENCE_QUERIES';save()
        statuses=Counter()
        for index,task in enumerate(spec['references']):
            record={'id':task['id'],'index':index,'query_sha256':hashlib.sha256(task['query'].encode()).hexdigest()}
            path=job/'reference_records'/(str(index).zfill(5)+'.json');assert not path.exists()
            if not task['readonly_lexical_gate']:
                record['status']='NOT_DISPATCHED_READONLY_GATE'
            else:
                report['dispatched_references']+=1;save()
                record['request_started']=datetime.now().astimezone().isoformat()
                try:
                    code,body,digest=query(task['query'],'reference',str(index).zfill(5))
                    record.update({'http_status':code,'response_sha256':digest,'size':body.get('size')})
                    if code==200:
                        record['status']='EXECUTED_EMPTY' if body.get('size')==0 else 'EXECUTED_NONEMPTY'
                    else:
                        message=str(body.get('error_message',''))
                        record.update({'status':'GOLD_TIMEOUT' if 'timeout' in message.lower() else 'GOLD_EXECUTION_ERROR',
                                       'error':message.replace(auth['password'],'[REDACTED]')[:1000]})
                except Exception as exc:
                    record.update({'status':'TRANSPORT_OR_RESOURCE_STOP','error_type':type(exc).__name__})
                    path.write_text(json.dumps(record,ensure_ascii=False,indent=2)+'\n');statuses[record['status']]+=1
                    report['completed_reference_receipts']+=1;report['reference_status_counts']=dict(statuses);save()
                    raise RuntimeError('Unknown native query state: stop own server, no replay')
            record['request_finished']=datetime.now().astimezone().isoformat()
            path.write_text(json.dumps(record,ensure_ascii=False,indent=2)+'\n')
            statuses[record['status']]+=1
            report['completed_reference_receipts']+=1;report['reference_status_counts']=dict(statuses);save()
        report['phase']='GRAPH_REFERENCE_AUDIT_FINISHED'
    except Exception as exc:
        report['phase']='STOPPED';report['error_type']=type(exc).__name__
        report['error']=str(exc).replace(auth['password'],'[REDACTED]')[:400]
    finally:
        watch_stop.set();watchdog.join(timeout=2)
        for proc in [current_child,server]:
            if proc is not None and proc.poll() is None:
                proc.terminate()
                try:proc.wait(timeout=10)
                except subprocess.TimeoutExpired:proc.kill();proc.wait()
        report['resident_limit_stop']=memory_stop.is_set()
        report['child_pids_absent']={str(p):not Path('/proc/'+str(p)).exists() for p in report['child_pids']}
        report['not_dispatched_references']=report['planned_references']-report['dispatched_references']
        report['finished']=datetime.now().astimezone().isoformat();save()
        print(json.dumps({'phase':report['phase'],'graph':spec['graph'],'identity_pass':report['identity_pass'],
            'completed_reference_receipts':report['completed_reference_receipts'],
            'reference_status_counts':report['reference_status_counts'],'error_type':report.get('error_type')}),flush=True)

if __name__=='__main__':main()
