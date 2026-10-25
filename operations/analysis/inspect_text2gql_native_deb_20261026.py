"""Inspect fixed Debian data archive, extract only named engine ELF files locally.

No package installation, maintainer scripts, server, importer or model is executed.
"""
from pathlib import Path,PurePosixPath
from datetime import datetime
import hashlib,json,tarfile,shutil,struct

BASE=Path('D:/ch7/reports/text2gql_native_engine_20261026')
receipt=json.loads((BASE/'artifact_receipt.json').read_text(encoding='utf-8'))
deb=BASE/'tugraph-4.5.2-1.x86_64.deb'
assert receipt['status']=='ARTIFACT_DOWNLOADED_CONTROL_INSPECTED'
assert shutil.disk_usage(BASE).free>4*1024**3
member=next(x for x in receipt['ar_members'] if x['name']=='data.tar.gz')
out=BASE/'selected_elf';out.mkdir(exist_ok=True)
catalog=[];extracted=[]
# Initial complete catalog measured server=935263232, import=848934168 bytes.
# Keep that initial report, then extract these bounded, named files locally.
selected={'lgraph_server','lgraph_import','libvsag.so'}
with deb.open('rb') as raw:
    raw.seek(member['offset'])
    with tarfile.open(fileobj=raw,mode='r|gz') as tf:
        for info in tf:
            name=PurePosixPath(info.name)
            assert '..' not in name.parts
            catalog.append({'path':info.name,'bytes':info.size,'type':info.type.decode('ascii'),
                            'linkname':info.linkname})
            if name.name not in selected or not info.isfile(): continue
            if not (0<info.size<1024**3):
                continue  # Catalog oversized ELF first; do not silently raise resource bounds.
            dst=out/name.name
            assert not dst.exists(), 'Do not overwrite previously extracted ELF'
            src=tf.extractfile(info);h=hashlib.sha256()
            with dst.open('xb') as f:
                remaining=info.size
                while remaining:
                    block=src.read(min(1024*1024,remaining));assert block
                    f.write(block);h.update(block);remaining-=len(block)
            # Read ELF headers/dynamic strings, never invoke the executable.
            data=dst.read_bytes();assert data[:6]==b'\x7fELF\x02\x01'
            e_phoff=struct.unpack_from('<Q',data,32)[0]
            e_phentsize,e_phnum=struct.unpack_from('<HH',data,54)
            segments=[]
            for n in range(e_phnum):
                seg=struct.unpack_from('<IIQQQQQQ',data,e_phoff+n*e_phentsize)
                segments.append({'type':seg[0],'offset':seg[2],'vaddr':seg[3],'filesz':seg[5]})
            dynamic=next((s for s in segments if s['type']==2),None)
            needed=[];strtab=None;rpaths=[]
            if dynamic:
                for pos in range(dynamic['offset'],dynamic['offset']+dynamic['filesz'],16):
                    tag,value=struct.unpack_from('<qQ',data,pos)
                    if tag==0:break
                    if tag==1:needed.append(value)
                    elif tag==5:strtab=value
                    elif tag in (15,29):rpaths.append(value)
            def string(offset):
                load=next(s for s in segments if s['type']==1 and s['vaddr']<=strtab<s['vaddr']+s['filesz'])
                pos=load['offset']+strtab-load['vaddr']+offset
                return data[pos:data.index(b'\0',pos)].decode('ascii')
            extracted.append({'archive_path':info.name,'local_name':dst.name,'bytes':info.size,
                'sha256':h.hexdigest(),'needed_libraries':[string(n) for n in needed],
                'rpath_or_runpath':[string(n) for n in rpaths]})
            del data
result={'kind':'PINNED_ENGINE_ARCHIVE_STATIC_INSPECTION_NOT_EXECUTION',
        'time_local':datetime.now().astimezone().isoformat(),'deb_sha256':receipt['sha256'],
        'members':catalog,'unpacked_regular_bytes':sum(x['bytes'] for x in catalog if x['type']=='0'),
        'selected_elf':extracted,'new_model_calls':0,'new_database_queries':0,
        'remote_files_written':0,'engine_executed':False,'package_installed':False}
(BASE/'archive_inspection.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'members':len(catalog),'unpacked_regular_bytes':result['unpacked_regular_bytes'],
                  'selected_elf':extracted},ensure_ascii=True))
