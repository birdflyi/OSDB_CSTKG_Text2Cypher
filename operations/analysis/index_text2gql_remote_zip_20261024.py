"""Range-read an author-linked public ZIP's directory without executing/extracting it."""
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import requests
import zipfile

ROOT=Path(r'D:\ch7\literature\public_candidate_metadata_20261024\Text2GQL-Bench')
URL='https://tugraph-web.oss-cn-beijing.aliyuncs.com/tugraph/datasets/text2gql/Text2GraphQueryBenchmark/Text2GQL-Bench_dataset.zip'


class RemoteObject(io.RawIOBase):
    def __init__(self):
        self.position=0
        self.session=requests.Session()
        response=self.session.head(URL,timeout=20)
        response.raise_for_status()
        self.length=int(response.headers['Content-Length'])
        self.etag=response.headers['ETag']
        self.last_modified=response.headers.get('Last-Modified')
        self.receipts=[]
        self.folder=ROOT/'zip_range_snapshot_20261024'
        self.folder.mkdir(exist_ok=True)
    def seekable(self):return True
    def readable(self):return True
    def tell(self):return self.position
    def seek(self,offset,whence=0):
        self.position=(0 if whence==0 else self.position if whence==1 else self.length)+offset
        if self.position<0:raise ValueError('Negative offset')
        return self.position
    def read(self,size=-1):
        size=min(self.length-self.position,size if size>=0 else self.length-self.position)
        if size<=0:return b''
        if size>32*1024*1024:raise ValueError('Oversize metadata range')
        start=self.position;end=start+size-1
        target=self.folder/f'range_{start}_{end}.bin'
        if target.exists():
            # Never reuse an old object version's range without the identity receipt.
            identity=json.loads((self.folder/'object_identity.json').read_text())
            assert identity['etag']==self.etag and identity['length']==self.length
            payload=target.read_bytes()
        else:
            with self.session.get(URL,headers={'Range':f'bytes={start}-{end}','If-Match':self.etag},
                                  timeout=(10,25),stream=True) as response:
                assert response.status_code==206,'Server did not honor bounded range'
                assert response.headers.get('ETag')==self.etag,'Object changed during snapshot'
                assert response.headers['Content-Range']==f'bytes {start}-{end}/{self.length}'
                payload=response.content
            assert len(payload)==size
            with target.open('xb') as stream:stream.write(payload)
        assert len(payload)==size
        self.receipts.append(dict(start=start,end=end,bytes=size,sha256=hashlib.sha256(payload).hexdigest()))
        self.position+=size
        return payload


if __name__=='__main__':
    stream=RemoteObject()
    identity=dict(url=URL,length=stream.length,etag=stream.etag,last_modified=stream.last_modified)
    ip=stream.folder/'object_identity.json'
    if ip.exists():assert json.loads(ip.read_text())==identity
    else:ip.write_text(json.dumps(identity,indent=2),encoding='utf-8')
    with zipfile.ZipFile(stream) as archive:
        entries=[dict(name=i.filename,compressed_bytes=i.compress_size,bytes=i.file_size,
                      crc32=f'{i.CRC:08x}',offset=i.header_offset,compression=i.compress_type)
                 for i in archive.infolist()]
    result=dict(kind='REMOTE_ZIP_DIRECTORY_ONLY',timestamp_utc=datetime.now(timezone.utc).isoformat(),
        object_identity=identity,entries=entries,range_receipts=stream.receipts,
        downloaded_bytes=sum(x['bytes'] for x in stream.receipts),
        model_calls=0,author_source_executed=False,graph_imported=False,
        archive_fully_downloaded=False,whole_object_sha256=None,
        checksum_note='ETag pins HTTP object identity; it is not a whole-object SHA-256. Ranges have independent SHA-256.')
    (ROOT/'zip_directory_inventory_20261024.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps({k:result[k] for k in ['kind','downloaded_bytes','object_identity']}))
    print('entries',len(entries))
    print('query_files',json.dumps([x['name'] for x in entries if x['name'].endswith('.json') and ('/test/' in x['name'] or 'test' in x['name'].lower())][:45]))
