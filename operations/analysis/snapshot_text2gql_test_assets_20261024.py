"""Snapshot six Cypher TEST graphs and task metadata; no DB/code/model execution."""
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import zipfile
from index_text2gql_remote_zip_20261024 import RemoteObject, ROOT


class WindowedObject(RemoteObject):
    window=None
    def read(self,size=-1):
        if self.window and size>=0:
            start,payload=self.window
            offset=self.position-start
            if 0<=offset and offset+size<=len(payload):
                self.position+=size
                return payload[offset:offset+size]
        return super().read(size)


def snapshot():
    stream=WindowedObject()
    receipts=[]
    with zipfile.ZipFile(stream) as archive:
        entries=[i for i in archive.infolist() if '/test/' in i.filename and '/Cypher/' in i.filename
                 and not i.is_dir() and (i.filename.endswith('_cypher.json')
                 or '/TuGraph-DB_Instance/' in i.filename)]
        assert len(entries)==133
        start=min(i.header_offset for i in entries)
        end=max(i.header_offset+i.compress_size+len(i.filename.encode('utf-8'))+4096 for i in entries)
        assert end-start<8*1024*1024
        stream.seek(start)
        stream.window=(start,stream.read(end-start))
        for info in entries:
            assert info.file_size<10*1024*1024
            assert '..' not in Path(info.filename).parts
            payload=archive.read(info)
            target=ROOT/'selected_zip_assets_20261024'/info.filename
            target.parent.mkdir(parents=True,exist_ok=True)
            if target.exists():assert target.read_bytes()==payload
            else:
                with target.open('xb') as f:f.write(payload)
            receipts.append(dict(member=info.filename,bytes=len(payload),
                sha256=hashlib.sha256(payload).hexdigest(),crc32=f'{info.CRC:08x}'))
    datasets={};identities=[]
    for path in (ROOT/'selected_zip_assets_20261024/dataset/test').glob('*/Cypher/*_cypher.json'):
        rows=json.loads(path.read_text(encoding='utf-8'))
        graph=path.parts[-3]
        identities.extend((graph,str(row['id'])) for row in rows)
        datasets[graph]=dict(records=len(rows),unique_ids=len({str(row['id']) for row in rows}),
            fields=list(rows[0]),difficulty=dict(Counter(str(row['difficulty']) for row in rows)),
            source=dict(Counter(str(row['source']) for row in rows)),
            string_question_rows=sum(isinstance(row['initial_question'],str) for row in rows),
            string_reference_query_rows=sum(isinstance(row['initial_gql'],str) for row in rows))
    result=dict(kind='AUTHOR_LINKED_CYPHER_TEST_SNAPSHOT_NOT_ADMISSION',
        timestamp_utc=datetime.now(timezone.utc).isoformat(),object_etag=stream.etag,
        object_bytes=stream.length,whole_object_sha256=None,range_receipts=stream.receipts,
        assets=receipts,graphs=datasets,total_test_records=len(identities),
        unique_graph_id_pairs=len(set(identities)),model_calls=0,source_executed=False,
        database_imported=False,reference_execution_completed=False,
        engine='TuGraph source package; Neo4j equivalence not established',
        budget_authorized_for_prediction=False)
    (ROOT/'cypher_test_snapshot_inventory_20261024.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps({k:result[k] for k in ['kind','total_test_records','unique_graph_id_pairs','graphs']}))
    print('verified_assets',len(receipts))


if __name__=='__main__':snapshot()
