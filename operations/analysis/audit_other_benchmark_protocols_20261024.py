"""Pinned public assets and split metadata only; never execute author code or queries."""
from collections import Counter
import csv
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import requests

ROOT=Path(r'D:\ch7\literature\public_candidate_metadata_20261024')
SOURCES={
 'Zograscope': ('interact-erc/ZOGRASCOPE','b2f91718b1324fdf8c759131d0d7c47c0eb33373',[
    'data/zograscope_test_v1.csv','data/zograscope_train_v1.csv',
    'data/zograscope_length_test_v1.csv','data/zograscope_length_train_v1.csv']),
 'MindTheQuery': ('endeavorXx/Mind-the-Query','ec4054a1b7df7e6310efa95c56bb6e7cebc4d161',[
    'Manually_Validated_Datasets/annotation_protocol.txt'])}


def fetch():
    receipts=[]
    for name,(repo,commit,paths) in SOURCES.items():
        folder=ROOT/name
        tree=json.loads((folder/'tree.json').read_text(encoding='utf-8'))
        index={x['path']:x for x in tree['tree'] if x['type']=='blob'}
        for path in paths:
            assert '.env' not in path
            entry=index[path]
            target=folder/'upstream'/path
            url=f'https://raw.githubusercontent.com/{repo}/{commit}/{path}'
            if target.exists():
                payload=target.read_bytes()
            else:
                response=requests.get(url,timeout=25)
                response.raise_for_status()
                payload=response.content
            blob=hashlib.sha1(b'blob '+str(len(payload)).encode()+b'\0'+payload).hexdigest()
            assert len(payload)==entry['size'] and blob==entry['sha'],path
            if not target.exists():
                target.parent.mkdir(parents=True,exist_ok=True)
                with target.open('xb') as stream:stream.write(payload)
            receipts.append(dict(benchmark=name,path=path,url=url,bytes=len(payload),git_blob=blob,
                sha256=hashlib.sha256(payload).hexdigest(),status='VERIFIED_PINNED_BLOB'))
    result=dict(timestamp_utc=datetime.now(timezone.utc).isoformat(),model_calls=0,
                author_source_executed=False,test_assets_evaluator_only=True,files=receipts)
    (ROOT/'protocol_audit_additional_assets_20261024.json').write_text(json.dumps(result,indent=2),encoding='utf-8')


def inventory():
    root=ROOT/'Zograscope/upstream/data'
    def read_csv(name):
        return list(csv.DictReader(io.StringIO((root/name).read_text(encoding='utf-8-sig'))))
    def ids(rows):return {row['id'] for row in rows}
    tables={name:read_csv(name) for name in SOURCES['Zograscope'][2] for name in [Path(name).name]}
    iid=[s.strip() for s in (root/'ids_iid_test.txt').read_text().splitlines() if s.strip()]
    comp=[s.strip() for s in (root/'ids_compositional_test.txt').read_text().splitlines() if s.strip()]
    test=tables['zograscope_test_v1.csv'];train=tables['zograscope_train_v1.csv']
    length_test=tables['zograscope_length_test_v1.csv'];length_train=tables['zograscope_length_train_v1.csv']
    standard_corpus_by_id={row['id']:row for row in test+train}
    def metadata(rows):
        return dict(records=len(rows),unique_ids=len(ids(rows)),fields=list(rows[0]),
            num_nodes=dict(Counter(r['num_nodes'] for r in rows)),
            query_type_counts=dict(Counter(r['type'] for r in rows)),
            template_ids=len({r['template_id'] for r in rows}),
            missing_fields=dict(Counter(k for r in rows for k,v in r.items() if v is None or v=='')))
    partitions={'IID':[r for r in test if r['id'] in set(iid)],
                'COMP':[r for r in test if r['id'] in set(comp)],'LEN':length_test}
    train_templates={r['template_id'] for r in train}
    result=dict(kind='PINNED_SPLIT_METADATA_NOT_MODEL_RESULTS',timestamp_utc=datetime.now(timezone.utc).isoformat(),
        repository=SOURCES['Zograscope'][0],commit=SOURCES['Zograscope'][1],model_calls=0,
        rows_by_file={k:metadata(v) for k,v in tables.items()},
        iid_id_lines=len(iid),comp_id_lines=len(comp),iid_unique=len(set(iid)),comp_unique=len(set(comp)),
        iid_comp_overlap=len(set(iid)&set(comp)),
        listed_test_ids_missing_from_csv=sorted((set(iid)|set(comp))-ids(test)),
        unassigned_test_ids=sorted(ids(test)-(set(iid)|set(comp))),
        train_test_id_overlap=len(ids(train)&ids(test)),
        length_train_test_id_overlap=len(ids(length_train)&ids(length_test)),
        length_test_overlap_standard_test=len(ids(length_test)&ids(test)),
        length_test_overlap_standard_train=len(ids(length_test)&ids(train)),
        unique_union_test_ids=len(ids(length_test)|ids(test)),
        length_test_identical_rows_in_standard_corpus=sum(
            row==standard_corpus_by_id[row['id']] for row in length_test),
        partitions={k:metadata(v) for k,v in partitions.items()},
        template_id_overlap_with_standard_train={k:len({r['template_id'] for r in v}&train_templates) for k,v in partitions.items()},
        zero_shot_partition_independence_caveat='IID/COMP/LEN are author partitions relative to author training data, not independent graph domains.',
        entity_linking_protocol='README reported scores use nl_gold_linked; raw nl requires a separately labelled track. Never give gold links to one method only.',
        reference_execution_completed=False,prediction_admitted=False)
    (ROOT/'Zograscope/split_protocol_inventory_20261024.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps(result))
    print('MIND_ANNOTATION_PROTOCOL', (ROOT/'MindTheQuery/upstream/Manually_Validated_Datasets/annotation_protocol.txt').read_text(encoding='utf-8'))


if __name__=='__main__':
    fetch()
    inventory()
