"""Pinned-file split/input audit only. No query interpretation or execution."""
from collections import Counter
import csv
from datetime import datetime
import hashlib
import io
import json
from pathlib import Path

BASE=Path('D:/ch7/literature/public_candidate_metadata_20261024/Zograscope')
COMMIT='b2f91718b1324fdf8c759131d0d7c47c0eb33373'


def digest(data):return hashlib.sha256(data).hexdigest()
def signature(row):return digest(json.dumps(row,sort_keys=True,separators=(',',':')).encode())


def run():
    tree=json.loads((BASE/'tree.json').read_text(encoding='utf-8'))
    index={x['path']:x for x in tree['tree'] if x['type']=='blob'}
    names={'standard_train':'zograscope_train_v1.csv','standard_test':'zograscope_test_v1.csv',
           'length_train':'zograscope_length_train_v1.csv','length_test':'zograscope_length_test_v1.csv'}
    files={};tables={}
    for label,name in names.items():
        path='data/'+name;data=(BASE/'upstream'/path).read_bytes();entry=index[path]
        blob=hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()
        assert blob==entry['sha'] and len(data)==entry['size']
        rows=list(csv.DictReader(io.StringIO(data.decode('utf-8-sig'))))
        assert len(rows)==len({row['id'] for row in rows})
        assert all(isinstance(v,str) for row in rows for v in row.values())
        tables[label]=rows;files[label]={'path':path,'git_blob':blob,'sha256':digest(data),'rows':len(rows)}
    for name in ('ids_iid_test.txt','ids_compositional_test.txt'):
        path='data/'+name;data=(BASE/'upstream'/path).read_bytes()
        assert hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()==index[path]['sha']
    ids=lambda rows:{r['id'] for r in rows}
    st=tables['standard_train'];se=tables['standard_test'];lt=tables['length_train'];le=tables['length_test']
    standard={r['id']:r for r in st+se};length={r['id']:r for r in lt+le}
    assert not ids(st)&ids(se) and not ids(lt)&ids(le)
    assert set(standard)==set(length)
    conflicts=[qid for qid in standard if signature(standard[qid])!=signature(length[qid])]
    assert not conflicts,'Same ID has changed row across release partitions'
    iid=set((BASE/'upstream/data/ids_iid_test.txt').read_text().split())
    comp=set((BASE/'upstream/data/ids_compositional_test.txt').read_text().split())
    assert not iid&comp and iid|comp==ids(se)
    panels={'IID':[r for r in se if r['id'] in iid],'COMP':[r for r in se if r['id'] in comp],'LEN':le}
    def input_meta(rows):
        return {'n':len(rows),'num_nodes':dict(Counter(r['num_nodes'] for r in rows)),
                'type':dict(Counter(r['type'] for r in rows)),
                'raw_nl_empty':sum(not r['nl'].strip() for r in rows),
                'gold_linked_nl_empty':sum(not r['nl_gold_linked'].strip() for r in rows),
                'entities_field_blank':sum(not r['entities'].strip() for r in rows),
                'raw_and_gold_linked_nl_identical':sum(r['nl']==r['nl_gold_linked'] for r in rows)}
    overlaps={a:{b:len(ids(tables[a])&ids(tables[b])) for b in names} for a in names}
    lexical_overlap={};key_hypotheses={}
    for panel,rows in panels.items():
        train=lt if panel=='LEN' else st
        lexical_overlap[panel]={}
        for field in ('nl','nl_gold_linked'):
            tr={digest(r[field].encode()) for r in train}
            hits=[r for r in rows if digest(r[field].encode()) in tr]
            lexical_overlap[panel][field]={'test_rows_with_exact_text_in_correct_train':len(hits),
                'unique_exact_test_texts_matching_train':len({digest(r[field].encode()) for r in hits}),
                'normalization':'exact UTF-8 text, no paraphrase/equivalence inference'}
        key_hypotheses[panel]={}
        for fields in [('template_id',),('num_nodes','template_id'),('num_nodes','type','template_id')]:
            key=lambda r:tuple(r[f] for f in fields)
            known={key(r) for r in train};hits=[r for r in rows if key(r) in known]
            key_hypotheses[panel]['+'.join(fields)]={
                'test_rows_with_key_in_correct_train':len(hits),
                'test_unique_keys':len({key(r) for r in rows}),
                'shared_unique_keys':len({key(r) for r in hits})}
    union=ids(se)|ids(le)
    out={'kind':'ZOGRASCOPE_PINNED_INPUT_AND_SPLIT_CONTRACT_AUDIT',
         'time_local':datetime.now().astimezone().isoformat(),'commit':COMMIT,'files':files,
         'common_corpus_unique_ids':len(standard),'same_id_row_conflicts':conflicts,
         'standard_and_length_corpus_exact_row_identity':True,'id_overlap_matrix':overlaps,
         'panels':{name:input_meta(rows) for name,rows in panels.items()},
         'correct_train_node_counts':{'standard':dict(Counter(r['num_nodes'] for r in st)),
                                      'length':dict(Counter(r['num_nodes'] for r in lt))},
         'exact_text_overlap_with_correct_train':lexical_overlap,
         'metadata_grouping_hypotheses_not_structural_proof':key_hypotheses,
         'unique_union_test_ids_for_identical_zero_shot_inputs':len(union),
         'reuse_condition':'Only identical full model/prompt/context/permissions may share zero-shot predictions across panels. Training or ICL changes require separate predictions.',
         'training_contracts':{'IID_COMP':'standard_train only; standard_test held out',
                               'LEN':'length_train only; length_test held out'},
         'wrong_checkpoint_warning':'Standard_train contains LEN test IDs; length_train contains standard test IDs. Do not share a supervised checkpoint across these protocols without exposure disclosure.',
         'entity_linking_tracks':{'author_README_main':'nl_gold_linked for all methods equally',
                                  'raw_NL':'nl, separate declared track with a fixed equal linking procedure'},
         'template_family_status':'No tested metadata composite has been established as the author anonymized structural family. Overlap is not proof of leakage or invalid split; zero overlap is not proof of novelty.',
         'length_status':'Author length is relative to its own training partition; do not call it unseen graph-domain generalization.',
         'new_model_calls':0,'new_database_queries':0,'network_requests':0,
         'author_code_executed':False,'gold_queries_interpreted_or_executed':False,
         'CypherBench_frozen_method_modified':False,'prediction_admitted':False,
         'outstanding':['Paper/repository count discrepancy IID767/COMP1350 versus pinned768/1349',
                        'Original graph version, native deployment and reference-result certification',
                        'Author structural-family definition/evaluator and stable statistical grouping',
                        'Named baseline checkpoint identity and training exposure',
                        'Frozen method/input/evaluator and additional scoped model budget']}
    path=Path('D:/ch7/reports/zograscope_split_input_contracts_20261025.json')
    path.write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'common_corpus':len(standard),'overlap_matrix':overlaps,
        'panels':out['panels'],'lexical_overlap':lexical_overlap,
        'metadata_keys':key_hypotheses,'unique_test_union':len(union)}))


if __name__=='__main__':run()
