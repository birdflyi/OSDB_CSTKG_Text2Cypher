"""Source-only audit of the released MTQ corpus against the paper domain list.

Never execute author code or Cypher, import a generation client, or read model
outputs. Paper-domain membership is a reconstruction candidate, not certified
paper sample identity or formal prediction admission. Preserve all positions.
"""
import ast
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys
import urllib.request

ROOT=Path('D:/ch7')
REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
CANON=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
BASE=ROOT/'literature/public_candidate_metadata_20261024/MindTheQuery'
REV='ec4054a1b7df7e6310efa95c56bb6e7cebc4d161'
ZOG='b2f91718b1324fdf8c759131d0d7c47c0eb33373'
DOMAINS={'bloom50':1547,'healthcare':2566,'wwc':2946}
OUT=ROOT/'reports/mtq_paper_cohort_contract_20261026'

def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def read(path): return json.loads(path.read_text(encoding='utf-8-sig'))
def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('x',encoding='utf-8',newline='\n') as stream:
        json.dump(value,stream,ensure_ascii=False,indent=2);stream.write('\n')
def fp(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
def pair(row): return row['NL Question'],row['Cypher']
def get(url):
    req=urllib.request.Request(url,headers={'User-Agent':'T1-public-source-contract-audit'})
    with urllib.request.urlopen(req,timeout=30) as response:
        assert response.status==200
        data=response.read(16*1024**2+1)
    assert len(data)<=16*1024**2
    return data
def verified(data,entry):
    assert len(data)==entry['size']
    blob=hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()
    assert blob==entry['sha']
    return {'path':entry['path'],'bytes':len(data),'git_blob':blob,'sha256':hashlib.sha256(data).hexdigest()}

def main():
    assert not OUT.exists(),'One-shot output already exists; preserve and inspect.'
    OUT.mkdir()
    tree=read(BASE/'tree.json'); assert tree.get('sha')==REV and not tree.get('truncated')
    entries={e['path']:e for e in tree['tree'] if e['type']=='blob'}
    original=[e for name,e in entries.items() if name.startswith('Automated_Validated_Datasets/')
        and name.split('/')[1] in DOMAINS and name.endswith('_all_passed.json')]
    author=[e for name,e in entries.items() if name.endswith('.py')]
    assert len(original)==15
    runtime=REPO/'experiment-harness/public_benchmark_v1'
    before={str(p.relative_to(runtime)):sha(p) for p in runtime.rglob('*.py')}
    freeze=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')
    for name,digest in freeze['code_files'].items(): assert sha(CANON/name)==digest
    sys.path.insert(0,str(CANON/'experiment-harness/public_benchmark_v1'))
    from attempt_budget import AttemptBudget
    budget=AttemptBudget(ROOT/'experiments/ch7_private/continuation_20261021/budget',
        ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json',path_rewrites=[
            (r'C:\Users\10651\Documents\trae_projects\ch7_private',ROOT/'experiments/ch7_private')])
    balance=budget.inspect()
    assert balance['reserved']==5192 and balance['cap']==12000 and not balance['pending']
    write(OUT/'protocol.json',{'kind':'PAPER_DOMAIN_COHORT_RECONSTRUCTION_CANDIDATE_NOT_FORMAL_EVALUATION',
        'sealed_local':datetime.now().astimezone().isoformat(),'source_sha256':sha(Path(__file__)),
        'author_commit':REV,'domains_and_expected_counts_from_paper_table':DOMAINS,
        'planned_original_files':15,'planned_author_python_files':len(author),
        'selection':'All original five categories in all three paper-held-out domains; no method results read.',
        'identity':'Exact decoded question/query pair AND full decoded JSON row; multisets preserve duplicates. No semantic normalization.',
        'limitations':['Matching domain counts does not certify paper IDs or complete manual correctness.',
                      'Current released TRAIN/TEST names are a different split contract.',
                      'No label imputation, reference correction, task deletion, author code execution or new prediction.']})
    def fetch(entry):
        name=entry['path']; cached=BASE/'upstream'/name
        if cached.is_file():
            data=cached.read_bytes(); receipt=verified(data,entry);receipt['existing_cache']=True
        else:
            data=get('https://raw.githubusercontent.com/endeavorXx/Mind-the-Query/'+REV+'/'+name)
            receipt=verified(data,entry);receipt['existing_cache']=False
            dest=OUT/'upstream'/name;dest.parent.mkdir(parents=True,exist_ok=True)
            with dest.open('xb') as stream: stream.write(data)
        return name,data,receipt
    with ThreadPoolExecutor(max_workers=3) as pool: fetched=list(pool.map(fetch,original+author))
    receipts=[r for _,_,r in fetched]
    write(OUT/'source_receipts.json',receipts)
    original_groups=defaultdict(list)
    definitions=[]
    for name,data,_ in fetched:
        if name.endswith('.json'):
            rows=json.loads(data);assert isinstance(rows,list)
            original_groups[name.split('/')[1]].extend(rows)
        elif name.endswith('.py'):
            parsed=ast.parse(data.decode('utf-8-sig'))
            definitions.append({'path':name,'sha256':hashlib.sha256(data).hexdigest(),
                'functions':[{'name':n.name,'line':n.lineno,'end':getattr(n,'end_lineno',None)}
                    for n in ast.walk(parsed) if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef))]})
    split_entries=[e for name,e in entries.items() if name.startswith('Train_Test_Splits/Automated/')
        and len(name.split('/'))==5 and name.split('/')[2] in DOMAINS and name.endswith('.json')]
    released=defaultdict(list);split_receipts=[];positions=[]
    for entry in sorted(split_entries,key=lambda e:e['path']):
        name=entry['path'];data=(BASE/'upstream'/name).read_bytes()
        split_receipts.append(verified(data,entry));graph=name.split('/')[2];split=name.split('/')[3]
        rows=json.loads(data);released[(graph,split)].extend(rows)
        for index,row in enumerate(rows):
            positions.append({'graph':graph,'released_split':split,'source_path':name,
                'index':index,'unique_id':row.get('unique_id'),'pair_sha256':fp(pair(row)),
                'row_sha256':fp(row),'disposition':'PAPER_DOMAIN_CANDIDATE_NOT_ADMITTED'})
    graph_summaries={}
    for graph,expected in DOMAINS.items():
        full=original_groups[graph];tr=released[(graph,'train')];te=released[(graph,'test')]
        assert len(full)==len(tr)+len(te)==expected
        assert Counter(map(pair,full))==Counter(map(pair,tr+te))
        row_identity=Counter(map(fp,full))==Counter(map(fp,tr+te))
        graph_summaries[graph]={'original_positions':len(full),'released_train_positions':len(tr),
            'released_test_positions':len(te),'full_question_query_multiset_identity':True,
            'full_json_row_multiset_identity':row_identity,
            'exact_pair_unique':len(set(map(pair,full))),
            'pair_duplicate_excess':len(full)-len(set(map(pair,full)))}
    assert len(positions)==7059
    old=read(ROOT/'reports/mindthequery_split_contracts_20261025.json')
    assert old['commit']==REV and old['audit_complete']
    split=old['split_summary']
    held_out_released_train=sum(graph_summaries[g]['released_train_positions'] for g in DOMAINS)
    held_out_released_test=sum(graph_summaries[g]['released_test_positions'] for g in DOMAINS)
    manual_rows=sum(v['rows'] for k,v in split.items() if k.startswith('Manual/') and k.endswith('/test'))
    labels=Counter()
    for key,value in split.items():
        if key.startswith('Manual/') and key.endswith('/test'): labels.update(value['logical_labels'])
    assert manual_rows==1829 and labels==Counter({'1':1316,'':428,'0':85})
    write(OUT/'candidate_positions.json',positions)
    write(OUT/'author_function_inventory.json',definitions)
    papers={
        'mindthequery_paper.pdf':'https://aclanthology.org/2025.emnlp-industry.133.pdf',
        'spot_paper.pdf':'https://aclanthology.org/2025.findings-acl.524.pdf'}
    paper_receipts=[]
    for name,url in papers.items():
        data=get(url);assert data.startswith(b'%PDF-')
        (OUT/name).write_bytes(data)
        paper_receipts.append({'url':url,'file':name,'sha256':sha(OUT/name),'bytes':len(data)})
    public_links=[]
    for project in ('interact-erc/SPOT','interact-erc/Text-To-Cypher-knowledge-graph-data-generation'):
        commit=json.loads(get('https://api.github.com/repos/'+project+'/commits/main'))
        revision=commit['sha'];assert len(revision)==40
        tree2=json.loads(get('https://api.github.com/repos/'+project+'/git/trees/'+revision+'?recursive=1'))
        assert tree2['sha']==revision and not tree2.get('truncated')
        lookup={e['path']:e for e in tree2['tree'] if e['type']=='blob'}
        raw=get('https://raw.githubusercontent.com/'+project+'/'+revision+'/README.md')
        r=verified(raw,lookup['README.md']);r.update(repository=project,commit=revision)
        folder=OUT/project.split('/')[-1];write(folder/'tree.json',tree2)
        (folder/'README.md').write_bytes(raw);public_links.append(r)
    for name,digest in freeze['code_files'].items(): assert sha(CANON/name)==digest
    assert before=={str(p.relative_to(runtime)):sha(p) for p in runtime.rglob('*.py')}
    assert balance==budget.inspect()
    summary={'kind':'MTQ_PAPER_DOMAIN_CANDIDATE_AND_CANDIDATE_SCORE_SOURCE_AUDIT',
        'time_local':datetime.now().astimezone().isoformat(),'MTQ_commit':REV,'ZOGRASCOPE_commit':ZOG,
        'original_files_planned':15,'original_files_verified':len(original),
        'author_python_files_planned':len(author),'author_python_files_verified_and_ast_parsed':len(definitions),
        'original_graphs':graph_summaries,'paper_domain_candidate_positions':len(positions),
        'candidate_positions_from_current_released_TRAIN':held_out_released_train,
        'candidate_positions_from_current_released_TEST':held_out_released_test,
        'paper_18470_2000_training_validation_ID_assignment_established':False,
        'paper_7059_exact_experimental_ID_manifest_established':False,
        'manual_test_positions_retained':manual_rows,'manual_logical_labels':dict(labels),
        'paper_annotation_semantics':'logical=semantic correctness; result=nonempty output, with0 covering empty OR error; neither is a new-model EX score.',
        'manual_only_missing_result_labels':859,'manual_positive_with_result0_not_assumed_empty':206,
        'ZOGRASCOPE_pinned_tree_complete':True,
        'ZOGRASCOPE_pinned_python_file_count':sum(e['type']=='blob' and e['path'].endswith('.py')
            for e in read(ROOT/'literature/public_candidate_metadata_20261024/Zograscope/tree.json')['tree']),
        'papers':paper_receipts,'author_linked_source_repositories':public_links,
        'frozen_source_hashes_unchanged':len(freeze['code_files']),
        'method_runtime_hashes_unchanged':len(before),'actual_attempts':balance['reserved'],
        'active_cap':balance['cap'],'remaining_original_CB_scope':balance['remaining'],'pending':balance['pending'],
        'formal_N':None,'formal_dispatched':0,'formal_completed':0,'formal_failed':0,'formal_undispatched':None,
        'new_model_calls':0,'new_database_queries':0,'new_private_endpoint_probes':0,
        'author_code_executed':False,'prediction_admitted':False,'T1_complete':False,
        'Text2GQL_conditional_budget_activated':False,
        'protocol_sha256':sha(OUT/'protocol.json'),'position_registry_sha256':sha(OUT/'candidate_positions.json'),
        'limits':['Counts and content alignment establish a release-corpus candidate, not proof of paper run IDs or correct reference answers.',
                  'Do not use current released training files as ICL/training for the candidate domain holdout.',
                  'Do not execute data generators, train models, or reinterpret source validation flags as prediction correctness.',
                  'Final answer order/multiplicity/types/null/failure comparator and graph identity still require explicit acceptance.']}
    write(OUT/'released_split_receipts.json',split_receipts)
    write(OUT/'summary.json',summary)
    write(ROOT/'reports/t1_mtq_zog_score_source_contract_summary_20261026.json',summary)
    print(json.dumps({k:v for k,v in summary.items() if k not in ('papers','author_linked_source_repositories','limits')},ensure_ascii=True))

if __name__=='__main__': main()
