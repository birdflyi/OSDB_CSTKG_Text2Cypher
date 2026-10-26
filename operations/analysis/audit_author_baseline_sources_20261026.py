"""Bounded primary-source capture/static inventory; never import author code.

One request per explicit URL, no retries, no credentials, no model/DB calls.
Raw copyrighted sources and PDF text stay outside the publication repository.
"""
import ast
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys

import fitz
import requests
from bs4 import BeautifulSoup

ROOT = Path('D:/ch7')
REPO = ROOT / 'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
CANON = ROOT / 'code/OSDB_CSTKG_Text2Cypher_publication_t1'
OUT = ROOT / 'reports/author_baseline_contracts_20261026'
TREE = ROOT / 'reports/mtq_paper_cohort_contract_20261026/Text-To-Cypher-knowledge-graph-data-generation/tree.json'
REV = '013eab46e71aa1b5cfbd3ed91a44c030b17931ea'
SOURCES = {
    'ri_official.html': 'https://proceedings.nips.cc/paper_files/paper/2025/hash/906727703322b2e33b37c855fc15e8b7-Abstract-Conference.html',
    'ri_official.pdf': 'https://papers.nips.cc/paper_files/paper/2025/file/906727703322b2e33b37c855fc15e8b7-Paper-Conference.pdf',
    'smith_official.html': 'https://aclanthology.org/2026.acl-long.1601/',
    'smith_official.pdf': 'https://aclanthology.org/2026.acl-long.1601.pdf',
    'cyquark_v1.html': 'https://arxiv.org/html/2606.14325v1',
}


def read(p): return json.loads(p.read_text(encoding='utf-8-sig'))
def digest(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def save(p, v):
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open('x', encoding='utf-8', newline='\n') as f:
        json.dump(v, f, ensure_ascii=False, indent=2); f.write('\n')
def runtime():
    base = REPO / 'experiment-harness/public_benchmark_v1'
    return {str(p.relative_to(base)): digest(p) for p in base.rglob('*.py')}
def budget():
    sys.path.insert(0, str(CANON / 'experiment-harness/public_benchmark_v1'))
    from attempt_budget import AttemptBudget
    b = AttemptBudget(ROOT/'experiments/ch7_private/continuation_20261021/budget',
        ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json',
        path_rewrites=[(r'C:\Users\10651\Documents\trae_projects\ch7_private',
                        ROOT/'experiments/ch7_private')]).inspect()
    return {k:b[k] for k in ('reserved','cap','remaining','pending')}


def main():
    assert not OUT.exists(), 'Preserve prior capture; do not rerun or overwrite.'
    OUT.mkdir()
    tree = read(TREE); assert tree['sha'] == REV and not tree.get('truncated',False)
    blobs = [x for x in tree['tree'] if x['type']=='blob']
    selected = [x for x in blobs if not x['path'].startswith('docs/imgs/')]
    assert len(blobs)==35 and sum(x['size'] for x in selected)<2_000_000
    freeze = ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json'
    frozen = read(freeze)['code_files']
    assert all(digest(CANON/p)==s for p,s in frozen.items())
    before = runtime(); b = budget(); assert not b['pending']
    save(OUT/'protocol.json', {'created_utc':datetime.now(timezone.utc).isoformat(),
        'script_sha256':digest(Path(__file__)), 'source_urls':SOURCES,
        'repo_revision':REV,'tree_sha256':digest(TREE),'selected_paths':[x['path'] for x in selected],
        'excluded_paths':[x['path'] for x in blobs if x not in selected],
        'maximum_bytes_per_source':10_000_000,'http_attempts_per_url':1,
        'no_author_code_execution':True,'no_weights_training_model_db_or_private_network':True,
        'freeze_sha256':digest(freeze),'canonical_files':frozen,'runtime_before':before,'budget_before':b})
    receipts=[]; inventory=[]
    session=requests.Session()
    session.headers['User-Agent']='T1-primary-source-audit/1.0'

    def capture(name,url,expected=None):
        item={'name':name,'url':url,'requested_utc':datetime.now(timezone.utc).isoformat()}
        receipts.append(item)
        try:
            with session.get(url,timeout=(10,45),stream=True) as response:
                item.update(status=response.status_code,final_url=response.url)
                response.raise_for_status(); data=bytearray()
                for block in response.iter_content(65536):
                    data.extend(block)
                    if len(data)>10_000_000: raise ValueError('Source size bound exceeded')
            if expected:
                h=hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()
                assert h==expected, 'Pinned Git blob mismatch'
                item['git_blob_verified']=h
            p=OUT/'raw'/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
            item.update(bytes=len(data),sha256=digest(p))
            if p.suffix=='.pdf':
                with fitz.open(p) as doc:
                    pages=[page.get_text() for page in doc]
                    links=[{'page':i+1,'uri':link['uri']} for i,page in enumerate(doc)
                           for link in page.get_links() if 'uri' in link]
                save(OUT/'derived'/(name+'.pages.json'),pages)
                save(OUT/'derived'/(name+'.links.json'),links)
                item['pages']=len(pages)
            elif p.suffix=='.html':
                soup=BeautifulSoup(bytes(data),'html.parser')
                save(OUT/'derived'/(name+'.links.json'),
                     [{'text':a.get_text(' ',strip=True),'href':a['href']} for a in soup.find_all('a',href=True)])
                (OUT/'derived'/(name+'.txt')).write_text(soup.get_text('\n',strip=True),encoding='utf-8')
            if name.startswith('generator/'):
                text=bytes(data).decode('utf-8-sig')
                row={'path':name.removeprefix('generator/'),'bytes':len(data),
                     'sha256':digest(p),'urls':sorted(set(re.findall(r'https?://[^\s<>\"\)]+',text)))}
                if p.suffix=='.py':
                    module=ast.parse(text,filename=name)
                    row['static_ast_parse']=True
                    row['top_level_definitions']=[node.name for node in module.body
                        if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef))]
                inventory.append(row)
        except Exception as e:
            item['error_class']=type(e).__name__
        save(OUT/'receipts'/(str(len(receipts)).zfill(2)+'.json'),item)

    for name,url in SOURCES.items(): capture(name,url)
    for entry in selected:
        capture('generator/'+entry['path'],
            'https://raw.githubusercontent.com/interact-erc/Text-To-Cypher-knowledge-graph-data-generation/'+REV+'/'+entry['path'],entry['sha'])
    save(OUT/'source_receipts.json',receipts)
    save(OUT/'generator_static_inventory.json',inventory)
    after=runtime(); b2=budget()
    assert before==after and b==b2 and all(digest(CANON/p)==s for p,s in frozen.items())
    failures=[x['name'] for x in receipts if 'error_class' in x]
    result={'planned_sources':len(SOURCES)+len(selected),'dispatched_http':len(receipts),
        'captured_sources':len(receipts)-len(failures),'failed_sources':failures,
        'undispatched_sources':0,'captured_bytes':sum(x.get('bytes',0) for x in receipts),
        'generator_blobs_total':len(blobs),'generator_text_blobs_selected':len(selected),
        'generator_python_static_parsed':sum(x.get('static_ast_parse',False) for x in inventory),
        'budget_before':b,'budget_after':b2,'canonical_unchanged':len(frozen),
        'runtime_unchanged':len(before),'model_db_ssh_private_service_requests':0,
        'new_formal_predictions':0,'entire_T1_complete':False}
    save(OUT/'capture_summary.json',result)
    print(json.dumps(result,ensure_ascii=False))


if __name__=='__main__':main()
