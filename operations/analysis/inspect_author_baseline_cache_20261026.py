"""Offline HTML parser repair and independent receipt verification. No network."""
import ast
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urljoin, urlsplit

import fitz
from bs4 import BeautifulSoup

OUT=Path('D:/ch7/reports/author_baseline_contracts_20261026')
def read(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def write(p,v):
    p.parent.mkdir(parents=True,exist_ok=True)
    with p.open('x',encoding='utf-8',newline='\n') as f:
        json.dump(v,f,ensure_ascii=False,indent=2);f.write('\n')


def main():
    receipts=read(OUT/'source_receipts.json')
    assert len(receipts)==28 and all(x['status']==200 for x in receipts)
    assert all(sha(OUT/'raw'/x['name'])==x['sha256'] for x in receipts)
    assert all(x.get('error_class') in (None,'TypeError') for x in receipts)
    assert {x['name'] for x in receipts if x.get('error_class')}=={
        'ri_official.html','smith_official.html','cyquark_v1.html'}
    write(OUT/'offline_repair_protocol.json',{'script_sha256':sha(Path(__file__)),
        'original_receipts_sha256':sha(OUT/'source_receipts.json'),
        'original_parse_error':'BeautifulSoup received bytearray instead of bytes',
        'repair_scope':'HTML cache only; no HTTP retries; original errors preserved'})
    links={}; page_counts={}
    for entry in receipts:
        p=OUT/'raw'/entry['name']
        if p.suffix=='.html':
            soup=BeautifulSoup(p.read_bytes(),'html.parser')
            ls=[{'text':a.get_text(' ',strip=True),'url':urljoin(entry['url'],a['href'])}
                for a in soup.find_all('a',href=True)]
            links[entry['name']]=ls
            write(OUT/'derived'/(entry['name']+'.repaired.links.json'),ls)
            (OUT/'derived'/(entry['name']+'.repaired.txt')).write_text(soup.get_text('\n',strip=True),encoding='utf-8')
        if p.suffix=='.pdf':
            with fitz.open(p) as doc:
                page_counts[entry['name']]=len(doc)
                # Preserve physical PDF page numbering; PDF page labels/web parser differ.
                pages=(4,20,21) if entry['name'].startswith('ri') else (4,5,12,13)
                for i in pages:
                    doc[i].get_pixmap(matrix=fitz.Matrix(1.2,1.2)).save(
                        OUT/'derived'/(entry['name']+'.page'+str(i+1)+'.png'))
    # Full selected text corpus only. These are candidate strings, not author-identity proof.
    inventory=read(OUT/'generator_static_inventory.json')
    assert len(inventory)==23 and sum(x.get('static_ast_parse',False) for x in inventory)==11
    matches=[]
    for entry in inventory:
        path=OUT/'raw/generator'/entry['path']
        assert sha(path)==entry['sha256']
        for n,line in enumerate(path.read_text(encoding='utf-8-sig').splitlines(),1):
            if re.search(r'checkpoint|huggingface|from_pretrained|transformers|vllm|torch|evaluate|predict|finetun|fine.tun',line,re.I):
                matches.append({'path':entry['path'],'line':n,'text':line})
    write(OUT/'generator_model_resource_string_candidates.json',matches)
    write(OUT/'independent_capture_verification.json',{
        'all_28_http_200_and_raw_sha_verified':True,'raw_bytes':sum(x['bytes'] for x in receipts),
        'html_local_parse_failures_original':3,'html_local_parse_repairs':3,
        'source_http_failures':0,'new_http_requests_for_repair':0,
        'git_blobs_verified':sum('git_blob_verified' in x for x in receipts),
        'static_python_modules':11,'pdf_physical_pages':page_counts,
        'model_resource_candidate_string_matches':len(matches),
        'author_code_executed':False,'new_model_predictions':0})
    print(json.dumps({'raw_sources':len(receipts),'html_repaired':3,
        'model_resource_candidate_string_matches':matches,
        'primary_page_resource_links':{k:[x for x in v if re.search(
            r'github|huggingface|supplement|software|code|data|pdf',x['url'],re.I)]
            for k,v in links.items()}},ensure_ascii=True))


if __name__=='__main__':main()
