"""Bounded public metadata extension; no model weights, install or execution.

Three new primary-source documents once each; preserve failures, no retry.
Existing publisher/anonymous-HF evidence is reused with hashes, not re-fetched.
"""
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin
import hashlib,json,urllib.request
from bs4 import BeautifulSoup

ROOT=Path('D:/ch7');OUT=ROOT/'reports/author_resource_extension_v1_20261027'
URLS={
 'smith_pypi_0_2_0.json':'https://pypi.org/pypi/cyphersmith/0.2.0/json',
 'ri_coauthor_homepage.html':'https://zwchai.com/',
 'smith_coauthor_publications.html':'https://ix.cs.uoregon.edu/~thien/publication.html',
}
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def write(p,v):
 with Path(p).open('x',encoding='utf-8',newline='\n') as f:json.dump(v,f,ensure_ascii=False,indent=2);f.write('\n')

def main():
 OUT.mkdir(exist_ok=False)
 old=ROOT/'reports/author_baseline_contracts_20261026/raw/smith_official.html'
 hf=ROOT/'reports/baseline_resource_inventory_20261024_v2/hf_ri_detail.json'
 reused={str(p):sha(p) for p in (old,hf)}
 write(OUT/'protocol.json',{'kind':'PUBLIC_METADATA_EXTENSION_NOT_AUTHOR_MODEL_REPRODUCTION',
  'created_local':datetime.now().astimezone().isoformat(),'source_sha256':sha(__file__),
  'planned_new_documents':URLS,'reused_cache_sha256':reused,
  'limits':['Exactly one bounded GET per new URL, timeout20s, max2MiB, no automatic retry.',
   'Do not install/import the similarly named package or download weights.',
   'Names and third-party quantization/provider availability do not prove author provenance.',
   'No email/issue/message to authors; user already reported no existing author files.',
   'Public search absence is not global resource nonexistence.']})
 receipts=[]
 for name,url in URLS.items():
  rec={'url':url,'file':name,'retrieved_local':datetime.now().astimezone().isoformat()}
  try:
   req=urllib.request.Request(url,headers={'User-Agent':'T1-public-resource-metadata-audit'})
   with urllib.request.urlopen(req,timeout=20) as response:
    raw=response.read(2*1024**2+1);assert len(raw)<=2*1024**2 and response.status==200
    rec.update(status=response.status,final_url=response.geturl(),content_type=response.headers.get('Content-Type'))
   (OUT/name).write_bytes(raw);rec.update(bytes=len(raw),sha256=sha(OUT/name),state='CAPTURED')
  except Exception as e:rec.update(state='FETCH_FAILED_NO_RETRY',error_type=type(e).__name__,error=str(e)[:250])
  receipts.append(rec)
 write(OUT/'receipts.json',receipts)
 smith=BeautifulSoup(old.read_bytes(),'html.parser')
 paper_authors=[m.get('content') for m in smith.select('meta[name="citation_author"]')]
 if not paper_authors:
  paper_authors=['Zeyu Zhang','Kexuan Sun','Zheng Tang','Jens-S. Vöckler','Thien Huu Nguyen','Thuy Vu']
  author_extraction='Manually reviewed cached publisher author list'
 else:author_extraction='citation_author publisher metadata'
 findings={'paper_author_list':paper_authors,'paper_author_extraction':author_extraction,
  'paper_url':'https://aclanthology.org/2026.acl-long.1601/','PyPI_identity_verified_as_paper_method':False}
 if (OUT/'smith_pypi_0_2_0.json').exists():
  p=read(OUT/'smith_pypi_0_2_0.json')['info'];assert p['name']=='cyphersmith' and p['version']=='0.2.0'
  findings['same_name_package']={'name':p['name'],'version':p['version'],'declared_author':p.get('author'),
   'project_urls':p.get('project_urls'),'requires_python':p.get('requires_python'),
   'disposition':'NOT_ADMITTED_AS_ACL_PAPER_IMPLEMENTATION',
   'reason':'Package metadata names a different author and describes provider-neutral generation/validation. No author association has been established; same name is insufficient.'}
 homepage=[]
 for name,term in [('ri_coauthor_homepage.html','Cypher-RI'),('smith_coauthor_publications.html','CypherSmith')]:
  p=OUT/name
  if not p.exists():continue
  soup=BeautifulSoup(p.read_bytes(),'html.parser');node=soup.find(string=lambda s:s and term in s)
  if node is None:homepage.append({'file':name,'paper_found':False});continue
  # Local paper container only; other publications' code links are irrelevant.
  li=node.find_parent('li');section=li or node.parent
  links=[{'label':a.get_text(' ',strip=True),'url':urljoin(URLS[name],a['href'])} for a in section.find_all('a',href=True)]
  homepage.append({'file':name,'paper_found':True,'paper_container':section.name,
   'paper_container_text':section.get_text(' ',strip=True)[:1700],'paper_container_links':links,
   'exact_author_checkpoint_or_controller_established':False})
 write(OUT/'identity_findings.json',findings);write(OUT/'coauthor_paper_links.json',homepage)
 oldhf=read(hf)
 assert all(sha(p)==h for p,h in reused.items())
 summary={'kind':'AUTHOR_RESOURCE_EXTENSION_AND_NAME_COLLISION_NOT_REPRODUCTION',
  'planned_source_GETs':len(URLS),'dispatched_source_GETs':len(receipts),
  'captured_sources':sum(r['state']=='CAPTURED' for r in receipts),
  'failed_sources':sum(r['state']!='CAPTURED' for r in receipts),'undispatched':0,
  'new_checkpoint_identity_certifications':0,'new_inference_controllers_obtained':0,
  'smith_same_name_PyPI_package_is_not_certified_author_code':True,
  'smith_package_declared_author':findings.get('same_name_package',{}).get('declared_author'),
  'RI_old_cached_candidate_not_newly_authenticated':{'model_id':oldhf['id'],'revision':oldhf['sha'],
   'author_provenance_confirmed':False,'new_weights_downloaded':0},
  'coauthor_pages_captured':len(homepage),'new_model_calls':0,'new_DB_calls':0,
  'package_installs':0,'author_code_executed':False,'author_contacted':False,
  'new_named_method_formal_N':None,'model_dispatched':0,'model_completed':0,'model_failed':0,'model_undispatched':None,
  'remaining':['RI author-associated exact checkpoint plus schema-selection/continuation controller.',
   'Smith final parser/checkpoint, training data version and2488-versus2348 task mapping.',
   'CYQUARK target-graph/seed checkpoint or outputs, exact generation/training/inference contract.'],
  'no_global_resource_absence_claim':True,'T1_complete':False,
  'artifacts_sha256':{p.name:sha(p) for p in OUT.glob('*.json')}}
 write(OUT/'summary.json',summary);print(json.dumps(summary,ensure_ascii=True))

if __name__=='__main__':main()
