"""Freeze two candidates by graph features, never model performance."""
import argparse
import json
import math
from pathlib import Path

import run_baselines as b


def distribution(counts):
    total=sum(counts.values())
    return {k:v/total for k,v in counts.items()} if total else {}


def js_divergence(left,right):
    keys=set(left)|set(right)
    result=0.0
    for key in keys:
        p,q=left.get(key,0),right.get(key,0)
        m=(p+q)/2
        if p:
            result+=.5*p*math.log2(p/m)
        if q:
            result+=.5*q*math.log2(q/m)
    return result


def select(profiles):
    baseline={}
    for name in ('redis','duckdb'):
        d=distribution(profiles[name]['features']['service_counts'])
        for k,v in d.items():
            baseline[k]=baseline.get(k,0)+v/2
    eligible={}
    rejected={}
    for name in ('gridgain','tdengine','iotdb','elasticsearch'):
        p=profiles[name]
        f,source=p['features'],p['provenance']
        quarantine=source['quarantined_records']/source['raw_records']
        time_known=sum(f['time_present'].values())/f['edges'] if f['edges'] else 0
        if f['nodes']<1000 or f['edges']<5000 or quarantine>.05 or time_known<.95:
            rejected[name]={'nodes':f['nodes'],'edges':f['edges'],'quarantine_fraction':quarantine,'time_known_fraction':time_known}
        else:
            eligible[name]={'nodes':f['nodes'],'edges':f['edges'],'reference_edges':f['reference_edges'],
                            'typed_signatures':f['typed_signatures'],'quarantine_fraction':quarantine,
                            'time_known_fraction':time_known,'service_distribution_js':js_divergence(distribution(f['service_counts']),baseline)}
    if len(eligible)<2:
        raise ValueError('Not enough qualified candidates; do not force two projects')
    scale=min(eligible,key=lambda k:(-eligible[k]['edges'],k))
    contrast=min((k for k in eligible if k!=scale),key=lambda k:(-eligible[k]['service_distribution_js'],k))
    return {'mode':'E4_FEATURE_SELECTED_BEFORE_HOLDOUT_PREDICTIONS','scale_project':scale,'distribution_contrast_project':contrast,
            'selection_rule':'Quality gates: >=1000 nodes, >=5000 edges, <=5% quarantined rows, >=95% edge event-time presence. Then maximum total edges; among remaining maximum JS divergence of service frequency from equally weighted Redis/DuckDB baseline. Lexicographic ties.',
            'eligible':eligible,'rejected':rejected,'holdout_predictions_seen':0,
            'limitations':'Convenience candidate pool from one 2023 dataset and same projection/schema. Graph selection is observational. Reference count and task-subgraph statistics are reported separately; total size is not automatically query difficulty.'}


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--profiles',required=True)
    p.add_argument('--output',required=True)
    a=p.parse_args()
    root=Path(a.profiles)
    profiles={name:json.loads((root/(name+'.json')).read_text(encoding='utf-8')) for name in ('redis','duckdb','gridgain','tdengine','iotdb','elasticsearch')}
    result=select(profiles)
    result['profile_hashes']={name:b.sha(root/(name+'.json')) for name in profiles}
    result['selector_sha256']=b.sha(__file__)
    b.save(Path(a.output),result)
    print(json.dumps(result,indent=2))
