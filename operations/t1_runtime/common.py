"""Small atomic snapshots and literal metric helpers; no model dependency."""
import json
import math
import os
import time
from pathlib import Path


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def atomic(path, value):
    path = Path(path)
    tmp = path.with_name(path.name + '.writing')
    path.parent.mkdir(parents=True, exist_ok=True)
    with tmp.open('w', encoding='utf-8', newline='\n') as stream:
        json.dump(value, stream, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
        stream.flush()
    os.replace(tmp, path)


def immutable(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', encoding='utf-8', newline='\n') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write('\n')


def percentile(values, p):
    rows = sorted(values)
    if not rows: return None
    at = (len(rows)-1)*p; lower = math.floor(at); upper = math.ceil(at)
    return rows[lower] + (rows[upper]-rows[lower])*(at-lower)


def distribution(values):
    return {'n': len(values), 'mean': sum(values)/len(values) if values else None,
            'p50': percentile(values,.5), 'p90': percentile(values,.9),
            'p99': percentile(values,.99)}


def valid_token(value):
    return type(value) is int and value >= 0


def rates(events, now=None, since=None):
    now = time.time() if now is None else now
    result = {}
    for span in (60, 300):
        rows = [r for r in events if now-span < r['ended_epoch'] <= now]
        known = [r['completion_tokens'] for r in rows if valid_token(r.get('completion_tokens'))]
        exposure=max(1.,min(span,now-since)) if since is not None else span
        result[str(span)] = {'completed': len(rows), 'per_minute': len(rows)*60/exposure,
                            'complete_responses': sum(r['complete'] for r in rows),
                            'output_tokens_per_second': sum(known)/exposure if len(known)==len(rows) else None,'window_seconds':exposure,
                            'usage_unknown': len(rows)-len(known)}
    return result
