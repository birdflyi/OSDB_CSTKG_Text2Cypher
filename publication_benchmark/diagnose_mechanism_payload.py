"""Separate protocol envelope faults from as-written service-call payloads.

Diagnostic only. Never supplies missing status/ID or changes the primary score.
"""
import argparse
import json
from pathlib import Path
import re

import run_baselines as b
from mechanism_core import compile_call


def run(folder, output):
    folder = Path(folder)
    specs = {s['id']: s for s in json.loads((folder / 'private_specifications.json').read_text(encoding='utf-8'))}
    records = []
    for f in sorted((folder / 'records').glob('C_*.json')):
        record = json.loads(f.read_text(encoding='utf-8'))
        response = folder / 'calls' / f.stem / 'response.json'
        x = {'id': record['id'], 'strict_result_correct': record['result_correct'], 'failure': record['failure'],
             'core_evaluable': False, 'core_call_valid': False, 'core_call_exact_reference': False}
        if response.exists():
            r = json.loads(response.read_text(encoding='utf-8'))
            choices = r.get('body', {}).get('choices') or []
            if r['http_status'] == 200 and choices and choices[0].get('finish_reason') == 'stop':
                try:
                    text = choices[0]['message']['content'].strip()
                    if '```' in text:
                        blocks = re.findall(r'```(?:json)?\s*([\s\S]*?)\s*```', text)
                        if len(blocks) != 1:
                            raise ValueError('Ambiguous block')
                        text = blocks[0]
                    value = json.loads(text)
                    if isinstance(value, list):
                        if len(value) != 1:
                            raise ValueError('Not singleton')
                        value = value[0]
                    if not isinstance(value, dict):
                        raise ValueError('Not object')
                    x.update(missing_id='id' not in value, missing_status='status' not in value,
                             raw_model_id=value.get('id'), raw_model_status=value.get('status'))
                    if {'operation', 'arguments'} <= set(value):
                        call = {k: value[k] for k in ('operation', 'arguments')}
                        x['core_evaluable'] = True
                        x['core_call_exact_reference'] = call == specs[record['id']]['reference_call']
                        try:
                            compile_call(call)
                            x['core_call_valid'] = True
                        except (ValueError, TypeError, KeyError):
                            pass
                except (ValueError, KeyError, TypeError):
                    pass
        records.append(x)
    result = {'mode': 'CORE_PAYLOAD_DIAGNOSTIC_NOT_END_TO_END_SUCCESS', 'service_cases': len(records),
        'core_evaluable': sum(x['core_evaluable'] for x in records),
        'core_calls_valid': sum(x['core_call_valid'] for x in records),
        'core_calls_exact_reference': sum(x['core_call_exact_reference'] for x in records),
        'strict_service_results_correct': sum(x['strict_result_correct'] for x in records),
        'records': records, 'code_sha256': b.sha(__file__),
        'limits': 'Uses reference calls only after prediction. Does not fill envelope fields, execute missing-status responses, or change primary scores.'}
    b.save(Path(output), result)
    print(json.dumps({k: v for k, v in result.items() if k != 'records'}, ensure_ascii=False))


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--run', required=True)
    p.add_argument('--output', required=True)
    a = p.parse_args()
    run(a.run, a.output)
