import json

import run_with_transport_recovery as r


def test_completed_bad_format_is_never_retried(tmp_path, monkeypatch):
    monkeypatch.setattr(r, 'SOURCE', None)
    calls = []
    def invalid(self, stage, *args):
        calls.append(stage)
        f = self.out / 'calls' / stage
        f.mkdir(parents=True)
        (f / 'request.json').write_text('{}')
        (f / 'response.json').write_text(json.dumps({'http_status': 200, 'body': {'choices': []}}))
        raise ValueError('Bad model JSON')
    monkeypatch.setattr(r.b.Client, 'call', invalid)
    c = r.RecoveryClient({}, tmp_path)
    a = c.call('s', 'prompt', {}, ['X'], {'id', 'status', 'rows', 'evidence_ids'})
    assert len(calls) == 1
    assert a[0]['status'] == 'ERROR'


def test_http_recovery_selects_first_success_and_retains_failed_attempt(tmp_path, monkeypatch):
    monkeypatch.setattr(r, 'SOURCE', None)
    monkeypatch.setattr(r.time, 'sleep', lambda seconds: None)
    calls = []
    answer = [{'id': 'X', 'status': 'ANSWER', 'rows': [[17]], 'evidence_ids': ['E']}]
    def intermittent(self, stage, *args):
        calls.append(stage)
        f = self.out / 'calls' / stage
        f.mkdir(parents=True)
        (f / 'request.json').write_text('{}')
        status = 500 if len(calls) == 1 else 200
        (f / 'response.json').write_text(json.dumps({'http_status': status, 'body': {}}))
        if status == 500:
            raise RuntimeError('HTTP failed')
        return answer
    monkeypatch.setattr(r.b.Client, 'call', intermittent)
    c = r.RecoveryClient({}, tmp_path)
    a = c.call('s', 'prompt', {}, ['X'], {'id', 'status', 'rows', 'evidence_ids'})
    assert a == answer  # Preserves 17 even if an external gold would differ.
    assert len(calls) == 2
    assert len(list((tmp_path / 'attempts').rglob('response.json'))) == 2
    assert json.loads((tmp_path / 'calls/s/selection.json').read_text())['selected_attempt'] == 2
