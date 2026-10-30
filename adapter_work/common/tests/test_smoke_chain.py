from pathlib import Path
import sys
from contextlib import nullcontext

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import smoke_chain
from offline_pipeline import save


def test_chain_dispatches_feedback_and_consumes_it_without_agent(tmp_path, monkeypatch):
    output = tmp_path/'chain'
    initial_dir = tmp_path/'initial'
    initial_dir.mkdir()
    calls = []
    row = dict(cell_id='M03-B01', position_id='B01-P01', status='feedback_pending')

    def consume(config):
        calls.append('consume')
        if 'selected_positions' not in config:
            save(initial_dir/'M03-B01/B01-P01/feedback_request.json', dict(position=row))
            return dict(results=[row], planned=1)
        assert config['selected_positions'] == {'M03-B01':['B01-P01']}
        assert len(config['source_finals']) == 1
        return dict(results=[dict(row, status='EXECUTED')], planned=1)

    async def generate(config):
        calls.append('generate')
        assert config['physical_attempt_cap'] == 3
        save(Path(config['run_dir'])/'finals/reply.json', dict(status='answered'))
        return dict(reserved_physical_attempts=1)

    monkeypatch.setattr(smoke_chain, 'consume', consume)
    monkeypatch.setattr(smoke_chain.request_pipeline, 'run', generate)
    monkeypatch.setattr(smoke_chain, 'scan_legacy', lambda *args:0)
    monkeypatch.setattr(smoke_chain, 'Ledger', lambda *args:nullcontext(object()))
    monkeypatch.setattr(smoke_chain, 'checkpoint_replies', lambda path,ledger:save(path,[]))
    offline = save(tmp_path/'offline.json', dict(output=str(initial_dir)))
    requests = save(tmp_path/'requests.json', dict(name='fixture',legacy_ledger='unused',ledger_dir='unused',
                    approval_path='unused',budget_scope='fixture',physical_attempt_cap=3))
    smoke_chain.run(dict(output=str(output), offline=offline, requests=requests))
    import json
    result=json.loads((output/'summary.json').read_bytes())
    assert calls == ['consume','generate','consume']
    assert result['final_counts'] == {'EXECUTED':1}
    assert result['initial']['results'][0]['status'] == 'feedback_pending'
