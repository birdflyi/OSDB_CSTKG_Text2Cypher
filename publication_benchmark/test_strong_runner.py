import json
from unittest.mock import patch

import pytest

import run_strong_baselines as r


CONFIG = {'model': 'test-model', 'temperature': 0.2, 'top_p': 0.95,
          'reasoning_effort': 'low', 'endpoint': 'https://example.invalid', 'api_key': 'test-only'}


def test_unrecorded_attempt_cannot_be_automatically_repeated(tmp_path):
    folder = tmp_path / 'calls' / 'stage'
    folder.mkdir(parents=True)
    payload = {k: CONFIG[k] for k in ('model', 'temperature', 'top_p', 'reasoning_effort')}
    payload.update(max_tokens=6000, messages=[{'role': 'system', 'content': r.s.PROMPTS['G']},
                                          {'role': 'user', 'content': '{}'}])
    (folder / 'request.json').write_text(json.dumps(payload), encoding='utf-8')
    with patch.object(r.requests, 'post') as post:
        with pytest.raises(RuntimeError, match='Unrecorded'):
            r.dispatch(tmp_path, 'stage', CONFIG, 'G', {})
    post.assert_not_called()


def test_server_failure_is_retained_without_semantic_repair(tmp_path):
    class Response:
        status_code = 500
        def json(self):
            return {'error': 'test failure'}
    with patch.object(r.requests, 'post', return_value=Response()) as post:
        result = r.dispatch(tmp_path, 'stage', CONFIG, 'G', {})
    assert result['failure'] == 'INFRASTRUCTURE' and not result['repairable']
    assert not r.observe(result, 'G')['check']['repairable']
    assert post.call_count == 1
    saved = (tmp_path / 'calls' / 'stage' / 'request.json').read_text(encoding='utf-8')
    assert 'test-only' not in saved


def test_result_matching_requires_full_public_columns():
    spec = {'columns': ['count'], 'expected_rows': [[3]]}
    observation = {'execution': {'status': 'EXECUTED', 'columns': ['wrong'], 'rows': [[3]]}}
    assert not r.score(observation, spec)


def test_progress_never_claims_planned_completion_before_all_arms_finish(tmp_path):
    assert r.summary(tmp_path, [], 72, False)['status'] == 'RUNNING'
    assert r.summary(tmp_path, [], 72, True)['status'] == 'STOPPED_INFRASTRUCTURE'
