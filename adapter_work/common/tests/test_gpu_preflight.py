import hashlib
import json
import pytest
from adapter_work.gpu_preflight import inspect_model


def test_model_intake_checks_metadata_and_shards_without_loading(tmp_path):
    config = tmp_path / 'config.json'
    config.write_text('{}')
    (tmp_path / 'model.safetensors.index.json').write_text(json.dumps({'weight_map': {'x': 'part.safetensors'}}))
    spec = dict(name='fixture', path=str(tmp_path), file_sha256={'config.json': hashlib.sha256(config.read_bytes()).hexdigest()})
    assert inspect_model(spec)['problems'] == ['Missing or empty weight shard: part.safetensors']
    (tmp_path / 'part.safetensors').write_bytes(b'fixture-not-real-weights')
    report = inspect_model(spec)
    assert not report['problems'] and not report['weight_content_verified']
    config.write_text('{"changed":true}')
    assert inspect_model(spec)['problems'] == ['Missing or changed file: config.json']


def test_model_intake_rejects_path_escape(tmp_path):
    spec = dict(name='fixture', path=str(tmp_path), file_sha256={'../outside': 'a'*64})
    with pytest.raises(ValueError, match='within'):
        inspect_model(spec)
