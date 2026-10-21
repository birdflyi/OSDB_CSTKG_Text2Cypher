import json

from diagnose_mechanism_payload import run


def test_correct_payload_without_envelope_never_becomes_primary_success(tmp_path):
    call = {'operation': 'issue_openers', 'arguments': {'issue_id': 'I_1#1'}}
    (tmp_path / 'private_specifications.json').write_text(json.dumps([{'id': 'X', 'reference_call': call}]))
    (tmp_path / 'records').mkdir()
    (tmp_path / 'records/C_case.json').write_text(json.dumps({'id': 'X', 'result_correct': False, 'failure': 'MODEL_RESPONSE_PROTOCOL'}))
    folder = tmp_path / 'calls/C_case'
    folder.mkdir(parents=True)
    (folder / 'response.json').write_text(json.dumps({'http_status': 200, 'body': {'choices': [
        {'finish_reason': 'stop', 'message': {'content': json.dumps(call)}}]}}))
    output = tmp_path / 'diagnostic.json'
    run(tmp_path, output)
    result = json.loads(output.read_text())
    assert result['core_calls_exact_reference'] == 1
    assert result['strict_service_results_correct'] == 0
    assert result['records'][0]['raw_model_status'] is None
    assert result['records'][0]['raw_model_id'] is None
