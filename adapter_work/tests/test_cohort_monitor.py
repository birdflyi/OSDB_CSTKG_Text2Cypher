import json
from adapter_work.cohort_monitor import Journal


def test_partial_line_and_duplicate_final(tmp_path):
    path = tmp_path / 'journal.jsonl'
    event = {'kind': 'send_started', 'request_id': 'r', 't': 1}
    path.write_text(json.dumps(event), encoding='utf-8')
    journal = Journal(path)
    journal.update()
    assert not journal.active
    with path.open('a', encoding='utf-8') as stream:
        stream.write('\n')
    journal.update()
    assert 'r' in journal.active
    final = {'kind': 'path_final_saved', 'logical_key': 'm__b__q__direct__initial'}
    with path.open('a', encoding='utf-8') as stream:
        for event in ({'kind': 'attempt_terminal', 'request_id': 'r', 'status': 'unknown'}, final, final):
            stream.write(json.dumps(event) + '\n')
    journal.update()
    assert not journal.active
    assert journal.statuses == {'unknown': 1}
    assert journal.cells == {'b / direct': 1}
    journal.update()
    assert journal.cells == {'b / direct': 1}
