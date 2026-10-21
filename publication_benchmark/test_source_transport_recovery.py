import json
from pathlib import Path
import pytest
from run_source_transport_recovery import infrastructure,source_calls


def test_transport_status_not_answer_quality():
    assert infrastructure({'http_status':500})
    assert infrastructure({'http_status':429})
    assert infrastructure({'transport_error':'Timeout'})
    assert not infrastructure({'http_status':200,'body':{'answer':'wrong'}})
    assert not infrastructure({'http_status':200,'body':{'choices':[{'finish_reason':'length'}]}})
    assert not infrastructure({'http_status':400})


def test_reuse_wrong_refused_or_incomplete_responses(tmp_path):
    for name,terminal in [('refused',{'http_status':200,'body':{'status':'UNSUPPORTED'}}),
        ('wrong',{'http_status':200,'body':{'wrong':True}}),('failed',{'http_status':500})]:
        folder=tmp_path/'calls'/name;folder.mkdir(parents=True)
        (folder/'request.json').write_text('{}')
        (folder/'terminal.json').write_text(json.dumps(terminal))
    reuse,failed=source_calls(tmp_path)
    assert {p.name for p in reuse}=={'refused','wrong'}
    assert [p.name for p in failed]==['failed']


def test_unrecorded_attempt_cannot_be_redispatched(tmp_path):
    folder=tmp_path/'calls'/'pending';folder.mkdir(parents=True)
    (folder/'request.json').write_text('{}')
    with pytest.raises(RuntimeError,match='reconciliation'):source_calls(tmp_path)
