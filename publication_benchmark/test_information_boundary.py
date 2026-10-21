import json

from information_boundary_witness import build


def test_identical_service_observations_do_not_determine_monthly_openings(tmp_path):
    root=tmp_path/'worlds'
    build(root)
    a=json.loads((root/'A'/'oracle_observations.json').read_text(encoding='utf-8'))
    b=json.loads((root/'B'/'oracle_observations.json').read_text(encoding='utf-8'))
    assert a['service_rows']==b['service_rows']
    assert a['target_rows']==[[1]] and b['target_rows']==[[0]]
    assert len(a['calls'])==67
