"""Regression witnesses from real smoke: aliases, write rejection, sealed score."""

import sys
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [
    str(ROOT / "experiment-harness/public_benchmark_v1"),
    str(ROOT / "adapter_work"),
]
from public_read_guard import read_query, ReadOnlyViolation
from offline_backends import SelectionBackend
from offline_pipeline import save


def test_temporal_alias_is_not_a_start_command():
    query = "WITH datetime('2023-07-01') AS start RETURN start.epochSeconds"
    assert read_query(query) == query
    carried = "WITH datetime('2023-07-01') AS start WITH start RETURN start.epochSeconds"
    assert read_query(carried) == carried
    with pytest.raises(ReadOnlyViolation):
        read_query('WITH 1 AS x CREATE (n) RETURN n')
    for query in (
        "CREATE (n) RETURN n",
        "MATCH (n) SET n.x=1 RETURN n",
        "MATCH (n) DELETE n RETURN n",
    ):
        with pytest.raises(ReadOnlyViolation):
            read_query(query)


def test_osdb_score_reads_pinned_reference_after_final(tmp_path):
    reference = save(
        tmp_path / "refs.json", [dict(task_id="x", numeric_reference_rows=[[3]])]
    )
    backend = SelectionBackend("OSDB-STKG", dict(reference=reference), None)
    answer = dict(status="EXECUTED", complete=True, columns=["n"], rows=[[3]])
    receipt = save(tmp_path / "final.json", answer)
    assert backend.score(dict(task_id="x"), answer, receipt)["value"] is True
    (tmp_path / "final.json").write_text("{}")
    from engine.errors import IntegrityError

    with pytest.raises(IntegrityError):
        backend.score(dict(task_id="x"), answer, receipt)

@pytest.mark.parametrize('backend', ['neo4j','tugraph-4.5.2'])
def test_native_count_reuses_each_backend_identity_contract(backend):
    import schema_services
    from osdb_native_set_count_control_dev import compile_native_set_count
    native = backend == 'tugraph-4.5.2'
    schema = dict(entities=[dict(label='Person', properties={'key':'str'}, identity_property='key')], relations=[])
    call = dict(nodes=[dict(id='n',label='Person')],edges=[],where=[],
                returns=[dict(field='n.key' if native else 'n.eid',alias='id')])
    plan = dict(status='ANSWER',calls=[call,call],combine=dict(op='intersection',key='id',output='count'))
    compiled = compile_native_set_count(plan,schema,
        selection_compiler=lambda c,s:schema_services.compile_selection(c,s,backend=backend),
        set_contract=schema_services._set_contract,identity_kind=None if native else 'str')
    assert compiled['identity_contract']==dict(entity_type='Person',kind='str')
    assert 't1SourceCount = 2' in compiled['query']
