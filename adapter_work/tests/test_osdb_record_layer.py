import csv
import hashlib
import json
import sys
from pathlib import Path
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from osdb_record_layer import FIELDS, records


def test_duplicate_and_unresolved_records_are_retained_without_entity_claims(tmp_path):
    p=tmp_path/'raw.csv'
    row={key:'' for key in FIELDS}
    row.update(event_type='GollumEvent',src_entity_type='Gollum',tar_entity_id='A_1')
    with p.open('w',newline='',encoding='utf8') as f:
        w=csv.DictWriter(f,fieldnames=FIELDS);w.writeheader();w.writerows([row,row])
    result=list(records(p,hashlib.sha256(p.read_bytes()).hexdigest()))
    assert len(result)==2 and result[0]['node_uid']!=result[1]['node_uid']
    props=json.loads(result[0]['properties_json'])
    assert json.loads(props['raw_record_json'])==row
    assert result[0]['label']=='OSDBRawRecord' and 'entity_id' not in props


def test_source_change_is_rejected(tmp_path):
    p=tmp_path/'raw.csv';p.write_text('changed')
    with pytest.raises(ValueError,match='SHA mismatch'):
        list(records(p,'0'*64))
