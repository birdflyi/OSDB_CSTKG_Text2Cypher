import pytest
import hashlib
from adapter_work.cyquark_training import author_tsv_rows
from adapter_work.cyquark_training import training_options


def test_legacy_recipe_is_unchanged():
    assert training_options({'epochs': 1}) == {'optim': 'adamw_torch', 'lr_scheduler_type': 'constant'}
    with pytest.raises(ValueError):
        training_options({'epochs': 4})


def test_explicit_settings_and_epoch_exception():
    recipe = dict(training_profile='explicit_v2', settings_evidence_sha256='a'*64,
                  epochs=4, optimizer='adafactor', lr_scheduler_type='cosine')
    assert training_options(recipe) == {'optim': 'adafactor', 'lr_scheduler_type': 'cosine'}
    del recipe['settings_evidence_sha256']
    with pytest.raises(ValueError, match='evidence'):
        training_options(recipe)


@pytest.mark.parametrize('patch', [{'epochs': 0}, {'optimizer': 'unknown'},
                                    {'lr_scheduler_type': None}, {'training_profile': 'typo'}])
def test_invalid_explicit_settings_rejected(patch):
    recipe = dict(training_profile='explicit_v2', settings_evidence_sha256='a'*64,
                  epochs=1, optimizer='adafactor', lr_scheduler_type='cosine')
    recipe.update(patch)
    with pytest.raises(ValueError):
        training_options(recipe)


def test_author_import_preserves_provenance_and_excludes_heldout(tmp_path):
    path = tmp_path / 'supplied.tsv'
    path.write_text('X\tY\nQuestion one?\tRETURN 1\nQuestion two?\tRETURN 2\n', encoding='utf8')
    source = dict(benchmark='b', graph='g', archive_sha256='a'*64,
                  member_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                  member_path='g/supplied.tsv', permission_evidence_sha256='b'*64)
    rows, rejected = author_tsv_rows(path, benchmark='b', graph='g', source=source,
                                    heldout_questions=['QUESTION TWO?'])
    assert len(rows) == len(rejected) == 1
    assert rows[0]['messages'] == [{'role': 'user', 'content': 'Question one?'}]
    assert rows[0]['cypher'] == 'RETURN 1'
    assert rows[0]['provenance']['kind'] == 'author_supplied'
    assert 'logical_filter_passed' not in rows[0]
    with pytest.raises(ValueError, match='binding'):
        author_tsv_rows(path, benchmark='b', graph='other', source=source)
    path.write_text('X\tY\nchanged\tRETURN 1\n', encoding='utf8')
    with pytest.raises(ValueError, match='Changed'):
        author_tsv_rows(path, benchmark='b', graph='g', source=source)


@pytest.mark.parametrize('content', ['A\tB\nx\ty\n', 'X\tY\nx\t\n', 'X\tY\nx\ty\tz\n'])
def test_author_import_rejects_invalid_columns_and_records(tmp_path, content):
    path = tmp_path / 'bad.tsv'
    path.write_text(content, encoding='utf8')
    source = dict(benchmark='b', graph='g', archive_sha256='a'*64,
                  member_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                  member_path='bad.tsv', permission_evidence_sha256='b'*64)
    with pytest.raises(ValueError):
        author_tsv_rows(path, benchmark='b', graph='g', source=source)
