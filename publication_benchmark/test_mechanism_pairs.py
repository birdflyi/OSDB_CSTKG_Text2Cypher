import pytest

from analyze_mechanism_pairs import analyze, ARMS


def fixture():
    specs = [{'id': str(i), 'group': 'g', 'family': 'f', 'variant_index': i,
              'expected_rows': [[1 if i < 4 else i]]} for i in range(6)]
    records = [{'id': s['id'], 'arm': arm, 'correct': False,
                'observation': {'execution': {'status': 'EXECUTED', 'columns': ['n'], 'rows': [[999]]}}}
               for arm in ARMS for s in specs]
    return specs, records


def test_identical_wrong_answers_are_not_robust_success():
    specs, records = fixture()
    result = analyze(specs, records)
    assert result['arms']['G0']['equivalent_groups_all_correct'] == 0
    assert result['arms']['G0']['equivalent_groups_identical_but_wrong'] == 1
    assert result['arms']['G0']['contrast_pairs_both_correct'] == 0


def test_missing_or_duplicate_records_are_rejected():
    specs, records = fixture()
    with pytest.raises(ValueError):
        analyze(specs, records[:-1])
    with pytest.raises(ValueError):
        analyze(specs, records + records[:1])


def test_non_discriminating_contrast_is_rejected():
    specs, records = fixture()
    specs[-1]['expected_rows'] = [[1]]
    with pytest.raises(ValueError, match='Contrast'):
        analyze(specs, records)
