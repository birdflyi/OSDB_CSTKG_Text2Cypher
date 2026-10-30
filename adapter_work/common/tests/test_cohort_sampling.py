import hashlib
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from cohort_sampling import allocate, select_population, write_bundle


def population():
    return [dict(position_id=f'{graph}-{i}', task_id=str(i), benchmark='b', graph=graph,
                 split='test', source_group='published',
                 structure_bucket='unknown' if i == 0 else 'simple',
                 reference_answer='NEVER_EMIT', prediction_correct=i % 2 == 0)
            for graph in ('a', 'b') for i in range(20)]


def policy(**kwargs):
    return dict(mode='fixed_stratified', seed=20261030, benchmark='b',
                primary_fields=['graph'], target=12, **kwargs)


def test_seeded_selection_is_independent_of_source_order():
    a, strata = select_population(population(), policy())
    b, _ = select_population(list(reversed(population())), policy())
    assert a == b and len(a) == 12
    assert sum(s['selected'] for s in strata) == 12
    assert all(s['selected'] >= 1 for s in strata)
    assert 'NEVER_EMIT' not in json.dumps(a)
    assert all('prediction_correct' not in row for row in a)


def test_seed_changes_positions_without_changing_counts():
    a, sa = select_population(population(), policy())
    p = policy()
    p['seed'] = 10
    b, sb = select_population(population(), p)
    assert [r['position_id'] for r in a] != [r['position_id'] for r in b]
    assert sa == sb


def test_predictions_and_reference_values_do_not_select_positions():
    rows = population()
    a, _ = select_population(rows, policy())
    for row in rows:
        row['prediction_correct'] = not row['prediction_correct']
        row['reference_answer'] = 'changed'
    b, _ = select_population(rows, policy())
    assert [r['position_id'] for r in a] == [r['position_id'] for r in b]


def test_explicit_graph_quotas_and_weights():
    rows, strata = select_population(population(), policy(primary_quotas=[
        dict(values=['a'], count=4), dict(values=['b'], count=8)]))
    assert sum(r['graph'] == 'a' for r in rows) == 4
    assert sum(r['design_weight'] for r in rows) == pytest.approx(40)
    assert sum(s['population'] for s in strata) == 40


@pytest.mark.parametrize('target', [0, -1, True, 1, 41])
def test_invalid_total_rejected(target):
    p = policy()
    p['target'] = target
    with pytest.raises(ValueError):
        select_population(population(), p)


@pytest.mark.parametrize('quotas', [[], [dict(values=['a'], count=12)],
    [dict(values=['a'], count=6), dict(values=['a'], count=6)],
    [dict(values=['a'], count=30), dict(values=['b'], count=1)]])
def test_invalid_explicit_quotas_rejected(quotas):
    with pytest.raises(ValueError):
        select_population(population(), policy(primary_quotas=quotas))


def test_unfunded_structure_group_is_not_silently_dropped():
    p = policy(primary_quotas=[dict(values=['a'], count=1), dict(values=['b'], count=11)])
    with pytest.raises(ValueError, match='cover all strata'):
        select_population(population(), p)


@pytest.mark.parametrize('mutation', ['duplicate', 'benchmark', 'missing_graph', 'bucket'])
def test_bad_population_rejected(mutation):
    rows = population()
    if mutation == 'duplicate':
        rows.append(rows[0])
    elif mutation == 'benchmark':
        rows[0]['benchmark'] = 'different'
    elif mutation == 'missing_graph':
        del rows[0]['graph']
    else:
        rows[0]['structure_bucket'] = 'invented'
    with pytest.raises(ValueError):
        select_population(rows, policy())


def test_full_release_retains_every_position():
    p = policy()
    p['mode'] = 'full_release'
    rows, _ = select_population(population(), p)
    assert len(rows) == 40
    assert all(r['inclusion_probability'] == r['design_weight'] == 1 for r in rows)


def test_no_arbitrary_result_based_strata():
    p = policy()
    p['primary_fields'] = ['prediction_correct']
    with pytest.raises(ValueError):
        select_population(population(), p)


def test_allocate_capacity_and_tie_order():
    assert allocate({'b': 4, 'a': 4}, 3) == {'a': 2, 'b': 1}
    assert allocate({'a': 1, 'b': 4}, 4) == {'a': 1, 'b': 3}


def test_bundle_checks_source_and_refuses_overwrite(tmp_path):
    source, config = tmp_path / 'population.jsonl', tmp_path / 'policy.json'
    source.write_text('\n'.join(json.dumps(row) for row in population()), encoding='utf-8')
    p = policy()
    p['population_sha256'] = hashlib.sha256(source.read_bytes()).hexdigest()
    config.write_text(json.dumps(p), encoding='utf-8')
    output = tmp_path / 'out'
    first = write_bundle(source, config, output)
    assert first['selected_count'] == 12 and not first['run_allowed']
    with pytest.raises(FileExistsError):
        write_bundle(source, config, output)
    source.write_text('changed', encoding='utf-8')
    with pytest.raises(ValueError, match='SHA'):
        write_bundle(source, config, tmp_path / 'changed')


def test_primary_allocation_reserves_all_structure_strata():
    rows = [dict(position_id=f'a-{i}', task_id=str(i), graph='a', benchmark='b',
                 structure_bucket='simple') for i in range(100)]
    for bucket in ('simple', 'composed', 'nested_or_variable', 'unknown'):
        rows.append(dict(position_id=bucket, task_id=bucket, graph='b', benchmark='b',
                         structure_bucket=bucket))
    p = policy()
    p['target'] = 5
    selected, _ = select_population(rows, p)
    assert sum(row['graph'] == 'b' for row in selected) == 4


@pytest.mark.parametrize('target', [12.0, True])
def test_explicit_quotas_do_not_bypass_integer_target(target):
    p = policy(primary_quotas=[dict(values=['a'], count=6), dict(values=['b'], count=6)])
    p['target'] = target
    with pytest.raises(ValueError):
        select_population(population(), p)


def test_no_null_benchmark_even_when_population_agrees():
    rows, p = population(), policy()
    p['benchmark'] = None
    for row in rows:
        row['benchmark'] = None
    with pytest.raises(ValueError):
        select_population(rows, p)


@pytest.mark.parametrize('field', ['graph', 'task_id'])
def test_output_identity_required_even_when_not_used_for_strata(field):
    rows, p = population(), policy()
    p['primary_fields'] = ['split']
    rows[0][field] = None
    with pytest.raises(ValueError):
        select_population(rows, p)
