from audit_discrimination import bag, evaluate
from run_baselines import build_specs
from test_baselines import fixture_graph


def test_out_of_window_witness_exposes_only_relevant_dropped_bound():
    nodes, edges = fixture_graph()
    specs = build_specs(nodes, edges)
    assert bag(evaluate(nodes, edges, specs, 'drop_start')) == bag(evaluate(nodes, edges, specs, 'drop_start', True))
    link = next(e for e in edges if e['service'] == 'LINKS_TO')
    earlier = edges + [{**link, 'uid': 'earlier', 'time': '2022-06-01T00:00:00Z'}]
    later = edges + [{**link, 'uid': 'later', 'time': '2024-06-01T00:00:00Z'}]
    gold = evaluate(nodes, edges, specs, 'drop_start')
    assert bag(evaluate(nodes, earlier, specs, 'drop_start')) == bag(gold)
    assert bag(evaluate(nodes, earlier, specs, 'drop_start', True)) != bag(gold)
    assert bag(evaluate(nodes, later, specs, 'drop_start', True)) == bag(gold)
    assert bag(evaluate(nodes, later, specs, 'drop_end', True)) != bag(gold)


def test_original_parallel_edges_and_path_direction_detect_other_faults():
    nodes, edges = fixture_graph()
    specs = build_specs(nodes, edges)
    for kind in ('distinct_count', 'distinct_rows', 'reverse_comment_direction', 'drop_zero_window'):
        assert bag(evaluate(nodes, edges, specs, kind)) != bag(evaluate(nodes, edges, specs, kind, True))


def test_null_time_only_enters_the_query_if_both_time_predicates_are_removed():
    nodes, edges = fixture_graph()
    specs = build_specs(nodes, edges)
    link = next(e for e in edges if e['service'] == 'LINKS_TO')
    fixture = edges + [{**link, 'uid': 'nulltime', 'time': None}]
    assert evaluate(nodes, fixture, specs, 'drop_both') == [['example.org', 2]]
    assert evaluate(nodes, fixture, specs, 'drop_both', True) == [['example.org', 3]]
    assert evaluate(nodes, fixture, specs, 'drop_start', True) == [['example.org', 2]]
