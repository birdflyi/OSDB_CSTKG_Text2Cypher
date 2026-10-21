from run_baselines import score
from run_comparison import graph_arm


def test_numbers_compare_by_value_without_coercing_strings_or_booleans():
    specs = [{'id': 'X', 'expected_status': 'ANSWER', 'columns': ['n'], 'expected_rows': [[3]]}]
    def correct(value):
        answer = [{'id': 'X', 'status': 'ANSWER', 'rows': [[value]], 'evidence_ids': ['E']}]
        return score(specs, answer, {'X': {'E'}})['summary']['table_and_decision_correct']
    assert correct(3.0) == 1
    assert correct('3') == 0
    assert correct(True) == 0


def test_upstream_failure_is_not_sent_to_an_answer_model(tmp_path):
    dispatches = []
    class FailedClient:
        def batches(self, stage, prompt, schema, tasks, keys, batch_size):
            if tasks:
                dispatches.append(stage)
                return [{'id': t['id'], 'status': 'ERROR', 'cypher': None,
                         'reason': 'upstream failed'} for t in tasks]
            return []
    tasks = [{'id': 'X', 'question': 'Count records', 'columns': ['n']}]
    answers, evidence, queries = graph_arm(FailedClient(), 'arm', 'prompt', tasks, tmp_path)
    assert dispatches == ['arm_queries']
    assert answers == [{'id': 'X', 'status': 'ERROR', 'rows': [], 'evidence_ids': []}]
    assert queries == answers
    assert evidence == {'X': set()}
