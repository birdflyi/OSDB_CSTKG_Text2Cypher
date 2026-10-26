"""Independent outcome and identity fault injection, no live/model requests."""
from copy import deepcopy
import unittest
from offline_score_pairing import evaluate, fingerprint, PairingError


CONTEXT = dict(cohort='synthetic-v1', method='fixed-method', model='no-model-called',
    input_contract='equal-input-v1', score_contract='constructed-exact-v1')


def answer(rows):
    return {'status':'EXECUTED', 'complete':True, 'rows':rows}


def validate(value):
    if type(value) is not dict or value.get('status') != 'EXECUTED' or value.get('complete') is not True:
        raise ValueError('Incomplete answer')
    if type(value.get('rows')) is not list:
        raise ValueError('Rows required')


def compare(a,b):
    return a['rows'] == b['rows']


def task(qid, graph='g', reference='known'):
    return dict(id=qid, graph=graph, task_sha256=fingerprint([graph,qid]),
        reference=answer([[qid]]) if reference=='known' else None)


def observation(t, status='EXECUTED', value='same'):
    return dict(id=t['id'], graph=t['graph'], task_sha256=t['task_sha256'],
        context=deepcopy(CONTEXT), status=status,
        answer=deepcopy(t['reference']) if value=='same' and status=='EXECUTED' else value if value!='same' else None)


def run(tasks, observations, **kwargs):
    return evaluate(tasks, observations, context=CONTEXT, validate_answer=validate,
        compare=kwargs.get('compare',compare))


class PairingFaults(unittest.TestCase):
    def test_reordering_cannot_pair_wrong_questions(self):
        a,b=task('a'),task('b')
        self.assertEqual(run([a,b],[observation(b),observation(a)])['summary']['correct_complete_answers'],2)

    def test_same_id_in_two_graphs_remains_two_positions(self):
        a,b=task('same','g1'),task('same','g2')
        self.assertEqual(run([a,b],[observation(a),observation(b)])['summary']['planned'],2)

    def test_missing_position_is_unsent_and_fraction_withheld(self):
        a,b=task('a'),task('b');s=run([a,b],[observation(a)])['summary']
        self.assertEqual((s['planned'],s['unsent'],s['known_reference_denominator']),(2,1,2))
        self.assertIsNone(s['diagnostic_known_reference_fraction'])

    def test_duplicate_final_cannot_select_last_or_best(self):
        a=task('a')
        with self.assertRaises(PairingError):run([a],[observation(a),observation(a)])

    def test_unknown_id_is_not_silently_ignored(self):
        with self.assertRaises(PairingError):run([task('a')],[observation(task('b'))])

    def test_reference_version_mismatch_blocks_scoring(self):
        a=task('a');o=observation(a);o['task_sha256']='0'*64
        with self.assertRaises(PairingError):run([a],[o])

    def test_each_context_component_must_match(self):
        a=task('a')
        for field in CONTEXT:
            with self.subTest(field=field),self.assertRaises(PairingError):
                o=observation(a);o['context'][field]='changed';run([a],[o])

    def test_valid_empty_is_complete_and_can_match(self):
        a=task('a');a['reference']=answer([])
        s=run([a],[observation(a)])['summary']
        self.assertEqual((s['complete_answers'],s['correct_complete_answers']),(1,1))

    def test_failure_empty_is_not_success(self):
        a=task('a');a['reference']=answer([])
        with self.assertRaises(PairingError):run([a],[observation(a,'EXECUTION_ERROR',answer([]))])

    def test_every_failure_refusal_and_unknown_completion_is_retained(self):
        from offline_score_pairing import FAILURES
        tasks=[task(k) for k in sorted(FAILURES)]
        s=run(tasks,[observation(t,t['id']) for t in tasks])['summary']
        self.assertEqual((s['planned'],s['complete_answers'],s['correct_complete_answers']),(6,0,0))
        self.assertEqual(s['diagnostic_known_reference_fraction'],0)

    def test_unknown_gold_never_becomes_wrong_or_correct(self):
        a=task('a',reference='unknown');o=observation(a,value=answer([]))
        r=run([a],[o]);self.assertEqual(r['records'][0]['score_state'],'UNKNOWN_REFERENCE')
        self.assertIsNone(r['records'][0]['score']);self.assertEqual(r['summary']['complete_answers'],1)

    def test_score_exception_is_unresolved_not_wrong(self):
        a=task('a')
        def broken(a,b):raise RuntimeError('synthetic comparator failure')
        s=run([a],[observation(a)],compare=broken)['summary']
        self.assertEqual((s['scoring_errors'],s['wrong_complete_answers']),(1,0))
        self.assertIsNone(s['diagnostic_known_reference_fraction'])

    def test_score_integer_is_not_a_bool(self):
        a=task('a');s=run([a],[observation(a)],compare=lambda a,b:1)['summary']
        self.assertEqual(s['scoring_errors'],1)

    def test_incomplete_success_is_invalid_not_covered(self):
        a=task('a');o=observation(a);o['answer']['complete']=False
        s=run([a],[o])['summary'];self.assertEqual(s['complete_answers'],0)
        self.assertEqual(s['status_counts'],{'INVALID_ANSWER':1})

    def test_wrong_final_answer_counts_separately_from_failure(self):
        a=task('a');s=run([a],[observation(a,value=answer([['wrong']]))])['summary']
        self.assertEqual((s['complete_answers'],s['wrong_complete_answers']),(1,1))

    def test_duplicate_or_malformed_manifest_blocks_whole_batch(self):
        a=task('a')
        for tasks in ([a,a],[dict(a,reference=answer(None))],[dict(a,task_sha256='bad')]):
            with self.subTest(tasks=tasks),self.assertRaises(PairingError):run(tasks,[])

    def test_no_predictions_preserves_full_denominator(self):
        s=run([task('a'),task('b')],[])['summary']
        self.assertEqual((s['planned'],s['unsent'],s['coverage_denominator']),(2,2,2))


if __name__=='__main__':unittest.main()
