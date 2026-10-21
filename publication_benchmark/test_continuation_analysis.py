import json
import analyze_continuation as a


def test_unknown_usage_is_not_zero_cost():
    result=a.usage([{'http_status':200,'seconds':2,'body':{'usage':{'prompt_tokens':3,'completion_tokens':4,'total_tokens':7}}},
                    {'transport_error':'ReadTimeout','seconds':120},None])
    assert result['known_total_tokens']==7
    assert result['unknown_usage_attempts']==2
    assert not result['total_tokens_complete']
    assert result['currency_cost'] is None


def test_unique_ledger_does_not_double_count_copied_cache(tmp_path):
    budget=tmp_path/'budget'
    budget.mkdir()
    original=tmp_path/'original'
    original.mkdir()
    terminal={'http_status':200,'body':{'usage':{'total_tokens':10}},'seconds':1}
    (original/'terminal.json').write_text(json.dumps(terminal))
    (budget/'0001.json').write_text(json.dumps({'request_path':str(original/'request.json')}))
    copied=tmp_path/'copied'
    copied.mkdir()
    (copied/'terminal.json').write_text(json.dumps(terminal))
    result=a.ledger(tmp_path)
    assert result['attempts']==1
    assert result['known_total_tokens']==10


def test_goal_bootstrap_does_not_treat_rewrites_as_new_goals():
    result=a.goal_bootstrap([{'semantic_goal':'one','difference':1}]*100+
                            [{'semantic_goal':'two','difference':-1}],repetitions=100)
    assert result['goals']==2
    assert result['mean']==0


def test_zero_success_cannot_produce_free_correct_answer():
    assert a.goal_bootstrap([])['interval'] is None
    result=a.usage([{'http_status':200,'body':{},'seconds':1}])
    assert result['unknown_usage_attempts']==1
