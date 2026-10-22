from datetime import datetime, timezone, timedelta
import pytest
from ten_second_retry import run_sequence, retry_delay


def exercise(records):
    now = [0.0]
    calls, waits = [], []
    def attempt(index):
        calls.append((index, now[0]))
        now[0] += 4
        return records[index - 1]
    def sleep(seconds): now[0] += seconds
    result = run_sequence(attempt, waits.append, clock=lambda: now[0], sleep=sleep)
    return result, calls, waits


def test_failures_are_kept_and_recovery_waits_after_failed_completion():
    result, calls, waits = exercise([{'http_status': 500}, {'transport_error': 'ReadTimeout'}, {'http_status': 200}])
    assert calls == [(1, 0), (2, 14), (3, 28)]
    assert [w['actual_wait_seconds'] for w in waits] == [10, 10]
    assert len(result['attempts']) == 3 and result['selected_attempt'] == 3


def test_three_physical_failures_stop_without_fourth_call():
    result, calls, _ = exercise([{'http_status': 500}] * 3)
    assert len(calls) == 3 and result['status'] == 'STOPPED_THREE_INFRASTRUCTURE_FAILURES'


@pytest.mark.parametrize('record', [
    {'http_status': 200, 'body': {'answer': 'wrong'}},
    {'http_status': 200, 'body': {'status': 'UNSUPPORTED'}},
    {'http_status': 200, 'body': {'choices': [{'finish_reason': 'length'}]}},
    {'http_status': 401}, {'http_status': 400},
])
def test_non_infrastructure_results_are_never_rerolled(record):
    result, calls, waits = exercise([record])
    assert len(calls) == 1 and not waits and result['attempts'] == [record]


def test_retry_after_never_shortens_the_wait():
    now = datetime(2026, 10, 22, tzinfo=timezone.utc)
    assert retry_delay({'retry_after': '1'}) == 10
    assert retry_delay({'retry_after': '25'}) == 25
    header = (now + timedelta(seconds=30)).strftime('%a, %d %b %Y %H:%M:%S GMT')
    assert retry_delay({'retry_after': header}, utcnow=lambda: now) == 30


def test_unrecorded_failure_is_not_silently_retried():
    calls = []
    def attempt(index):
        calls.append(index)
        raise RuntimeError('Pending unrecorded dispatch; stop')
    with pytest.raises(RuntimeError):
        run_sequence(attempt, lambda record: None)
    assert calls == [1]
