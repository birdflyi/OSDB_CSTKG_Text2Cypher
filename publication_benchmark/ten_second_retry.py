"""Bounded infrastructure retry; caller owns immutable attempt/budget records."""
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import time


def infrastructure_failure(record):
    status = record.get('http_status')
    return bool(record.get('transport_error') or status == 429 or
                (isinstance(status, int) and status >= 500))


def retry_delay(record, utcnow=lambda: datetime.now(timezone.utc)):
    """Ten seconds after failure, or longer if the server asks us to wait."""
    header = record.get('retry_after')
    if header is None:
        return 10.0
    try:
        delay = float(header)
    except (ValueError, TypeError):
        try:
            date = parsedate_to_datetime(header)
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            delay = (date - utcnow()).total_seconds()
        except (ValueError, TypeError, OverflowError):
            delay = 0
    return max(10.0, delay)


def run_sequence(attempt, record_wait, maximum_attempts=3,
                 clock=time.monotonic, sleep=time.sleep):
    """Return first non-infrastructure outcome, never inspect model correctness.

    `attempt(index)` MUST charge/save each physical attempt before returning.
    This function performs no hidden network call and catches no pending request.
    A connection/read timeout can have consumed provider work; record that cost
    as unknown when usage is missing. Three infrastructure outcomes end the run.
    """
    if not 1 <= maximum_attempts <= 3:
        raise ValueError('At most three actual attempts in one sequence')
    records = []
    for index in range(1, maximum_attempts + 1):
        if records:
            intended = retry_delay(records[-1])
            before = clock()
            while clock() - before < intended:
                sleep(min(10.0, intended - (clock() - before)))
            record_wait({'before_attempt': index, 'requested_wait_seconds': intended,
                         'actual_wait_seconds': clock() - before})
        record = attempt(index)
        records.append(record)
        if not infrastructure_failure(record):
            break
    return {'attempts': records, 'selected_attempt': len(records),
            'status': 'STOPPED_THREE_INFRASTRUCTURE_FAILURES' if
                len(records) == 3 and all(infrastructure_failure(r) for r in records)
                else 'SEQUENCE_FINISHED',
            'limits': 'Recovery observation, not evidence of rate-limit cause. Never reroll a returned model answer.'}
