import asyncio
from types import SimpleNamespace

import pytest

from common.dispatch import generate_path


@pytest.mark.parametrize('status', ['length', 'empty', 'refused'])
def test_nonusable_paid_response_is_not_parsed_or_reissued(status):
    calls = []

    async def generate(item):
        calls.append(item)
        return dict(status=status, content='RETURN 1', http_status=200)

    def parse(*args, **kwargs):
        raise AssertionError('Incomplete response must not reach parser')

    spec = dict(benchmark='b', task_id='t', method='direct', graph='g',
                messages=[dict(role='user', content='question')])
    result = asyncio.run(generate_path(SimpleNamespace(parse=parse), spec,
                        SimpleNamespace(generate=generate), model='m', sampling={}))
    assert result['status'] == status and result['prediction'] is None
    assert len(calls) == 1 and len(result['replies']) == 1
