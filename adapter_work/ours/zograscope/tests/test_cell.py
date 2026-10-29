import pytest


@pytest.mark.skip(reason='resource_pending: this benchmark has no constrained-IR compiler yet; only the CypherBench engine chain exists. Wrap or build the compiler before adapting.')
def test_adapter_pending():
    assert True
