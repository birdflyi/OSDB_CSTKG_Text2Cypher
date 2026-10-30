"""Alias routes must wake on policy deadlines without another request release."""
import asyncio

import pytest

from engine.config import Endpoint
from engine.pool import DynamicPool


@pytest.mark.parametrize('timer', ['cooldown_until', 'quarantined_until', 'degraded_until'])
def test_alias_policy_expiry_wakes_waiter(timer):
    async def scenario():
        pool = DynamicPool(2)
        await pool.update([Endpoint('route', 'http://localhost/unused', 'physical',
                                    'unused', 2, serves=('logical',))])
        state = next(iter(pool.states.values()))
        held = None
        if timer == 'degraded_until':
            held = await pool.acquire('logical', 1)
            state.degraded = True
        setattr(state, timer, pool.clock() + 0.03)
        # A long admission deadline and a shorter test watchdog distinguish an
        # expiry wakeup from sleeping to the request's own admission deadline.
        permit = await asyncio.wait_for(pool.acquire('logical', 5), 1)
        assert permit.state is state
        await permit.release()
        if held:
            await held.release()
    asyncio.run(scenario())
