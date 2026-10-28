from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager


class RecoveryGate:
    """Rolling retry admission, separate from the global endpoint permit pool.

    After eight completed retries, two transport failures halve the admission
    cap. Existing requests drain. New requests fill vacancies immediately; there
    is no full-batch barrier and no automatic upward ramp during recovery.
    """

    def __init__(self, cap=16, minimum=4):
        if not 1 <= minimum <= cap:
            raise ValueError("Invalid recovery bounds")
        self.cap, self.minimum = cap, minimum
        self.active = 0
        self.window = []
        self.changes = []
        self.condition = asyncio.Condition()

    @asynccontextmanager
    async def slot(self):
        async with self.condition:
            await self.condition.wait_for(lambda: self.active < self.cap)
            self.active += 1
        try:
            yield
        finally:
            async with self.condition:
                self.active -= 1
                self.condition.notify_all()

    async def record(self, status):
        async with self.condition:
            self.window.append(status in ("infra", "unknown", "throttled", "local_resource_error"))
            if len(self.window) == 8:
                if sum(self.window) >= 2:
                    previous = self.cap
                    self.cap = max(self.minimum, self.cap // 2)
                    if previous != self.cap:
                        self.changes.append(dict(previous=previous, current=self.cap))
                self.window.clear()
            self.condition.notify_all()
