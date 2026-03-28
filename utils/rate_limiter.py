"""Асинхронный rate-limiter по скользящему окну."""
import asyncio
import time
from collections import deque


class RateLimiter:
    def __init__(self, max_calls: int = 30, period: float = 60.0):
        self.max_calls = max_calls
        self.period = period
        self._calls: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            # убираем вызовы вне окна
            while self._calls and now - self._calls[0] >= self.period:
                self._calls.popleft()

            if len(self._calls) >= self.max_calls:
                sleep_for = self.period - (now - self._calls[0]) + 0.05
                await asyncio.sleep(sleep_for)
                now = time.monotonic()
                while self._calls and now - self._calls[0] >= self.period:
                    self._calls.popleft()

            self._calls.append(now)
