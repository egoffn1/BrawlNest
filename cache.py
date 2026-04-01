import time
import asyncio
from typing import Any, Optional

class InMemoryCache:
    def __init__(self):
        self._store = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            e = self._store.get(key)
            if e is None:
                return None
            v, exp = e
            if exp and time.monotonic() > exp:
                del self._store[key]
                return None
            return v

    async def set(self, key: str, value: Any, ttl: Optional[int] = None):
        async with self._lock:
            self._store[key] = (value, time.monotonic() + ttl if ttl else None)

    async def delete(self, key: str):
        async with self._lock:
            self._store.pop(key, None)

    async def exists(self, key: str) -> bool:
        return await self.get(key) is not None

    async def close(self):
        pass