"""Простой асинхронный in-memory кэш с TTL."""
import asyncio
import time
from typing import Any, Optional
from config import API_CFG


class MemoryCache:
    def __init__(self, ttl: Optional[int] = None):
        self._ttl = ttl if ttl is not None else API_CFG.get("cache_ttl", 300)
        self._store: dict[str, tuple[Any, float]] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            value, ts = entry
            if time.monotonic() - ts > self._ttl:
                del self._store[key]
                return None
            return value

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        effective_ttl = ttl if ttl is not None else self._ttl
        async with self._lock:
            self._store[key] = (value, time.monotonic())

    async def delete(self, key: str) -> None:
        async with self._lock:
            self._store.pop(key, None)

    async def clear(self) -> None:
        async with self._lock:
            self._store.clear()

    async def evict_expired(self) -> int:
        now = time.monotonic()
        async with self._lock:
            expired = [k for k, (_, ts) in self._store.items() if now - ts > self._ttl]
            for k in expired:
                del self._store[k]
        return len(expired)


cache = MemoryCache()