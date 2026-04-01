"""Simple in-memory cache (used by api_client for direct Brawl API calls)."""
import time
from typing import Any, Optional

class _Cache:
    def __init__(self):
        self._data = {}
        self._ts   = {}

    async def get(self, key: str) -> Optional[Any]:
        if key not in self._data:
            return None
        return self._data[key]

    async def set(self, key: str, value: Any, ttl: int = 300):
        self._data[key] = value
        self._ts[key]   = time.time() + ttl

    def _evict(self):
        now = time.time()
        expired = [k for k, exp in self._ts.items() if now > exp]
        for k in expired:
            self._data.pop(k, None)
            self._ts.pop(k, None)

cache = _Cache()
