"""Клиент Brawl Stars API с ротацией ключей, rate limiting и кэшем."""
import aiohttp
import asyncio
import itertools
import sys
import time
from typing import Optional, Dict, Any, List

from config import API_KEYS, API_CFG, PROXY_LIST
from utils.logger import setup_logger

logger = setup_logger(__name__)

BASE_URL = API_CFG.get("base_url", "https://api.brawlstars.com/v1")
TIMEOUT  = API_CFG.get("request_timeout", 10)

# Simple in-memory cache
_cache: Dict[str, Any] = {}
_cache_ts: Dict[str, float] = {}


class BrawlAPIClient:
    def __init__(self):
        self.api_keys = API_KEYS
        self._key_idx = 0
        self._session: Optional[aiohttp.ClientSession] = None
        self.last_status: Optional[int] = None
        self._requests: List[float] = []

    @property
    def has_keys(self) -> bool:
        return bool(self.api_keys)

    @staticmethod
    def normalize_tag(tag: str) -> str:
        return tag.strip().upper().replace("#", "")

    def _current_key(self) -> Optional[str]:
        if not self.api_keys:
            return None
        return self.api_keys[self._key_idx % len(self.api_keys)]

    def _rotate_key(self):
        if self.api_keys:
            self._key_idx = (self._key_idx + 1) % len(self.api_keys)

    async def _wait_rate_limit(self):
        now = time.time()
        self._requests = [t for t in self._requests if now - t < 60]
        limit = API_CFG.get("rate_limit_per_key", 30) * max(len(self.api_keys), 1)
        if len(self._requests) >= limit:
            sleep = 60 - (now - self._requests[0]) + 0.1
            await asyncio.sleep(sleep)
        self._requests.append(time.time())

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session and not self._session.closed:
            return self._session
        connector = aiohttp.TCPConnector(limit=50)
        timeout   = aiohttp.ClientTimeout(total=TIMEOUT)
        self._session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def _request(self, endpoint: str, use_cache: bool = True,
                        cache_ttl: int = 300) -> Optional[Dict]:
        if not self.has_keys:
            return None
        cache_key = f"bs:{endpoint}"
        if use_cache and cache_key in _cache:
            if time.time() - _cache_ts[cache_key] < cache_ttl:
                return _cache[cache_key]

        await self._wait_rate_limit()
        key = self._current_key()
        url = f"{BASE_URL}/{endpoint.lstrip('/')}"
        headers = {"Authorization": f"Bearer {key}"}
        session = await self._get_session()
        try:
            async with session.get(url, headers=headers) as resp:
                self.last_status = resp.status
                if resp.status == 200:
                    data = await resp.json()
                    if use_cache:
                        _cache[cache_key] = data
                        _cache_ts[cache_key] = time.time()
                    return data
                if resp.status == 404:
                    return None
                if resp.status == 403:
                    self._rotate_key()
                    return None
                if resp.status == 429:
                    ra = int(resp.headers.get("Retry-After", 60))
                    await asyncio.sleep(ra)
                    return await self._request(endpoint, use_cache=False, cache_ttl=cache_ttl)
                return None
        except Exception as e:
            logger.debug(f"Request error {url}: {e}")
            return None

    async def get_player(self, tag: str, force: bool = False) -> Optional[Dict]:
        t = self.normalize_tag(tag)
        return await self._request(f"players/%23{t}", use_cache=not force)

    async def get_battlelog(self, tag: str, force: bool = False) -> Optional[Dict]:
        t = self.normalize_tag(tag)
        return await self._request(f"players/%23{t}/battlelog", use_cache=not force, cache_ttl=60)

    async def get_club(self, tag: str, force: bool = False) -> Optional[Dict]:
        t = self.normalize_tag(tag)
        return await self._request(f"clubs/%23{t}", use_cache=not force)

    async def get_club_members(self, tag: str) -> Optional[Dict]:
        t = self.normalize_tag(tag)
        return await self._request(f"clubs/%23{t}/members", use_cache=False)

    async def get_brawlers(self) -> Optional[Dict]:
        return await self._request("brawlers", cache_ttl=3600)

    async def get_event_rotation(self) -> Optional[Dict]:
        return await self._request("events/rotation", cache_ttl=600)

    async def get_rankings_players(self, region: str = "global") -> Optional[Dict]:
        return await self._request(f"rankings/{region}/players", cache_ttl=120)

    async def get_rankings_clubs(self, region: str = "global") -> Optional[Dict]:
        return await self._request(f"rankings/{region}/clubs", cache_ttl=120)

    async def get_powerplay_seasons(self, region: str = "global") -> Optional[Dict]:
        return await self._request(f"rankings/{region}/powerplay/seasons", cache_ttl=3600)

    async def get_locations(self) -> Optional[Dict]:
        return await self._request("locations", cache_ttl=86400)
