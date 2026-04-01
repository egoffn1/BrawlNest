"""
Клиент официального API Brawl Stars.
Rate limiting: 30 запросов/минуту. Авто-повторы при 429.
"""
import aiohttp
import asyncio
import time
from typing import Optional, Dict, Any, List
from config import BRAWL_API_KEY
from utils.logger import setup_logger

logger = setup_logger(__name__)

BASE_URL = "https://api.brawlstars.com/v1"
TIMEOUT  = 10


class BrawlAPIClient:
    def __init__(self):
        self.api_key      = BRAWL_API_KEY
        self._session: Optional[aiohttp.ClientSession] = None
        self._rate_limit  = 30
        self._period      = 60
        self._requests: List[float] = []

    async def start(self):
        self._session = aiohttp.ClientSession()

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _wait_for_rate_limit(self):
        now = time.time()
        self._requests = [t for t in self._requests if now - t < self._period]
        if len(self._requests) >= self._rate_limit:
            sleep_time = self._period - (now - self._requests[0])
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
        self._requests.append(time.time())

    async def _request(self, endpoint: str, retry: int = 0) -> Optional[Dict]:
        if not self.api_key:
            return None
        await self._wait_for_rate_limit()
        url     = f"{BASE_URL}/{endpoint.lstrip('/')}"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        try:
            async with self._session.get(url, headers=headers, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    return await resp.json()
                elif resp.status == 404:
                    return None
                elif resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", 60))
                    await asyncio.sleep(retry_after)
                    if retry < 3:
                        return await self._request(endpoint, retry + 1)
                    return None
                else:
                    logger.error(f"Brawl API {resp.status} for {endpoint}")
                    return None
        except Exception as e:
            logger.exception(f"Brawl API error: {e}")
            return None

    async def get_player(self, tag: str) -> Optional[Dict]:
        tag = tag.strip().upper().lstrip("#")
        return await self._request(f"players/%23{tag}")

    async def get_club(self, tag: str) -> Optional[Dict]:
        tag = tag.strip().upper().lstrip("#")
        return await self._request(f"clubs/%23{tag}")

    async def get_club_members(self, tag: str) -> Optional[List[Dict]]:
        tag  = tag.strip().upper().lstrip("#")
        data = await self._request(f"clubs/%23{tag}/members")
        return data.get("items") if data else None

    async def get_battlelog(self, tag: str) -> Optional[Dict]:
        tag = tag.strip().upper().lstrip("#")
        return await self._request(f"players/%23{tag}/battlelog")

    async def get_brawlers(self) -> Optional[Dict]:
        return await self._request("brawlers")
