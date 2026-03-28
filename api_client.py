"""Клиент Brawl Stars API с ротацией ключей, rate limiting, кэшем и прокси."""
import aiohttp
import aiohttp_socks
import asyncio
import itertools
import sys
from typing import Optional, Dict, Any

from config import API_KEYS, API_CFG, PROXY_LIST
from utils.logger import setup_logger
from utils.rate_limiter import RateLimiter
from utils.cache import cache

logger = setup_logger()

BASE_URL = API_CFG.get("base_url", "https://api.brawlstars.com/v1")
TIMEOUT  = API_CFG.get("request_timeout", 10)


class BrawlAPIClient:
    def __init__(self):
        self.api_keys    = API_KEYS
        self._key_cycle  = itertools.cycle(self.api_keys)
        self._limiters   = {k: RateLimiter(
            max_calls=API_CFG.get("rate_limit_per_key", 30),
            period=60.0
        ) for k in self.api_keys}
        self._current_key = next(self._key_cycle)
        self._session: Optional[aiohttp.ClientSession] = None
        self.last_status: Optional[int] = None

        # Прокси
        self.proxies = PROXY_LIST
        self._proxy_cycle = itertools.cycle(self.proxies) if self.proxies else None
        self._current_proxy = None

        logger.info(
            f"API клиент: {len(self.api_keys)} ключей, {len(self.proxies)} прокси"
            if self.proxies
            else f"API клиент: {len(self.api_keys)} ключей (без прокси)"
        )

    async def _get_session(self, use_proxy: bool = True) -> aiohttp.ClientSession:
        """Создаёт новую сессию (с прокси или без)."""
        if self._session is not None and not self._session.closed:
            return self._session

        await self.close()  # закрываем старую, если была

        if use_proxy and self._proxy_cycle:
            self._current_proxy = next(self._proxy_cycle)
            try:
                connector = aiohttp_socks.SocksConnector.from_url(self._current_proxy)
                timeout = aiohttp.ClientTimeout(total=TIMEOUT)
                self._session = aiohttp.ClientSession(connector=connector, timeout=timeout)
                logger.debug(f"Создана сессия с прокси: {self._current_proxy}")
            except Exception as e:
                logger.error(f"Ошибка создания прокси {self._current_proxy}: {e}")
                # Пробуем следующий прокси рекурсивно
                await self._switch_proxy()
                return await self._get_session(use_proxy=True)
        else:
            # Без прокси
            connector = aiohttp.TCPConnector(limit=200)
            timeout = aiohttp.ClientTimeout(total=TIMEOUT)
            self._session = aiohttp.ClientSession(connector=connector, timeout=timeout)
            self._current_proxy = None
            logger.debug("Создана сессия без прокси")

        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def _switch_proxy(self):
        """Переключает на следующий прокси (если есть) или отключает прокси."""
        if self._proxy_cycle:
            self._current_proxy = next(self._proxy_cycle)
            logger.info(f"Переключились на прокси: {self._current_proxy}")
        else:
            self._current_proxy = None
            logger.info("Прокси отключены")

    @staticmethod
    def normalize_tag(tag: str) -> str:
        return tag.strip().upper().replace("#", "")

    async def _request(
        self,
        endpoint: str,
        use_cache: bool = True,
        cache_ttl: Optional[int] = None,
        attempt: int = 0,
    ) -> Optional[Dict[str, Any]]:
        """Выполняет запрос с повторными попытками при ошибках прокси/ключа."""
        cache_key = f"api:{endpoint}"

        if use_cache:
            cached = await cache.get(cache_key)
            if cached is not None:
                logger.debug(f"Cache hit: {endpoint}")
                return cached

        key = self._current_key
        limiter = self._limiters[key]
        await limiter.acquire()

        url = f"{BASE_URL}/{endpoint.lstrip('/')}"
        headers = {"Authorization": f"Bearer {key}"}

        # Получаем сессию (создаст новую, если нужно)
        session = await self._get_session()

        try:
            async with session.get(url, headers=headers) as resp:
                self.last_status = resp.status

                if resp.status == 200:
                    data = await resp.json()
                    if use_cache:
                        await cache.set(cache_key, data, ttl=cache_ttl)
                    return data

                if resp.status == 404:
                    return None

                if resp.status == 403:
                    logger.warning(f"403 на ключ {key[:10]}…, ротация")
                    print(
                        f"\033[91m[ERROR] 403 Forbidden\033[0m — ключ {key[:10]}… недействителен или истёк",
                        file=sys.stderr,
                    )
                    self._switch_key()
                    return await self._request(endpoint, use_cache=False, attempt=0)

                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", 5))
                    logger.warning(f"429 для {key[:10]}…, retry in {retry_after}s")
                    print(
                        f"\033[93m[WARN] 429 Too Many Requests\033[0m — ждём {retry_after} сек",
                        file=sys.stderr,
                    )
                    await asyncio.sleep(retry_after)
                    return await self._request(endpoint, use_cache=False, attempt=0)

                logger.error(f"HTTP {resp.status}: {url}")
                print(f"\033[91m[ERROR] HTTP {resp.status}\033[0m — {url}", file=sys.stderr)
                return None

        except (aiohttp.ClientError, asyncio.TimeoutError, aiohttp_socks.SocksError) as e:
            # Ошибка сети, таймаут или прокси
            error_msg = str(e).lower()
            is_proxy_error = (
                self._current_proxy is not None
                and ("proxy" in error_msg or "socks" in error_msg or "connection" in error_msg)
            )

            if is_proxy_error:
                logger.error(f"Ошибка прокси {self._current_proxy}: {e}")
                print(
                    f"\033[93m[WARN] Ошибка прокси {self._current_proxy}, пробуем следующий\033[0m",
                    file=sys.stderr,
                )
                await self.close()  # закрываем сессию
                # Переключаем на следующий прокси
                await self._switch_proxy()
                # Повторяем запрос (увеличиваем счётчик попыток)
                if attempt < len(self.proxies) * 2:  # максимум попыток (каждый прокси можно попробовать дважды)
                    return await self._request(endpoint, use_cache=False, attempt=attempt + 1)
                else:
                    # Все прокси перепробованы – отключаем прокси и пробуем без них
                    logger.error("Все прокси не работают, переходим на прямое соединение")
                    print(
                        "\033[93m[WARN] Все прокси не работают, переходим на прямое соединение\033[0m",
                        file=sys.stderr,
                    )
                    self._proxy_cycle = None
                    self._current_proxy = None
                    await self.close()
                    return await self._request(endpoint, use_cache=False, attempt=0)
            else:
                # Другая ошибка (не связанная с прокси)
                logger.error(f"Request error {url}: {e}")
                print(f"\033[91m[ERROR] {e}\033[0m", file=sys.stderr)
                return None

        except Exception as e:
            logger.error(f"Неизвестная ошибка {url}: {e}")
            print(f"\033[91m[ERROR] {e}\033[0m", file=sys.stderr)
            return None

    def _switch_key(self):
        self._current_key = next(self._key_cycle)

    # ────────────────────────────────────────────────────────────────────────
    # Публичные методы
    # ────────────────────────────────────────────────────────────────────────
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

    async def get_brawler(self, brawler_id: int) -> Optional[Dict]:
        return await self._request(f"brawlers/{brawler_id}", cache_ttl=3600)

    async def get_event_rotation(self) -> Optional[Dict]:
        return await self._request("events/rotation", cache_ttl=600)

    async def get_rankings_players(self, region: str = "global") -> Optional[Dict]:
        return await self._request(f"rankings/{region}/players", cache_ttl=120)

    async def get_rankings_clubs(self, region: str = "global") -> Optional[Dict]:
        return await self._request(f"rankings/{region}/clubs", cache_ttl=120)

    async def get_rankings_brawler(self, region: str, brawler_id: int) -> Optional[Dict]:
        return await self._request(f"rankings/{region}/brawlers/{brawler_id}", cache_ttl=120)

    async def get_powerplay_seasons(self, region: str = "global") -> Optional[Dict]:
        return await self._request(f"rankings/{region}/powerplay/seasons", cache_ttl=3600)

    async def get_powerplay_rankings(self, region: str, season_id: str) -> Optional[Dict]:
        return await self._request(
            f"rankings/{region}/powerplay/seasons/{season_id}", cache_ttl=300
        )

    async def get_locations(self) -> Optional[Dict]:
        return await self._request("locations", cache_ttl=86400)