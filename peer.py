"""HTTP P2P клиент для взаимодействия между узлами BrawlNest."""
import aiohttp
import asyncio
import time
import uuid
import json
from typing import List, Dict, Any, Optional
from config import settings
from utils.logger import setup_logger

logger = setup_logger(__name__)


class PeerClient:
    def __init__(self, cache):
        self.cache        = cache
        self.node_address = settings.node_address
        self.node_secret  = settings.node_secret
        self.timeout      = 2  # seconds for ping

        # unique node id
        import hashlib, random
        seed = f"{self.node_address}{random.random()}"
        self.node_id = hashlib.sha1(seed.encode()).hexdigest()[:12]

    # ─── Registration & Heartbeat ────────────────────────────────────────────

    async def register_node(self):
        await self.cache.sadd("nodes:active", self.node_id)
        heartbeat = json.dumps({"address": self.node_address, "last_seen": time.time()})
        await self.cache.setex(f"node:heartbeat:{self.node_id}", 60, heartbeat)
        # own ping = 0
        await self.cache.hset("node:ping", self.node_id, json.dumps({
            "address": self.node_address,
            "ping_ms": 0,
            "last_updated": time.time(),
        }))
        logger.info(f"Node registered: id={self.node_id} addr={self.node_address}")

    async def heartbeat(self):
        heartbeat = json.dumps({"address": self.node_address, "last_seen": time.time()})
        await self.cache.setex(f"node:heartbeat:{self.node_id}", 60, heartbeat)

    # ─── Node discovery ──────────────────────────────────────────────────────

    async def get_active_nodes(self) -> List[Dict]:
        """Вернуть список активных узлов (проверяем heartbeat TTL)."""
        ids = await self.cache.smembers("nodes:active")
        active = []
        for node_id in ids:
            raw = await self.cache.get(f"node:heartbeat:{node_id}") if hasattr(self.cache, 'get') else None
            if raw is None:
                # try redis directly
                try:
                    raw_str = await self.cache.redis.get(f"node:heartbeat:{node_id}")
                    if raw_str:
                        raw = json.loads(raw_str)
                except Exception:
                    pass
            if raw:
                active.append({"node_id": node_id, **raw})
        return active

    async def get_all_nodes_with_ping(self) -> List[Dict]:
        """Список всех узлов с их текущим пингом."""
        try:
            ping_data = await self.cache.hgetall("node:ping")
        except Exception:
            ping_data = {}
        result = []
        for nid, info in ping_data.items():
            if isinstance(info, str):
                try:
                    info = json.loads(info)
                except Exception:
                    continue
            result.append({"node_id": nid, **info})
        return sorted(result, key=lambda x: x.get("ping_ms", 9999))

    # ─── Ping measurement ────────────────────────────────────────────────────

    async def measure_pings(self):
        """Измерить пинг до всех активных узлов."""
        active = await self.get_active_nodes()
        headers = {"X-Node-Secret": self.node_secret}

        async def _ping(node: Dict):
            addr    = node.get("address", "")
            node_id = node.get("node_id", "")
            if addr == self.node_address or not addr:
                return
            t0 = time.monotonic()
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.get(f"{addr}/ping", headers=headers,
                                        timeout=aiohttp.ClientTimeout(total=self.timeout)) as resp:
                        if resp.status == 200:
                            ping_ms = int((time.monotonic() - t0) * 1000)
                            await self.cache.hset("node:ping", node_id, {
                                "address": addr,
                                "ping_ms": ping_ms,
                                "last_updated": time.time(),
                            })
            except Exception:
                # Node unavailable — remove from active set
                await self.cache.srem("nodes:active", node_id)
                try:
                    await self.cache.hdel("node:ping", node_id)
                except Exception:
                    pass

        await asyncio.gather(*[_ping(n) for n in active])

    # ─── Best node routing ───────────────────────────────────────────────────

    async def get_best_node_address(self) -> Optional[str]:
        """Вернуть адрес узла с наименьшим пингом."""
        nodes = await self.get_all_nodes_with_ping()
        if not nodes:
            return None
        best = min(nodes, key=lambda x: x.get("ping_ms", 9999))
        return best.get("address")

    def is_best_node(self, best_address: Optional[str]) -> bool:
        if not best_address:
            return True
        return best_address.rstrip("/") == self.node_address.rstrip("/")

    # ─── Peer fetch ──────────────────────────────────────────────────────────

    async def fetch_from_peers(self, endpoint: str, params: Dict = None,
                                method: str = "GET", json_data: Any = None) -> Optional[Dict]:
        active  = await self.get_active_nodes()
        headers = {"X-Node-Secret": self.node_secret}
        for node in active:
            addr = node.get("address", "")
            if not addr or addr == self.node_address:
                continue
            url = f"{addr}{endpoint}"
            try:
                async with aiohttp.ClientSession() as sess:
                    if method == "GET":
                        async with sess.get(url, params=params, headers=headers,
                                             timeout=aiohttp.ClientTimeout(total=self.timeout)) as resp:
                            if resp.status == 200:
                                return await resp.json()
                    elif method == "POST":
                        async with sess.post(url, params=params, json=json_data, headers=headers,
                                              timeout=aiohttp.ClientTimeout(total=self.timeout)) as resp:
                            if resp.status == 200:
                                return await resp.json()
            except Exception:
                continue
        return None
