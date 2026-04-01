"""
Redis-кэш с поддержкой hash-индексов, distributed locks, sadd/smembers.
"""
import redis.asyncio as redis
import json
import uuid
from typing import Any, Dict, List, Optional


class RedisCache:
    def __init__(self, url: str):
        self.redis = redis.from_url(url, decode_responses=True)

    # ─── Базовые операции ────────────────────────────────────────────────────

    async def get(self, key: str) -> Optional[Any]:
        data = await self.redis.get(key)
        return json.loads(data) if data else None

    async def set(self, key: str, value: Any, ttl: int = 300):
        await self.redis.set(key, json.dumps(value), ex=ttl)

    async def setex(self, key: str, ttl: int, value: str):
        await self.redis.setex(key, ttl, value)

    async def delete(self, key: str):
        await self.redis.delete(key)

    async def exists(self, key: str) -> bool:
        return bool(await self.redis.exists(key))

    async def expire(self, key: str, ttl: int):
        await self.redis.expire(key, ttl)

    async def ttl(self, key: str) -> int:
        return await self.redis.ttl(key)

    # ─── Set-операции (для узлов) ────────────────────────────────────────────

    async def sadd(self, name: str, *values):
        await self.redis.sadd(name, *values)

    async def smembers(self, name: str) -> set:
        return await self.redis.smembers(name)

    async def srem(self, name: str, *values):
        await self.redis.srem(name, *values)

    # ─── Hash-операции (для индексов имён) ──────────────────────────────────

    async def hset(self, name: str, key: str, value: Any):
        await self.redis.hset(name, key, json.dumps(value))

    async def hget(self, name: str, key: str) -> Optional[Any]:
        data = await self.redis.hget(name, key)
        return json.loads(data) if data else None

    async def hgetall(self, name: str) -> Dict[str, Any]:
        raw = await self.redis.hgetall(name)
        return {k: json.loads(v) for k, v in raw.items()}

    async def hmset_dict(self, name: str, mapping: Dict[str, Any], ttl: Optional[int] = None):
        if not mapping:
            return
        pipe = self.redis.pipeline()
        pipe.hset(name, mapping={k: json.dumps(v) for k, v in mapping.items()})
        if ttl:
            pipe.expire(name, ttl)
        await pipe.execute()

    async def hscan_search(self, name: str, query: str) -> List[Dict]:
        results = []
        q = query.lower()
        cursor = 0
        while True:
            cursor, items = await self.redis.hscan(name, cursor, count=200)
            for _tag, raw in items.items():
                try:
                    obj = json.loads(raw)
                    if q in obj.get("name", "").lower():
                        results.append(obj)
                except Exception:
                    continue
            if cursor == 0:
                break
        return results

    async def hdel(self, name: str, key: str):
        await self.redis.hdel(name, key)

    # ─── ZSet-операции (для пингов узлов) ───────────────────────────────────

    async def zadd(self, name: str, mapping: Dict[str, float]):
        await self.redis.zadd(name, mapping)

    async def zrange_withscores(self, name: str) -> List:
        return await self.redis.zrange(name, 0, -1, withscores=True)

    # ─── Distributed Lock ────────────────────────────────────────────────────

    async def acquire_lock(self, lock_key: str, ttl: int = 60) -> Optional[str]:
        token = str(uuid.uuid4())
        acquired = await self.redis.set(f"lock:{lock_key}", token, nx=True, ex=ttl)
        return token if acquired else None

    async def release_lock(self, lock_key: str, token: str):
        current = await self.redis.get(f"lock:{lock_key}")
        if current == token:
            await self.redis.delete(f"lock:{lock_key}")

    async def get_ttl(self, key: str) -> int:
        return await self.redis.ttl(key)

    async def close(self):
        await self.redis.aclose()
