"""Поиск по игрокам и клубам через Redis-индексы."""
from __future__ import annotations
from typing import Dict, List, Optional
from utils.logger import setup_logger

logger = setup_logger(__name__)


class SearchService:
    def __init__(self, db, github_svc, cache=None):
        self.db     = db
        self.github = github_svc
        self.cache  = cache

    async def _ensure_player_index(self):
        if self.cache and not await self.cache.exists("idx:player_names"):
            import asyncio
            asyncio.create_task(self._rebuild_players())

    async def _ensure_club_index(self):
        if self.cache and not await self.cache.exists("idx:club_names"):
            import asyncio
            asyncio.create_task(self._rebuild_clubs())

    async def _rebuild_players(self):
        index = await self.github.build_name_index()
        if index and self.cache:
            await self.cache.hmset_dict("idx:player_names", index, ttl=7200)

    async def _rebuild_clubs(self):
        index = await self.github.build_club_name_index()
        if index and self.cache:
            await self.cache.hmset_dict("idx:club_names", index, ttl=7200)

    async def search_players(self, name: str, limit: int = 20,
                              filters: Optional[Dict] = None) -> List[Dict]:
        if self.cache:
            await self._ensure_player_index()
            results = await self.cache.hscan_search("idx:player_names", name)
        else:
            players = await self.github.list_players()
            all_data = await self.github.bulk_fetch_players(players)
            q = name.lower()
            results = [p for p in all_data if q in p.get("name", "").lower()]

        if filters:
            min_tr = filters.get("min_trophies")
            if min_tr is not None:
                results = [r for r in results if r.get("trophies", 0) >= min_tr]

        return results[:limit]

    async def search_clubs(self, name: str, limit: int = 20) -> List[Dict]:
        if self.cache:
            await self._ensure_club_index()
            results = await self.cache.hscan_search("idx:club_names", name)
        else:
            clubs    = await self.github.list_clubs()
            all_data = await self.github.bulk_fetch_clubs(clubs)
            q = name.lower()
            results = [c for c in all_data if q in c.get("name", "").lower()]

        return results[:limit]

    async def advanced_search(self, query: str, search_type: str = "players",
                               sort_by: str = "trophies", order: str = "desc",
                               limit: int = 20, offset: int = 0) -> Dict:
        if search_type == "players":
            results = await self.search_players(query, limit + offset)
        else:
            results = await self.search_clubs(query, limit + offset)

        reverse = (order == "desc")
        results.sort(key=lambda x: x.get(sort_by, 0), reverse=reverse)
        return {
            "query": query, "type": search_type, "sort_by": sort_by, "order": order,
            "total": len(results), "limit": limit, "offset": offset,
            "results": results[offset:offset + limit],
        }
