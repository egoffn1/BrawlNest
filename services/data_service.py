"""DataService: единый источник данных (GitHub + Brawl API + DB + Cache)."""
from __future__ import annotations
import asyncio
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from services.github_service import GitHubService
from utils.logger import setup_logger

logger = setup_logger(__name__)


class DataService:
    def __init__(self, db, github: GitHubService, brawl_api=None, cache=None):
        self.db        = db
        self.github    = github
        self.brawl_api = brawl_api
        self.cache     = cache

    def _norm(self, tag: str) -> str:
        return tag.strip().upper().lstrip("#")

    # ─── Players ─────────────────────────────────────────────────────────────

    async def get_player(self, tag: str) -> Optional[Dict]:
        tag = self._norm(tag)
        key = f"player:{tag}"

        if self.cache:
            cached = await self.cache.get(key)
            if cached:
                return cached

        data = await self.github.get_player(tag)
        if data:
            if self.cache:
                await self.cache.set(key, data, ttl=600)
            brawlers = data.get("brawlers", [])
            if brawlers:
                asyncio.create_task(self.db.upsert_player_brawler_stats(tag, brawlers))
            return data

        if self.brawl_api and self.brawl_api.api_key:
            logger.info(f"Fetching player {tag} from Brawl API")
            raw = await self.brawl_api.get_player(tag)
            if raw:
                asyncio.create_task(self.github.upsert_player(tag, raw))
                asyncio.create_task(self.db.upsert_player(raw))
                if self.cache:
                    await self.cache.set(key, raw, ttl=600)
                return raw

        return None

    async def list_players(self, limit: int = 100, offset: int = 0) -> Dict:
        players = await self.github.list_players()
        return {"total": len(players), "limit": limit, "offset": offset,
                "players": players[offset:offset + limit]}

    # ─── Clubs ───────────────────────────────────────────────────────────────

    async def get_club(self, tag: str) -> Optional[Dict]:
        tag = self._norm(tag)
        key = f"club:{tag}"

        if self.cache:
            cached = await self.cache.get(key)
            if cached:
                return cached

        data = await self.github.get_club(tag)
        if data:
            if self.cache:
                await self.cache.set(key, data, ttl=600)
            return data

        if self.brawl_api and self.brawl_api.api_key:
            raw = await self.brawl_api.get_club(tag)
            if raw:
                asyncio.create_task(self.github.upsert_club(tag, raw))
                asyncio.create_task(self.db.upsert_club(raw))
                if self.cache:
                    await self.cache.set(key, raw, ttl=600)
                return raw

        return None

    async def list_clubs(self, limit: int = 100, offset: int = 0) -> Dict:
        clubs = await self.github.list_clubs()
        return {"total": len(clubs), "limit": limit, "offset": offset,
                "clubs": clubs[offset:offset + limit]}

    # ─── Battles ─────────────────────────────────────────────────────────────

    async def get_battles(self, tag: str, limit: int = 20) -> List[Dict]:
        tag = self._norm(tag)
        gh_battles = await self.github.get_battles(tag)
        if gh_battles:
            return gh_battles[:limit]
        return await self.db.get_recent_battles(tag, limit)

    async def get_battles_stats(self, tag: str) -> Dict:
        return await self.db.get_battle_stats(self._norm(tag))

    # ─── Brawlers ────────────────────────────────────────────────────────────

    async def get_player_brawlers(self, tag: str) -> List[Dict]:
        tag  = self._norm(tag)
        rows = await self.db.get_player_brawler_stats(tag)
        if rows:
            return rows
        data = await self.get_player(tag)
        if not data:
            return []
        brawlers = data.get("brawlers", [])
        if brawlers:
            asyncio.create_task(self.db.upsert_player_brawler_stats(tag, brawlers))
        return [
            {"brawler_id": b.get("id"), "brawler_name": b.get("name"),
             "trophies": b.get("trophies", 0), "highest_trophies": b.get("highestTrophies", 0),
             "power": b.get("power", 1), "rank": b.get("rank", 1)}
            for b in brawlers
        ]

    # ─── History ─────────────────────────────────────────────────────────────

    async def get_trophy_history(self, tag: str, days: int = 30) -> List[Dict]:
        tag = self._norm(tag)
        gh = await self.github.get_trophy_history(tag)
        if gh:
            return gh[:days]
        return await self.db.get_trophy_history(tag, days)

    async def get_club_history(self, tag: str, days: int = 30) -> List[Dict]:
        tag = self._norm(tag)
        gh = await self.github.get_club_history(tag)
        if gh:
            return gh[:days]
        return await self.db.get_club_history(tag, days)

    # ─── Team stats ──────────────────────────────────────────────────────────

    async def get_team_stats(self, tags: List[str]) -> Optional[Dict]:
        sorted_tags = sorted(t.upper().lstrip("#") for t in tags)
        team_hash   = hashlib.sha256(",".join(sorted_tags).encode()).hexdigest()[:16]
        # Try GitHub first
        gh_stats = await self.github.get_team_stats(team_hash)
        if gh_stats:
            return gh_stats
        # Fallback DB
        return await self.db.get_team_stats(tags)

    # ─── Map stats ───────────────────────────────────────────────────────────

    async def get_map_stats(self, limit: int = 50) -> List[Dict]:
        gh = await self.github.get_map_stats()
        if gh:
            return gh[:limit]
        return await self.db.get_map_stats(limit)

    # ─── Rankings ────────────────────────────────────────────────────────────

    async def get_rankings(self, kind: str = "players", limit: int = 100, offset: int = 0) -> List[Dict]:
        from datetime import date
        today = date.today().isoformat()
        if kind == "players":
            gh = await self.github.get_rankings_players(today)
            if gh and "players" in gh:
                data = gh["players"]
                return data[offset:offset + limit]
            rows = await self.db.get_top_players_from_trophy_history(limit, offset)
            return [{"player_tag": r["player_tag"], "trophies": r["trophies"],
                     "date": str(r.get("date", ""))} for r in rows]
        else:
            gh = await self.github.get_rankings_clubs(today)
            if gh and "clubs" in gh:
                data = gh["clubs"]
                return data[offset:offset + limit]
            clubs = await self.github.list_clubs()
            all_c = await self.github.bulk_fetch_clubs(clubs)
            ranked = sorted(all_c, key=lambda x: x.get("trophies", 0), reverse=True)
            return [{"tag": c.get("tag", ""), "name": c.get("name", ""),
                     "trophies": c.get("trophies", 0)} for c in ranked[offset:offset + limit]]

    # ─── Compare players ─────────────────────────────────────────────────────

    async def compare_players(self, tags: List[str]) -> List[Dict]:
        results = []
        for t in tags[:3]:
            p = await self.get_player(t)
            if p:
                results.append(p)
        return results
