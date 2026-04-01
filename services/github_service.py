"""GitHubService — обёртка над GitHubClient с кэшированием."""
from __future__ import annotations
from typing import Any, Dict, List, Optional
from github_client import GitHubClient
from utils.logger import setup_logger

logger = setup_logger(__name__)


class GitHubService:
    def __init__(self, gh: GitHubClient, cache=None):
        self.gh    = gh
        self.cache = cache

    async def list_players(self) -> List[Dict]:
        return await self.gh.list_players()

    async def list_clubs(self) -> List[Dict]:
        return await self.gh.list_clubs()

    async def get_player(self, tag: str) -> Optional[Dict]:
        return await self.gh.get_player(tag)

    async def upsert_player(self, tag: str, data: Dict) -> bool:
        ok = await self.gh.upsert_player(tag, data)
        if ok and self.cache:
            await self.cache.set(f"player:{tag.upper().lstrip('#')}", data, ttl=600)
        return ok

    async def get_club(self, tag: str) -> Optional[Dict]:
        return await self.gh.get_club(tag)

    async def upsert_club(self, tag: str, data: Dict) -> bool:
        ok = await self.gh.upsert_club(tag, data)
        if ok and self.cache:
            await self.cache.set(f"club:{tag.upper().lstrip('#')}", data, ttl=600)
        return ok

    async def get_trophy_history(self, tag: str) -> Optional[List]:
        return await self.gh.get_trophy_history(tag)

    async def upsert_trophy_history(self, tag: str, data: List) -> bool:
        return await self.gh.upsert_trophy_history(tag, data)

    async def get_club_history(self, tag: str) -> Optional[List]:
        return await self.gh.get_club_history(tag)

    async def upsert_club_history(self, tag: str, data: List) -> bool:
        return await self.gh.upsert_club_history(tag, data)

    async def get_battles(self, tag: str) -> Optional[List]:
        return await self.gh.get_battles(tag)

    async def upsert_battles(self, tag: str, data: List) -> bool:
        return await self.gh.upsert_battles(tag, data)

    async def get_map_stats(self) -> Optional[List]:
        return await self.gh.get_map_stats()

    async def upsert_map_stats(self, data: List) -> bool:
        return await self.gh.upsert_map_stats(data)

    async def get_team_stats(self, team_hash: str) -> Optional[Dict]:
        return await self.gh.get_team_stats(team_hash)

    async def upsert_team_stats(self, team_hash: str, data: Dict) -> bool:
        return await self.gh.upsert_team_stats(team_hash, data)

    async def get_rankings_players(self, date_str: str) -> Optional[Dict]:
        return await self.gh.get_rankings_players(date_str)

    async def upsert_rankings_players(self, date_str: str, data: Dict) -> bool:
        return await self.gh.upsert_rankings_players(date_str, data)

    async def get_rankings_clubs(self, date_str: str) -> Optional[Dict]:
        return await self.gh.get_rankings_clubs(date_str)

    async def upsert_rankings_clubs(self, date_str: str, data: Dict) -> bool:
        return await self.gh.upsert_rankings_clubs(date_str, data)

    async def list_team_codes(self) -> List[Dict]:
        return await self.gh.list_team_codes()

    async def get_team_code(self, code: str) -> Optional[Dict]:
        return await self.gh.get_team_code(code)

    async def upsert_team_code(self, code: str, data: Dict) -> bool:
        return await self.gh.upsert_team_code(code, data)

    async def delete_team_code(self, code: str) -> bool:
        return await self.gh.delete_team_code(code)

    async def bulk_fetch_players(self, items: List[Dict]) -> List[Dict]:
        return await self.gh.bulk_fetch_players(items)

    async def bulk_fetch_clubs(self, items: List[Dict]) -> List[Dict]:
        return await self.gh.bulk_fetch_clubs(items)

    async def build_name_index(self) -> Dict:
        return await self.gh.build_name_index()

    async def build_club_name_index(self) -> Dict:
        return await self.gh.build_club_name_index()
